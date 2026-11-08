package nzbfilesystem

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/javi11/nntppool/v5"
	"github.com/kipsilabs/altmount/internal/holes"
	"github.com/kipsilabs/altmount/internal/metadata"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
	"github.com/kipsilabs/altmount/internal/usenet"
)

// holeMetaSnapshot holds the mvf.meta fields the hole path needs, captured
// once in holeHooks() on the caller's own goroutine while mvf.meta is still
// guaranteed non-nil. onHole runs on detached per-segment download goroutines
// that Close() does not wait for, and Close() sets mvf.meta = nil with no
// synchronization against them — so the hole path must never read mvf.meta
// directly; this snapshot is its only race-free access.
type holeMetaSnapshot struct {
	fileSize      int64
	sourceNzbPath string
	totalSegments int
	// fingerprint names the provider set this handle's holes are judged
	// against; persisted with every run it records.
	fingerprint string
	// acceptableMissingPct is health.acceptable_missing_segments_percentage,
	// the user's ceiling on missing segments (see acceptableMissingPct).
	acceptableMissingPct float64
	// patches finds the seeded holes a usable PAR2 patch serves; nil when
	// no ceiling applies or there is no patch source.
	patches *holePatches
}

// holePatches finds, on first need, which seeded holes a usable PAR2 patch
// serves. Those are real data, so the acceptable-missing ceiling does not
// count them (as in health's classifyHoles); the fixed caps still do. The
// lookup runs at most once per handle, and only when a ceiling decision
// depends on it, so a patch added or pruned later is not seen. Guarded by
// mvf.holeMu.
type holePatches struct {
	source  PatchSource
	seeded  []seededHole
	looked  bool
	patched []int
}

// seededHole is a hole loaded from the persisted map, with what the patch
// check needs, captured while mvf.meta is valid.
type seededHole struct {
	index int
	id    string
	size  int64 // the whole decoded article, which a patch must match; 0 when unknown
}

// indices returns the seeded holes a usable patch serves, looking them up on
// the first call.
func (p *holePatches) indices() []int {
	if !p.looked {
		p.looked = true
		for _, h := range p.seeded {
			if patchUsable(p.source, h.id, h.size) {
				p.patched = append(p.patched, h.index)
			}
		}
	}
	return p.patched
}

// patchSizer is the cheap size check par2repair.PatchStore offers.
type patchSizer interface {
	Size(messageID string) (int64, bool)
}

// patchUsable reports whether the source holds a patch the reader serves for
// any read of this segment: one whose length is the whole decoded article.
// A stat is enough when the source offers one; otherwise the payload is read.
func patchUsable(source PatchSource, id string, size int64) bool {
	if size <= 0 {
		return false
	}
	id = strings.Trim(id, "<>")
	if sizer, ok := source.(patchSizer); ok {
		n, found := sizer.Size(id)
		return found && n == size
	}
	p, found := source.Get(id)
	return found && int64(len(p)) == size
}

// holeEligible reports whether this file's missing segments may be
// zero-filled: plain (unencrypted, non-nested, non-remuxed) video only.
// Padding anything else would silently corrupt copies/imports served
// through the mount.
func (mvf *MetadataVirtualFile) holeEligible() bool {
	return holes.EligibleFile(mvf.name) &&
		mvf.meta.Encryption == metapb.Encryption_NONE &&
		len(mvf.meta.NestedSources) == 0 &&
		len(mvf.meta.ClipBoundaries) == 0
}

// PatchSource serves repaired article payloads by message ID (implemented by
// par2repair.PatchStore). Must be fast local I/O and concurrency-safe.
type PatchSource interface {
	Get(messageID string) ([]byte, bool)
}

// holeHooks returns the reader hooks that implement on-the-fly zero-fill for
// this handle, or nil when the file is ineligible. The hooks are built once
// per handle; the holes they share are seeded from the persisted hole map so
// replay pre-pad works across opens. Stored holes past the acceptable-missing
// ceiling are checked first (see dropRecoveredStoredHoles).
//
// Files ineligible for zero-fill (encrypted, nested, non-video) still get
// hooks carrying only PatchLookup when a patch source is configured: their
// streams fail on new damage exactly as before, but articles repaired by a
// PAR2 job serve byte-exact.
func (mvf *MetadataVirtualFile) holeHooks() *usenet.HoleHooks {
	mvf.holeOnce.Do(func() {
		if !mvf.holeEligible() {
			if mvf.patchSource != nil {
				mvf.holeHooksVal = &usenet.HoleHooks{PatchLookup: mvf.patchLookup}
			}
			return
		}
		fingerprint := ""
		if mvf.configGetter != nil {
			fingerprint = mvf.configGetter().ProviderFingerprint()
		}
		// Only holes confirmed recently, and against the providers configured
		// now, are replayed without a fetch; anything else is asked for again.
		stored := &holes.Accumulator{}
		stored.Load(metadata.LiveKnownHoles(mvf.meta.KnownHoles, mvf.meta.HoleProviderFingerprint, fingerprint, time.Now()))
		pct := mvf.acceptableMissingPct()
		// Snapshot before any detached download goroutine exists — see the
		// holeMetaSnapshot doc comment for why onHole must not read mvf.meta.
		mvf.holeMeta = holeMetaSnapshot{
			fileSize:             mvf.meta.FileSize,
			sourceNzbPath:        mvf.meta.SourceNzbPath,
			totalSegments:        len(mvf.meta.SegmentData),
			fingerprint:          fingerprint,
			acceptableMissingPct: pct,
			patches:              mvf.seededHolePatches(stored.Runs(), pct),
		}
		if mvf.overCeiling(mvf.ceilingMissing(stored)) {
			mvf.dropRecoveredStoredHoles(stored)
		}
		mvf.holeAcc = stored
		mvf.holeHooksVal = &usenet.HoleHooks{
			OnHole:     mvf.onHole,
			KnownHoles: mvf.isKnownHole,
			Fetched:    mvf.onFetched,
		}
		if mvf.patchSource != nil {
			mvf.holeHooksVal.PatchLookup = mvf.patchLookup
		}
	})
	return mvf.holeHooksVal
}

// patchLookup adapts the patch source to the reader hook. Segment IDs are
// normalized to the bare form the patch store keys on.
func (mvf *MetadataVirtualFile) patchLookup(segID string) []byte {
	p, ok := mvf.patchSource.Get(strings.Trim(segID, "<>"))
	if !ok {
		return nil
	}
	return p
}

// dropRecoveredStoredHoles asks the providers once about each stored hole,
// for a file whose stored holes are past the acceptable-missing ceiling, and
// drops the ones a provider has again. Stored holes are often misses that did
// not last; counted, a recovered one would fail the file, and send it to
// repair, before it is ever fetched. Anything short of a positive answer (the
// article missing, a provider error, the timeout, no pool) keeps the hole
// counted. The checks are the priority-lane STAT the reader's miss re-check
// uses, under one storedHoleCheckTimeout. Called from holeHooks while
// mvf.meta is valid and before any hook can run.
func (mvf *MetadataVirtualFile) dropRecoveredStoredHoles(stored *holes.Accumulator) {
	if mvf.poolManager == nil {
		return
	}
	cp, err := mvf.poolManager.GetPool()
	if err != nil || cp == nil {
		return
	}
	segs := mvf.meta.SegmentData
	index := map[string]int{}
	var ids []string
	for _, r := range stored.Runs() {
		for i := r.Start; i < r.Start+r.Count && i < len(segs); i++ {
			id := segs[i].Id
			if id == "" || holes.IsPlaceholderID(id) {
				continue
			}
			index[id] = i
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(mvf.ctx, mvf.storedHoleCheckTimeout())
	defer cancel()
	for res := range cp.ExistsMany(ctx, ids, nntppool.ManyOptions{
		Lane:        nntppool.LanePriority,
		ArticleDate: mvf.articleDate(),
	}) {
		if res.Err == nil {
			removeHole(stored, index[res.MessageID])
		}
	}
}

// storedHoleCheckTimeout bounds the stored-hole check: MissRecheckTimeout,
// clamped to half the streaming read timeout. The check runs while the first
// read's reader is being built, inside that read's budget, and the read's
// watchdog fires once: the reader must be in place to be interrupted before it
// does, or a slow fetch after the check would run unbounded.
func (mvf *MetadataVirtualFile) storedHoleCheckTimeout() time.Duration {
	timeout := usenet.MissRecheckTimeout
	if mvf.configGetter != nil {
		if cfg := mvf.configGetter(); cfg != nil {
			if rt := cfg.GetStreamReadTimeout() / 2; rt > 0 && rt < timeout {
				timeout = rt
			}
		}
	}
	return timeout
}

// isKnownHole reports whether a segment is already in the hole map (replay
// pre-pad: zero-fill without a fetch round-trip). Once the file's holes are
// past the acceptable-missing ceiling nothing is replayed: the segment is
// fetched, so a recovered article serves real bytes and a still-missing one
// reaches onHole.
func (mvf *MetadataVirtualFile) isKnownHole(segIndex int) bool {
	mvf.holeMu.Lock()
	defer mvf.holeMu.Unlock()
	if !mvf.holeAcc.Has(segIndex) {
		return false
	}
	return !mvf.overCeiling(mvf.ceilingMissing(mvf.holeAcc))
}

// onFetched drops a segment from the handle's holes once a provider (or the
// segment cache) has served it. A known hole is fetched instead of replayed
// once the file is past the ceiling; left in after it is served, it would keep
// the file over the ceiling and keep its other holes from replaying. The
// persisted hole map is left to its TTL.
func (mvf *MetadataVirtualFile) onFetched(segIndex int) {
	mvf.holeMu.Lock()
	defer mvf.holeMu.Unlock()
	removeHole(mvf.holeAcc, segIndex)
}

// removeHole takes one segment out of acc.
func removeHole(acc *holes.Accumulator, segIndex int) {
	if !acc.Has(segIndex) {
		return
	}
	var kept holes.Accumulator
	for _, r := range acc.Runs() {
		if segIndex < r.Start || segIndex >= r.Start+r.Count {
			kept.AddRun(r)
			continue
		}
		kept.AddRun(holes.Run{Start: r.Start, Count: segIndex - r.Start})
		kept.AddRun(holes.Run{Start: segIndex + 1, Count: r.Start + r.Count - segIndex - 1})
	}
	*acc = kept
}

// ceilingMissing is the missing count judged against the acceptable-missing
// ceiling: the holes in acc less those a usable patch serves. Patches are
// looked up only when the raw total would pass the ceiling. Caller holds
// mvf.holeMu, or is holeHooks before the hooks exist.
func (mvf *MetadataVirtualFile) ceilingMissing(acc *holes.Accumulator) int {
	total := acc.Total()
	if mvf.holeMeta.patches == nil || !mvf.overCeiling(total) {
		return total
	}
	for _, i := range mvf.holeMeta.patches.indices() {
		if acc.Has(i) {
			total--
		}
	}
	return total
}

// overCeiling reports whether missing segments pass this handle's
// acceptable-missing ceiling. Caller holds mvf.holeMu.
func (mvf *MetadataVirtualFile) overCeiling(missing int) bool {
	return holes.ExceedsAcceptableMissing(missing, mvf.holeMeta.totalSegments, mvf.holeMeta.acceptableMissingPct)
}

// onHole is the synchronous pad/fail verdict for a segment just confirmed
// missing on every provider. It merges the miss into the handle's
// accumulator, applies the threshold table, and — when the file remains
// within the padding caps — records the file as degraded off the hot path.
// Runs on download goroutines: no network, no blocking I/O.
func (mvf *MetadataVirtualFile) onHole(segIndex int, segID string) holes.Decision {
	mvf.holeMu.Lock()
	alreadyKnown := mvf.holeAcc.Has(segIndex)
	mvf.holeAcc.Add(segIndex)
	runs := mvf.holeAcc.Runs()
	total := mvf.holeAcc.Total()
	longest := mvf.holeAcc.LongestRun()
	ceilingMissing := mvf.ceilingMissing(mvf.holeAcc)
	mvf.holeMu.Unlock()

	// Use the immutable snapshot captured in holeHooks(), never mvf.meta —
	// see the holeMetaSnapshot doc comment.
	segBytes := avgSegBytes(mvf.holeMeta.fileSize, mvf.holeMeta.totalSegments)
	verdict := holes.Classify(runs, mvf.holeMeta.fileSize, segBytes)
	if verdict != holes.VerdictDegraded {
		// Caps exceeded: fail the stream; the DataCorruptionError path takes
		// over (repair trigger, safety-folder move) as it always has.
		slog.WarnContext(mvf.ctx, "Missing segment exceeds padding caps, failing stream",
			"file", mvf.name,
			"segment_id", segID,
			"total_missing", total,
			"longest_run", longest)
		return holes.DecisionFail
	}
	if applyAcceptableMissing(verdict, ceilingMissing, mvf.holeMeta.totalSegments, mvf.holeMeta.acceptableMissingPct) == holes.VerdictFailed {
		// Same DataCorruptionError path as the caps above.
		slog.WarnContext(mvf.ctx, "Missing segments exceed acceptable_missing_segments_percentage, failing stream",
			"file", mvf.name,
			"segment_id", segID,
			"total_missing", total,
			"total_segments", mvf.holeMeta.totalSegments,
			"acceptable_missing_percentage", mvf.holeMeta.acceptableMissingPct)
		return holes.DecisionFail
	}

	// Record the degradation without stalling the download goroutine: hand a
	// value-typed event to the process-lived padRecorder worker. Replays of
	// already-known holes change nothing, so only new discoveries write.
	if !alreadyKnown {
		mvf.padRecorder.enqueue(padEvent{
			name:          mvf.name,
			segIndex:      segIndex,
			segID:         segID,
			sourceNzbPath: mvf.holeMeta.sourceNzbPath,
			fileSize:      mvf.holeMeta.fileSize,
			total:         total,
			longest:       longest,
			totalSegments: mvf.holeMeta.totalSegments,
			segBytes:      segBytes,
			fingerprint:   mvf.holeMeta.fingerprint,
		})
	}
	return holes.DecisionPad
}

// classifyStreamingFailure builds the playback-impact summary for a stream
// that FAILED on a missing article (hooks absent, pad caps or the
// acceptable-missing ceiling exceeded).
// Returns nil for ineligible files or non-hole failures (yEnc corruption,
// pool errors), which follow the plain corruption path.
func (mvf *MetadataVirtualFile) classifyStreamingFailure(dcErr *usenet.DataCorruptionError) *holes.Impact {
	if !mvf.holeEligible() || !usenet.IsArticleNotFound(dcErr.UnderlyingErr) {
		return nil
	}

	var acc holes.Accumulator
	acc.Load(metadata.KnownHolesFromProto(mvf.meta.KnownHoles))
	mvf.holeMu.Lock()
	hooked := mvf.holeAcc != nil
	if hooked {
		acc.Load(mvf.holeAcc.Runs())
	}
	mvf.holeMu.Unlock()

	// Judge by the ceiling onHole used: the handle's snapshot once its hooks
	// exist, so a config reload since open cannot turn a miss onHole failed
	// into a degraded verdict that skips repair.
	pct := mvf.acceptableMissingPct()
	if hooked {
		pct = mvf.holeMeta.acceptableMissingPct
	}

	// Fold in the failing segment when its position is known.
	if dcErr.FileOffset >= 0 {
		idx := buildSegmentIndex(mvf.meta.SegmentData)
		if segIdx := idx.findSegmentForOffset(dcErr.FileOffset); segIdx >= 0 {
			acc.Add(segIdx)
		}
	}

	totalSegments := len(mvf.meta.SegmentData)
	segBytes := avgSegBytes(mvf.meta.FileSize, totalSegments)
	// Apply the ceiling onHole applies, or a miss it failed would be
	// reclassified as degraded here and never repaired.
	verdict := applyAcceptableMissing(holes.Classify(acc.Runs(), mvf.meta.FileSize, segBytes),
		acc.Total(), totalSegments, pct)
	return &holes.Impact{
		Verdict:       verdict,
		TotalMissing:  acc.Total(),
		LongestRun:    acc.LongestRun(),
		TotalSegments: totalSegments,
		PaddedRatio:   paddedRatio(acc.Total(), segBytes, mvf.meta.FileSize),
	}
}

// acceptableMissingPct is the configured
// health.acceptable_missing_segments_percentage. Without a config getter it
// is 100, which ExceedsAcceptableMissing never exceeds, so padding keeps to
// the fixed caps alone.
func (mvf *MetadataVirtualFile) acceptableMissingPct() float64 {
	if mvf.configGetter == nil {
		return 100
	}
	return mvf.configGetter().GetAcceptableMissingSegmentsPercentage()
}

// seededHolePatches records what the patch check needs about the seeded
// holes, without any patch I/O. Nil when there is nothing to check: no patch
// source, no stored holes, or no ceiling. Called from holeHooks while mvf.meta
// is valid.
func (mvf *MetadataVirtualFile) seededHolePatches(runs []holes.Run, pct float64) *holePatches {
	if mvf.patchSource == nil || len(runs) == 0 || pct >= 100 {
		return nil
	}
	segs := mvf.meta.SegmentData
	p := &holePatches{source: mvf.patchSource}
	for _, run := range runs {
		start := min(run.Start, len(segs))
		end := start + min(run.Count, len(segs)-start)
		for i := start; i < end; i++ {
			size := segs[i].SegmentSize
			if size <= segs[i].EndOffset {
				size = 0 // inconsistent sizes: never count a patch for it
			}
			p.seeded = append(p.seeded, seededHole{index: i, id: segs[i].Id, size: size})
		}
	}
	return p
}

// applyAcceptableMissing escalates a verdict the fixed caps left short of
// failed to VerdictFailed once the missing segments pass the configured
// acceptable-missing percentage, the rule health's classifyHoles applies.
// Playback uses it both to fail a miss and to classify the failed stream, so
// the two never disagree.
func applyAcceptableMissing(verdict holes.Verdict, missing, totalSegments int, pct float64) holes.Verdict {
	if verdict != holes.VerdictFailed && holes.ExceedsAcceptableMissing(missing, totalSegments, pct) {
		return holes.VerdictFailed
	}
	return verdict
}

// avgSegBytes estimates the decoded segment size for the byte-ratio guard.
func avgSegBytes(fileSize int64, totalSegments int) int64 {
	if totalSegments <= 0 || fileSize <= 0 {
		return 1
	}
	return fileSize / int64(totalSegments)
}

// paddedRatio is missing bytes over file bytes (0 when size is unknown).
func paddedRatio(total int, segBytes, fileSize int64) float64 {
	if fileSize <= 0 {
		return 0
	}
	return float64(int64(total)*segBytes) / float64(fileSize)
}
