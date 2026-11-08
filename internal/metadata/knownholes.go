package metadata

import (
	"time"

	"github.com/kipsilabs/altmount/internal/holes"
	metapb "github.com/kipsilabs/altmount/internal/metadata/proto"
)

// LiveKnownHoles returns the stored runs still trusted now: stamped within
// holes.TTL and recorded under fingerprint. Unstamped (legacy) or foreign
// runs are dropped so the next read through them re-probes the article and,
// if it is still gone, records it afresh.
func LiveKnownHoles(rows []*metapb.HoleRun, storedFP, fingerprint string, now time.Time) []holes.Run {
	if storedFP != fingerprint {
		return nil
	}
	cutoff := now.Add(-holes.TTL).Unix()
	out := make([]holes.Run, 0, len(rows))
	for _, r := range rows {
		if r == nil || r.StartSegment < 0 || r.Count <= 0 || r.RecordedAt <= 0 || r.RecordedAt < cutoff {
			continue
		}
		out = append(out, holes.Run{Start: int(r.StartSegment), Count: int(r.Count)})
	}
	return out
}

// AddKnownHoles merges newly confirmed missing-segment runs into a file's
// persisted hole map under fingerprint, using the same read-modify-write
// path as status updates. Stored runs recorded under a different provider
// set are dropped first: they answered a question about other servers.
// Merging goes through holes.Accumulator, so the write is idempotent and
// concurrent discoveries collapse into maximal runs; a merged run is stamped
// now when it touches a new run and otherwise keeps its newest stored stamp.
func (ms *MetadataService) AddKnownHoles(virtualPath string, runs []holes.Run, fingerprint string) error {
	if len(runs) == 0 {
		return nil
	}
	now := time.Now()
	return ms.UpdateFileMetadata(virtualPath, func(metadata *metapb.FileMetadata) {
		var stored []*metapb.HoleRun
		if metadata.HoleProviderFingerprint == fingerprint {
			stored = metadata.KnownHoles
		}
		metadata.KnownHoles = mergeStamped(stored, runs, now)
		metadata.HoleProviderFingerprint = fingerprint
	})
}

func mergeStamped(stored []*metapb.HoleRun, fresh []holes.Run, now time.Time) []*metapb.HoleRun {
	var acc holes.Accumulator
	acc.Load(KnownHolesFromProto(stored))
	acc.Load(fresh)
	merged := acc.Runs()
	if len(merged) == 0 {
		return nil
	}
	out := make([]*metapb.HoleRun, 0, len(merged))
	for _, m := range merged {
		stamp := int64(0)
		for _, f := range fresh {
			if overlaps(m, f) {
				stamp = now.Unix()
				break
			}
		}
		if stamp == 0 {
			for _, s := range stored {
				if s != nil && overlaps(m, holes.Run{Start: int(s.StartSegment), Count: int(s.Count)}) && s.RecordedAt > stamp {
					stamp = s.RecordedAt
				}
			}
		}
		if stamp == 0 {
			stamp = now.Unix()
		}
		row := &metapb.HoleRun{StartSegment: int64(m.Start), Count: int64(m.Count), RecordedAt: stamp}
		// A Void layout is computed for one exact run: it points from the
		// end of the block containing the run's first byte to the first
		// intact block after its last. It carries over only while the run
		// keeps exactly that extent; a re-stamp of the same extent keeps it,
		// any merge that grows the run drops it so it is recomputed.
		if s := storedExact(stored, m); s != nil {
			row.VoidOffset, row.VoidSize = s.VoidOffset, s.VoidSize
		}
		out = append(out, row)
	}
	return out
}

// storedExact returns the stored row with exactly run's extent, or nil.
func storedExact(stored []*metapb.HoleRun, run holes.Run) *metapb.HoleRun {
	for _, s := range stored {
		if s != nil && s.StartSegment == int64(run.Start) && s.Count == int64(run.Count) {
			return s
		}
	}
	return nil
}

// SetHoleLayout records the Void layout computed for run (see HoleRun's
// void_offset). It is written only if a stored run with exactly that extent
// still exists: a run merged or dropped since the layout was computed must
// never receive it, because a Void sized for the old extent would point into
// the middle of the new hole. offset < 0 records "computed, plain zeros".
func (ms *MetadataService) SetHoleLayout(virtualPath string, run holes.Run, offset, size int64) error {
	return ms.UpdateFileMetadata(virtualPath, func(metadata *metapb.FileMetadata) {
		if s := storedExact(metadata.KnownHoles, run); s != nil {
			s.VoidOffset, s.VoidSize = offset, size
		}
	})
}

func overlaps(a, b holes.Run) bool {
	return a.Start < b.Start+b.Count && b.Start < a.Start+a.Count
}

// KnownHolesToProto converts accumulator runs for storage, stamped now.
func KnownHolesToProto(runs []holes.Run) []*metapb.HoleRun {
	if len(runs) == 0 {
		return nil
	}
	now := time.Now().Unix()
	out := make([]*metapb.HoleRun, 0, len(runs))
	for _, r := range runs {
		out = append(out, &metapb.HoleRun{StartSegment: int64(r.Start), Count: int64(r.Count), RecordedAt: now})
	}
	return out
}

// KnownHolesFromProto rebuilds runs from storage regardless of age or
// provider set; malformed rows are dropped. Callers deciding what to trust
// at read time use LiveKnownHoles.
func KnownHolesFromProto(rows []*metapb.HoleRun) []holes.Run {
	out := make([]holes.Run, 0, len(rows))
	for _, r := range rows {
		if r == nil || r.StartSegment < 0 || r.Count <= 0 {
			continue
		}
		out = append(out, holes.Run{Start: int(r.StartSegment), Count: int(r.Count)})
	}
	return out
}
