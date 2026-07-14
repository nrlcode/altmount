package validation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/holes"
	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/nntppool/v4"
)

const (
	defaultDurableImportConfirmationDelay = 30 * time.Second
	defaultDurableImportIncompleteDelay   = 30 * time.Second
	defaultDurableImportLeaseTTL          = 2 * time.Minute
	durableImportChunkTargetLimit         = 128
	durableImportChunkSpanLimit           = 1024
)

// FinalLayoutProvenanceKind is an importer-owned structural origin. Tolerant
// admission is possible only for a standalone file whose final metadata also
// proves that no nested, encrypted, or virtual-concatenation layer is present.
type FinalLayoutProvenanceKind string

const (
	FinalLayoutProvenanceUnknown              FinalLayoutProvenanceKind = "unknown"
	FinalLayoutProvenanceStandalone           FinalLayoutProvenanceKind = "standalone"
	FinalLayoutProvenanceArchiveMember        FinalLayoutProvenanceKind = "archive_member"
	FinalLayoutProvenanceNestedArchive        FinalLayoutProvenanceKind = "nested_archive"
	FinalLayoutProvenanceISOExpansion         FinalLayoutProvenanceKind = "iso_expansion"
	FinalLayoutProvenanceVirtualConcatenation FinalLayoutProvenanceKind = "virtual_concatenation"
)

// FinalLayoutProvenance is deliberately typed rather than accepting a caller-
// supplied tolerant-eligibility boolean. Its kind is bound into the durable run
// identity so a restarted validation cannot be reopened under a different
// provenance claim.
type FinalLayoutProvenance struct {
	Kind FinalLayoutProvenanceKind
}

// TargetedSTATCauseClass is the complete allowlist of optional durable cause
// summaries. Free-form transport errors never cross the persistence boundary.
type TargetedSTATCauseClass string

const (
	TargetedSTATCauseNone               TargetedSTATCauseClass = ""
	TargetedSTATCauseAuthentication     TargetedSTATCauseClass = "authentication"
	TargetedSTATCauseQuota              TargetedSTATCauseClass = "quota"
	TargetedSTATCauseServiceUnavailable TargetedSTATCauseClass = "service_unavailable"
	TargetedSTATCauseBreakerOpen        TargetedSTATCauseClass = "breaker_open"
	TargetedSTATCauseTimeout            TargetedSTATCauseClass = "timeout"
	TargetedSTATCauseTransport          TargetedSTATCauseClass = "transport"
	TargetedSTATCauseUnknown            TargetedSTATCauseClass = "unknown"
)

// TargetedSTATResult is one complete logical provider check. It contains no
// message identity, provider generation, raw cause, or provider credential.
// The validator attaches the immutable dispatch-snapshot identity itself.
type TargetedSTATResult struct {
	Outcome               nntppool.OutcomeKind
	ResponseCode          int
	CompletionDisposition ImportCheckDisposition
	CauseClass            TargetedSTATCauseClass
	AdmissionWait         time.Duration
	PoolQueue             time.Duration
	PipelineWait          time.Duration
	ResponseService       time.Duration
}

// TargetedSTATProvider freezes the exact provider activation selected for the
// durable run. A transport adapter must refuse the call as incomplete if its
// current provider configuration no longer matches all three fields.
type TargetedSTATProvider struct {
	ID              string
	Generation      int64
	ActivationEpoch int64
}

// TargetedSTATRequest is an in-memory transport request. Position is the
// duplicate-safe correlation key; MessageID must never be retained after the
// transport call or copied into durable evidence.
type TargetedSTATRequest struct {
	Position  int
	MessageID string
}

// TargetedSTATObservation returns one position-correlated logical result.
// Missing, duplicate, or unrequested positions make the bounded batch
// resumable and prevent any result in that batch from being committed.
type TargetedSTATObservation struct {
	Position int
	Result   TargetedSTATResult
}

// TargetedSTATTransport confines a bounded batch to one exact provider
// activation. An error, omission, or incomplete disposition is resumable work
// and is never persisted as a terminal provider check.
type TargetedSTATTransport interface {
	TargetedSTAT(
		ctx context.Context,
		provider TargetedSTATProvider,
		requests []TargetedSTATRequest,
	) ([]TargetedSTATObservation, error)
}

type durableFinalLayoutRepository interface {
	EnsureCandidateFileRevision(context.Context, database.FileRevisionSpec) (*database.HealthFileRevision, error)
	ActivateImportFileRevision(context.Context, int64, string) (*database.HealthFileRevision, error)
	ReconcileProviders(context.Context, []database.ProviderSpec) ([]database.HealthProvider, error)
	CaptureActiveProviderSnapshot(context.Context, time.Time) (*database.ProviderSnapshot, error)
	GetProviderSnapshot(context.Context, string) (*database.ProviderSnapshot, error)
	GetHealthRun(context.Context, string) (*database.HealthRun, error)
	EnsureScheduledHealthRun(context.Context, database.ScheduledHealthRunSpec) (*database.HealthRun, bool, error)
	GetImportValidation(context.Context, int64, string) (*database.ImportValidation, error)
	GetImportQueueDamagePolicy(context.Context, int64) (database.ImportDamagePolicy, bool, error)
	ValidateImportProviderSnapshotCurrent(context.Context, string) error
	AbandonImportValidation(context.Context, int64, string, string, time.Time) error
	RetireUnboundImportRun(context.Context, string, string, time.Time) error
	UpsertImportValidation(context.Context, database.ImportValidationWrite) (*database.ImportValidation, error)
	AcquireRunLease(context.Context, string, string, time.Duration) (*database.HealthRun, error)
	RenewHealthRunLease(context.Context, string, string, int64, time.Duration) (*database.HealthRun, error)
	ParkHealthRun(context.Context, string, string, int64, time.Time, time.Time) error
	GetHealthRunResumeState(context.Context, string) (*database.HealthRunResumeState, error)
	CommitHealthChunk(context.Context, database.HealthChunkCommit) (*database.HealthRun, error)
}

var _ durableFinalLayoutRepository = (*database.HealthStateRepository)(nil)

// DurableFinalLayoutValidatorOptions are immutable for one validator instance.
// ProviderSpecs must contain only the currently enabled providers in configured
// primary/backup order. A zero confirmation delay selects the 30-second default.
type DurableFinalLayoutValidatorOptions struct {
	ProviderSpecs     []database.ProviderSpec
	DamagePolicy      config.ImportDamagePolicy
	ConfirmationDelay time.Duration
	LeaseTTL          time.Duration
	WorkerID          string
	// OnHealthChanged is an invalidation-only callback. It is invoked only
	// after a durable health chunk or lifecycle transition commits, so API/SSE
	// readers can reload authoritative progress while ValidateFinalLayout is
	// still running.
	OnHealthChanged func()
}

// DurableFinalLayoutValidationResult contains only positional and lifecycle
// state. It never exposes or persists article identities.
type DurableFinalLayoutValidationResult struct {
	Status              ImportAdmissionStatus
	RetryAt             *time.Time
	ResumeRequired      bool
	UnresolvedPositions []int
	Impact              holes.Impact
	FileRevisionID      string
	RunID               string
}

// DurableFinalLayoutValidator implements the bounded, restart-safe import
// availability lifecycle. Importer/service wiring remains a separate slice.
type DurableFinalLayoutValidator struct {
	repository        durableFinalLayoutRepository
	transport         TargetedSTATTransport
	providerSpecs     []database.ProviderSpec
	damagePolicy      config.ImportDamagePolicy
	confirmationDelay time.Duration
	leaseTTL          time.Duration
	workerID          string
	onHealthChanged   func()
	now               func() time.Time
	invocation        atomic.Uint64
}

func NewDurableFinalLayoutValidator(
	repository durableFinalLayoutRepository,
	transport TargetedSTATTransport,
	options DurableFinalLayoutValidatorOptions,
) (*DurableFinalLayoutValidator, error) {
	if repository == nil || transport == nil {
		return nil, fmt.Errorf("durable import repository and targeted STAT transport are required")
	}
	if len(options.ProviderSpecs) == 0 {
		return nil, fmt.Errorf("at least one enabled import provider is required")
	}
	policy := options.DamagePolicy
	if policy == "" {
		policy = config.ImportDamagePolicyStrict
	}
	if policy != config.ImportDamagePolicyStrict && policy != config.ImportDamagePolicyTolerant {
		return nil, fmt.Errorf("import damage policy must be strict or tolerant")
	}
	delay := options.ConfirmationDelay
	if delay == 0 {
		delay = defaultDurableImportConfirmationDelay
	}
	if delay < 0 {
		return nil, fmt.Errorf("import confirmation delay must not be negative")
	}
	leaseTTL := options.LeaseTTL
	if leaseTTL == 0 {
		leaseTTL = defaultDurableImportLeaseTTL
	}
	if leaseTTL < 0 {
		return nil, fmt.Errorf("import validation lease TTL must not be negative")
	}
	workerID := sanitizeDurableWorkerID(options.WorkerID)
	return &DurableFinalLayoutValidator{
		repository: repository, transport: transport,
		providerSpecs: append([]database.ProviderSpec(nil), options.ProviderSpecs...),
		damagePolicy:  policy, confirmationDelay: delay, leaseTTL: leaseTTL,
		workerID: workerID, onHealthChanged: options.OnHealthChanged,
		now: func() time.Time { return time.Now().UTC() },
	}, nil
}

func (v *DurableFinalLayoutValidator) notifyHealthChanged() {
	if v != nil && v.onHealthChanged != nil {
		v.onHealthChanged()
	}
}

func sanitizeDurableWorkerID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "import-validator"
	}
	if len(value) > 48 {
		return "import-validator"
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '-' || character == '_' {
			continue
		}
		return "import-validator"
	}
	return value
}

func (v *DurableFinalLayoutValidator) nextLeaseOwner() string {
	return fmt.Sprintf("%s-%d", v.workerID, v.invocation.Add(1))
}

// ValidateFinalLayout advances as much durable work as the current call can
// complete. A confirmation wait returns immediately after parking the run; a
// later invocation (including after process restart) resumes from committed
// coverage. Incomplete transport work returns ResumeRequired without changing
// it into absence or a terminal import decision.
func (v *DurableFinalLayoutValidator) ValidateFinalLayout(
	ctx context.Context,
	queueItemID int64,
	finalVirtualPath string,
	finalMetadata *metapb.FileMetadata,
	provenance FinalLayoutProvenance,
) (DurableFinalLayoutValidationResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return DurableFinalLayoutValidationResult{}, err
	}
	if queueItemID <= 0 || strings.TrimSpace(finalVirtualPath) == "" {
		return DurableFinalLayoutValidationResult{}, fmt.Errorf("queue item ID and final virtual path are required")
	}
	if !validFinalLayoutProvenance(provenance.Kind) {
		return DurableFinalLayoutValidationResult{}, fmt.Errorf("invalid final layout provenance")
	}

	layout, err := metadata.ResolveCanonicalSegmentLayout(finalMetadata)
	if err != nil {
		return DurableFinalLayoutValidationResult{}, fmt.Errorf("resolve final canonical layout: %w", err)
	}
	spans := make([]int64, len(layout.Segments))
	for i, segment := range layout.Segments {
		spans[i] = segment.UsableBytes
	}
	revision, err := v.repository.EnsureCandidateFileRevision(ctx, database.FileRevisionSpec{
		FilePath: finalVirtualPath, LayoutFingerprint: layout.Fingerprint,
		VirtualSize: layout.VirtualSize, SegmentCount: int64(len(layout.Segments)),
	})
	if err != nil {
		return DurableFinalLayoutValidationResult{}, err
	}

	validation, err := v.repository.GetImportValidation(ctx, queueItemID, revision.ID)
	if err != nil {
		return DurableFinalLayoutValidationResult{}, err
	}
	hadExistingValidation := validation != nil
	if validation == nil {
		queuePolicy, err := v.queueDamagePolicy(ctx, queueItemID, v.damagePolicy)
		if err != nil {
			return DurableFinalLayoutValidationResult{}, err
		}
		validation, err = v.createValidation(
			ctx, queueItemID, revision, layout, provenance, queuePolicy,
		)
		if errors.Is(err, database.ErrImportDamagePolicy) {
			queuePolicy, policyErr := v.queueDamagePolicy(ctx, queueItemID, queuePolicy)
			if policyErr != nil {
				return DurableFinalLayoutValidationResult{}, policyErr
			}
			validation, err = v.createValidation(
				ctx, queueItemID, revision, layout, provenance, queuePolicy,
			)
		}
		if err != nil {
			return DurableFinalLayoutValidationResult{}, err
		}
	} else {
		runBinding, err := v.repository.GetHealthRun(ctx, validation.RunID)
		if err != nil {
			return DurableFinalLayoutValidationResult{}, err
		}
		if runBinding == nil {
			return DurableFinalLayoutValidationResult{}, fmt.Errorf("durable import health run does not exist")
		}
		expectedRunID := durableImportIdentity(
			"run", fmt.Sprint(queueItemID), revision.ID, string(provenance.Kind), runBinding.ProviderSnapshotID,
		)
		if validation.RunID != expectedRunID {
			return DurableFinalLayoutValidationResult{}, fmt.Errorf("durable import provenance does not match the existing validation")
		}
	}
	persistedPolicy := config.ImportDamagePolicy(validation.DamagePolicy)
	if persistedPolicy != config.ImportDamagePolicyStrict && persistedPolicy != config.ImportDamagePolicyTolerant {
		return DurableFinalLayoutValidationResult{}, fmt.Errorf("durable import validation has an invalid persisted damage policy")
	}

	_, terminal := terminalDurableImportResult(DurableFinalLayoutValidationResult{}, validation)
	needsCurrentSnapshot := !terminal ||
		(!revision.Active && (validation.Phase == database.ImportValidationPhaseAccepted ||
			validation.Phase == database.ImportValidationPhaseHealthPending))
	if hadExistingValidation && needsCurrentSnapshot {
		if _, err := v.repository.ReconcileProviders(ctx, v.providerSpecs); err != nil {
			return DurableFinalLayoutValidationResult{}, err
		}
		if err := v.repository.ValidateImportProviderSnapshotCurrent(ctx, validation.RunID); err != nil {
			if !errors.Is(err, database.ErrProviderSnapshotMismatch) {
				return DurableFinalLayoutValidationResult{}, err
			}
			now := v.now().UTC()
			if err := v.repository.AbandonImportValidation(
				ctx, queueItemID, revision.ID, validation.RunID, now,
			); err != nil {
				return DurableFinalLayoutValidationResult{}, err
			}
			v.notifyHealthChanged()
			validation, err = v.createValidation(
				ctx, queueItemID, revision, layout, provenance, persistedPolicy,
			)
			if err != nil {
				return DurableFinalLayoutValidationResult{}, err
			}
		}
	}

	base := DurableFinalLayoutValidationResult{
		FileRevisionID: revision.ID, RunID: validation.RunID,
		UnresolvedPositions: durableBitmapPositions(validation.UnresolvedBitmap, int64(len(layout.Segments))),
	}
	if result, terminal := terminalDurableImportResult(base, validation); terminal {
		return result, nil
	}
	if validation.Phase == database.ImportValidationPhaseConfirmationWait {
		if validation.ConfirmationDueAt == nil {
			return DurableFinalLayoutValidationResult{}, fmt.Errorf("durable confirmation wait has no deadline")
		}
		now := v.now().UTC()
		if now.Before(validation.ConfirmationDueAt.UTC()) {
			due := validation.ConfirmationDueAt.UTC()
			base.Status = ImportAdmissionAwaitConfirmation
			base.RetryAt = &due
			return base, nil
		}
	}

	owner := v.nextLeaseOwner()
	run, err := v.repository.AcquireRunLease(ctx, validation.RunID, owner, v.leaseTTL)
	if err != nil {
		return DurableFinalLayoutValidationResult{}, err
	}
	if err := validateDurableImportRun(run, revision, layout); err != nil {
		return DurableFinalLayoutValidationResult{}, err
	}
	snapshot, err := v.repository.GetProviderSnapshot(ctx, run.ProviderSnapshotID)
	if err != nil {
		return DurableFinalLayoutValidationResult{}, err
	}
	providers, err := orderedDurableImportProviders(snapshot)
	if err != nil {
		return DurableFinalLayoutValidationResult{}, err
	}

	if validation.Phase == database.ImportValidationPhaseConfirmationWait {
		validation, err = v.advanceValidation(
			ctx, validation, run, database.ImportValidationPhaseConfirmationPass,
			validation.UnresolvedBitmap, false, false, nil,
		)
		if err != nil {
			return DurableFinalLayoutValidationResult{}, err
		}
	}

	return v.driveValidation(
		ctx, validation, run, providers, layout, spans, finalVirtualPath,
		finalMetadata, provenance, persistedPolicy, owner,
	)
}

func (v *DurableFinalLayoutValidator) queueDamagePolicy(
	ctx context.Context,
	queueItemID int64,
	fallback config.ImportDamagePolicy,
) (config.ImportDamagePolicy, error) {
	policy, frozen, err := v.repository.GetImportQueueDamagePolicy(ctx, queueItemID)
	if err != nil {
		return "", err
	}
	if frozen {
		fallback = config.ImportDamagePolicy(policy)
	}
	if fallback != config.ImportDamagePolicyStrict && fallback != config.ImportDamagePolicyTolerant {
		return "", fmt.Errorf("durable import queue has an invalid persisted damage policy")
	}
	return fallback, nil
}

func validFinalLayoutProvenance(kind FinalLayoutProvenanceKind) bool {
	switch kind {
	case FinalLayoutProvenanceUnknown, FinalLayoutProvenanceStandalone,
		FinalLayoutProvenanceArchiveMember, FinalLayoutProvenanceNestedArchive,
		FinalLayoutProvenanceISOExpansion, FinalLayoutProvenanceVirtualConcatenation:
		return true
	default:
		return false
	}
}

func (v *DurableFinalLayoutValidator) createValidation(
	ctx context.Context,
	queueItemID int64,
	revision *database.HealthFileRevision,
	layout *metadata.CanonicalSegmentLayout,
	provenance FinalLayoutProvenance,
	damagePolicy config.ImportDamagePolicy,
) (*database.ImportValidation, error) {
	if _, err := v.repository.ReconcileProviders(ctx, v.providerSpecs); err != nil {
		return nil, err
	}
	now := v.now().UTC()
	snapshot, err := v.repository.CaptureActiveProviderSnapshot(ctx, now)
	if err != nil {
		return nil, err
	}
	if _, err := orderedDurableImportProviders(snapshot); err != nil {
		return nil, err
	}
	runID := durableImportIdentity(
		"run", fmt.Sprint(queueItemID), revision.ID, string(provenance.Kind), snapshot.ID,
	)
	run, _, err := v.repository.EnsureScheduledHealthRun(ctx, database.ScheduledHealthRunSpec{
		Run: database.HealthRunSpec{
			ID: runID, FileRevisionID: revision.ID, ProviderSnapshotID: snapshot.ID,
			Trigger: "import", Mode: "observation", TotalSegments: int64(len(layout.Segments)),
			CreatedAt: now,
		},
		DedupeKey: durableImportIdentity(
			"schedule", fmt.Sprint(queueItemID), revision.ID, string(provenance.Kind), snapshot.ID,
		),
		Priority: database.HealthRunPriorityNormal, NotBefore: now,
	})
	if err != nil {
		return nil, err
	}
	owner := v.nextLeaseOwner()
	leased, err := v.repository.AcquireRunLease(ctx, run.ID, owner, v.leaseTTL)
	if err != nil {
		return nil, err
	}
	write := database.ImportValidationWrite{
		ID:          durableImportIdentity("validation", fmt.Sprint(queueItemID), revision.ID),
		QueueItemID: queueItemID, FileRevisionID: revision.ID, RunID: run.ID,
		Phase:        database.ImportValidationPhaseInitialPass,
		DamagePolicy: database.ImportDamagePolicy(damagePolicy),
		LeaseOwner:   owner, FencingToken: leased.FencingToken,
		CreatedAt: now, UpdatedAt: now,
	}
	validation, writeErr := v.repository.UpsertImportValidation(ctx, write)
	if writeErr == nil {
		v.notifyHealthChanged()
	}
	// The creator lease belongs to this setup operation. Park immediately so
	// the common resume path acquires a fresh fencing token and one owner is
	// never silently reused across two logical invocations.
	parkErr := v.repository.ParkHealthRun(
		ctx, run.ID, owner, leased.FencingToken, now, now,
	)
	if parkErr == nil {
		v.notifyHealthChanged()
	}
	if writeErr != nil {
		// A concurrent first file can freeze the queue policy after this
		// provisional run was scheduled but before its validation is bound.
		// Retire only a genuinely unbound run; a same-file competitor may have
		// bound this exact deterministic run already, which is deliberately left
		// intact by the repository fence.
		retireErr := v.repository.RetireUnboundImportRun(ctx, run.ID, revision.ID, now)
		if retireErr == nil {
			v.notifyHealthChanged()
		}
		if retireErr != nil && !errors.Is(retireErr, database.ErrStaleImportValidation) {
			return nil, errors.Join(writeErr, retireErr)
		}
		return nil, writeErr
	}
	if parkErr != nil {
		return nil, parkErr
	}
	return validation, nil
}

// ActivateFileRevision publishes one already-admitted candidate. It is kept
// separate from validation so MetadataService can call it only after its atomic
// file rename succeeds.
func (v *DurableFinalLayoutValidator) ActivateFileRevision(
	ctx context.Context,
	queueItemID int64,
	revisionID string,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := v.repository.ActivateImportFileRevision(ctx, queueItemID, revisionID)
	if err == nil {
		v.notifyHealthChanged()
	}
	return err
}

func validateDurableImportRun(
	run *database.HealthRun,
	revision *database.HealthFileRevision,
	layout *metadata.CanonicalSegmentLayout,
) error {
	if run == nil || run.ID == "" || run.FileRevisionID != revision.ID ||
		run.Trigger != "import" || run.Mode != "observation" ||
		run.TotalSegments != int64(len(layout.Segments)) || run.FencingToken <= 0 {
		return fmt.Errorf("durable import run does not match the final canonical layout")
	}
	return nil
}

type durableImportProviderKey struct {
	id              string
	generation      int64
	activationEpoch int64
}

func orderedDurableImportProviders(snapshot *database.ProviderSnapshot) ([]database.ProviderSnapshotEntry, error) {
	if snapshot == nil || snapshot.ID == "" || len(snapshot.Entries) == 0 {
		return nil, fmt.Errorf("import provider snapshot must not be empty")
	}
	providers := append([]database.ProviderSnapshotEntry(nil), snapshot.Entries...)
	sort.Slice(providers, func(i, j int) bool {
		leftRole := 1
		if providers[i].Role == database.ProviderRolePrimary {
			leftRole = 0
		}
		rightRole := 1
		if providers[j].Role == database.ProviderRolePrimary {
			rightRole = 0
		}
		if leftRole != rightRole {
			return leftRole < rightRole
		}
		if providers[i].Order != providers[j].Order {
			return providers[i].Order < providers[j].Order
		}
		return providers[i].ProviderID < providers[j].ProviderID
	})
	seen := make(map[string]struct{}, len(providers))
	for i, provider := range providers {
		if strings.TrimSpace(provider.ProviderID) == "" || provider.ProviderGeneration <= 0 ||
			provider.ProviderActivationEpoch <= 0 ||
			(provider.Role != database.ProviderRolePrimary && provider.Role != database.ProviderRoleBackup) {
			return nil, fmt.Errorf("invalid provider snapshot entry %d", i)
		}
		if _, duplicate := seen[provider.ProviderID]; duplicate {
			return nil, fmt.Errorf("provider snapshot contains a duplicate stable identity")
		}
		seen[provider.ProviderID] = struct{}{}
	}
	if providers[0].Role != database.ProviderRolePrimary {
		return nil, fmt.Errorf("import provider snapshot requires an active primary")
	}
	return providers, nil
}

type durableImportCoverage struct {
	total      int64
	baseStage  string
	tested     map[durableImportProviderKey][]byte
	baseTested map[durableImportProviderKey][]byte
	present    map[durableImportProviderKey][]byte
	absent     map[durableImportProviderKey][]byte
}

func loadDurableImportCoverage(
	state *database.HealthRunResumeState,
	providers []database.ProviderSnapshotEntry,
	stage string,
) (*durableImportCoverage, error) {
	if state == nil || state.Run.TotalSegments <= 0 {
		return nil, fmt.Errorf("durable import resume state is missing")
	}
	coverage := &durableImportCoverage{
		total: state.Run.TotalSegments, baseStage: stage,
		tested:     make(map[durableImportProviderKey][]byte, len(providers)),
		baseTested: make(map[durableImportProviderKey][]byte, len(providers)),
		present:    make(map[durableImportProviderKey][]byte, len(providers)),
		absent:     make(map[durableImportProviderKey][]byte, len(providers)),
	}
	for _, provider := range providers {
		key := durableProviderKey(provider)
		coverage.tested[key] = make([]byte, durableBitmapLength(coverage.total))
		coverage.baseTested[key] = make([]byte, durableBitmapLength(coverage.total))
		coverage.present[key] = make([]byte, durableBitmapLength(coverage.total))
		coverage.absent[key] = make([]byte, durableBitmapLength(coverage.total))
	}
	for _, chunk := range state.Chunks {
		if chunk.Stage != stage {
			continue
		}
		key := durableImportProviderKey{
			id: chunk.ProviderID, generation: chunk.ProviderGeneration,
			activationEpoch: chunk.ProviderActivationEpoch,
		}
		tested, current := coverage.tested[key]
		if !current {
			return nil, fmt.Errorf("durable import chunk is outside the frozen provider snapshot")
		}
		if chunk.ObservationKind != database.HealthObservationSTAT || chunk.SegmentStart < 0 ||
			chunk.SegmentCount <= 0 || chunk.SegmentStart > coverage.total-chunk.SegmentCount ||
			len(chunk.TestedBitmap) != durableBitmapLength(chunk.SegmentCount) ||
			len(chunk.PresentBitmap) != durableBitmapLength(chunk.SegmentCount) ||
			len(chunk.AbsentBitmap) != durableBitmapLength(chunk.SegmentCount) {
			return nil, fmt.Errorf("durable import chunk has invalid canonical coverage")
		}
		durableOrRelative(tested, chunk.TestedBitmap, chunk.SegmentStart, chunk.SegmentCount)
		if chunk.Stage == stage {
			durableOrRelative(coverage.baseTested[key], chunk.TestedBitmap, chunk.SegmentStart, chunk.SegmentCount)
		}
		durableOrRelative(coverage.present[key], chunk.PresentBitmap, chunk.SegmentStart, chunk.SegmentCount)
		durableOrRelative(coverage.absent[key], chunk.AbsentBitmap, chunk.SegmentStart, chunk.SegmentCount)
	}
	return coverage, nil
}

func durableProviderKey(provider database.ProviderSnapshotEntry) durableImportProviderKey {
	return durableImportProviderKey{
		id: provider.ProviderID, generation: provider.ProviderGeneration,
		activationEpoch: provider.ProviderActivationEpoch,
	}
}

func (c *durableImportCoverage) testedPosition(provider database.ProviderSnapshotEntry, position int) bool {
	return durableBitmapSet(c.tested[durableProviderKey(provider)], int64(position))
}

func (c *durableImportCoverage) baseTestedPosition(provider database.ProviderSnapshotEntry, position int) bool {
	return durableBitmapSet(c.baseTested[durableProviderKey(provider)], int64(position))
}

func (c *durableImportCoverage) anyPresent(position int) bool {
	for _, present := range c.present {
		if durableBitmapSet(present, int64(position)) {
			return true
		}
	}
	return false
}

func (v *DurableFinalLayoutValidator) driveValidation(
	ctx context.Context,
	validation *database.ImportValidation,
	run *database.HealthRun,
	providers []database.ProviderSnapshotEntry,
	layout *metadata.CanonicalSegmentLayout,
	spans []int64,
	finalVirtualPath string,
	finalMetadata *metapb.FileMetadata,
	provenance FinalLayoutProvenance,
	damagePolicy config.ImportDamagePolicy,
	owner string,
) (DurableFinalLayoutValidationResult, error) {
	state, err := v.repository.GetHealthRunResumeState(ctx, run.ID)
	if err != nil {
		return DurableFinalLayoutValidationResult{}, err
	}
	base := DurableFinalLayoutValidationResult{
		FileRevisionID: run.FileRevisionID, RunID: run.ID,
	}

	switch validation.Phase {
	case database.ImportValidationPhaseInitialPass:
		coverage, err := loadDurableImportCoverage(state, providers, database.HealthRunStageImportInitialSTAT)
		if err != nil {
			return DurableFinalLayoutValidationResult{}, err
		}
		complete, err := v.runInitialPass(ctx, run, owner, providers, layout, coverage)
		if err != nil {
			return DurableFinalLayoutValidationResult{}, err
		}
		unresolved := initialDurableUnresolved(coverage, providers)
		base.UnresolvedPositions = unresolved
		if !complete {
			due := v.now().UTC().Add(defaultDurableImportIncompleteDelay)
			if err := v.parkHealthRun(
				ctx, run.ID, owner, run.FencingToken, due, v.now().UTC(),
			); err != nil {
				return DurableFinalLayoutValidationResult{}, err
			}
			base.Status = ImportAdmissionAwaitConfirmation
			base.ResumeRequired = true
			base.RetryAt = &due
			return base, nil
		}
		if err := validateInitialDurableCoverage(coverage, providers, unresolved); err != nil {
			return DurableFinalLayoutValidationResult{}, err
		}
		unresolvedBitmap := durablePositionsBitmap(unresolved, coverage.total)
		if len(unresolved) == 0 {
			_, err = v.advanceValidation(
				ctx, validation, run, database.ImportValidationPhaseAccepted,
				unresolvedBitmap, true, false, nil,
			)
			if err != nil {
				return DurableFinalLayoutValidationResult{}, err
			}
			base.Status = ImportAdmissionAccept
			return base, nil
		}
		now := v.now().UTC()
		due := now.Add(v.confirmationDelay)
		_, err = v.advanceValidation(
			ctx, validation, run, database.ImportValidationPhaseConfirmationWait,
			unresolvedBitmap, true, false, &due,
		)
		if err != nil {
			return DurableFinalLayoutValidationResult{}, err
		}
		if err := v.parkHealthRun(
			ctx, run.ID, owner, run.FencingToken, due, now,
		); err != nil {
			return DurableFinalLayoutValidationResult{}, err
		}
		base.Status = ImportAdmissionAwaitConfirmation
		base.RetryAt = &due
		return base, nil

	case database.ImportValidationPhaseConfirmationPass:
		coverage, err := loadDurableImportCoverage(state, providers, database.HealthRunStageImportConfirmationSTAT)
		if err != nil {
			return DurableFinalLayoutValidationResult{}, err
		}
		initialUnresolved := durableBitmapPositions(validation.UnresolvedBitmap, coverage.total)
		complete, err := v.runConfirmationPass(
			ctx, run, owner, providers, layout, coverage, initialUnresolved,
		)
		if err != nil {
			return DurableFinalLayoutValidationResult{}, err
		}
		finalUnresolved := make([]int, 0, len(initialUnresolved))
		for _, position := range initialUnresolved {
			if !coverage.anyPresent(position) {
				finalUnresolved = append(finalUnresolved, position)
			}
		}
		base.UnresolvedPositions = finalUnresolved
		if !complete {
			due := v.now().UTC().Add(defaultDurableImportIncompleteDelay)
			if err := v.parkHealthRun(
				ctx, run.ID, owner, run.FencingToken, due, v.now().UTC(),
			); err != nil {
				return DurableFinalLayoutValidationResult{}, err
			}
			base.Status = ImportAdmissionAwaitConfirmation
			base.ResumeRequired = true
			base.RetryAt = &due
			return base, nil
		}
		if err := validateConfirmationDurableCoverage(
			coverage, providers, initialUnresolved,
		); err != nil {
			return DurableFinalLayoutValidationResult{}, err
		}
		phase, impact, err := v.terminalPhase(
			damagePolicy,
			finalVirtualPath, finalMetadata, provenance, finalUnresolved, spans, layout.VirtualSize,
		)
		if err != nil {
			return DurableFinalLayoutValidationResult{}, err
		}
		validation, err = v.advanceValidation(
			ctx, validation, run, phase,
			durablePositionsBitmap(finalUnresolved, coverage.total), true, true, nil,
		)
		if err != nil {
			return DurableFinalLayoutValidationResult{}, err
		}
		base.Impact = impact
		base.Status = durablePhaseStatus(validation.Phase)
		return base, nil
	default:
		return DurableFinalLayoutValidationResult{}, fmt.Errorf("unsupported durable import validation phase %q", validation.Phase)
	}
}

func (v *DurableFinalLayoutValidator) runInitialPass(
	ctx context.Context,
	run *database.HealthRun,
	owner string,
	providers []database.ProviderSnapshotEntry,
	layout *metadata.CanonicalSegmentLayout,
	coverage *durableImportCoverage,
) (bool, error) {
	for providerIndex, provider := range providers {
		targets := make([]int, 0, len(layout.Segments))
		for position := range layout.Segments {
			if providerIndex > 0 && coverage.anyPresent(position) {
				continue
			}
			if !coverage.baseTestedPosition(provider, position) {
				targets = append(targets, position)
			}
		}
		complete, err := v.dispatchDurableSTATTargets(
			ctx, run, owner, providers, provider, database.HealthRunStageImportInitialSTAT,
			layout, coverage, targets,
		)
		if err != nil || !complete {
			return complete, err
		}
	}
	return true, nil
}

func (v *DurableFinalLayoutValidator) runConfirmationPass(
	ctx context.Context,
	run *database.HealthRun,
	owner string,
	providers []database.ProviderSnapshotEntry,
	layout *metadata.CanonicalSegmentLayout,
	coverage *durableImportCoverage,
	initialUnresolved []int,
) (bool, error) {
	for _, provider := range providers {
		targets := make([]int, 0, len(initialUnresolved))
		for _, position := range initialUnresolved {
			if !coverage.baseTestedPosition(provider, position) {
				targets = append(targets, position)
			}
		}
		complete, err := v.dispatchDurableSTATTargets(
			ctx, run, owner, providers, provider, database.HealthRunStageImportConfirmationSTAT,
			layout, coverage, targets,
		)
		if err != nil || !complete {
			return complete, err
		}
	}
	return true, nil
}

type durableSTATObservation struct {
	position int
	result   TargetedSTATResult
}

type durableImportLeaseHeartbeatError struct {
	cause error
}

func (e *durableImportLeaseHeartbeatError) Error() string {
	return "durable import validation lost its run lease"
}

func (e *durableImportLeaseHeartbeatError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (v *DurableFinalLayoutValidator) dispatchDurableSTATTargets(
	ctx context.Context,
	run *database.HealthRun,
	owner string,
	providers []database.ProviderSnapshotEntry,
	provider database.ProviderSnapshotEntry,
	stage string,
	layout *metadata.CanonicalSegmentLayout,
	coverage *durableImportCoverage,
	targets []int,
) (bool, error) {
	for first := 0; first < len(targets); {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		last := first + 1
		for last < len(targets) && last-first < durableImportChunkTargetLimit &&
			targets[last]-targets[first] < durableImportChunkSpanLimit {
			last++
		}
		requests := make([]TargetedSTATRequest, 0, last-first)
		requested := make(map[int]struct{}, last-first)
		for _, position := range targets[first:last] {
			requests = append(requests, TargetedSTATRequest{
				Position: position, MessageID: layout.Segments[position].MessageID,
			})
			requested[position] = struct{}{}
		}
		results, transportErr := v.targetedSTATWithLeaseHeartbeat(
			ctx,
			run,
			owner,
			TargetedSTATProvider{
				ID: provider.ProviderID, Generation: provider.ProviderGeneration,
				ActivationEpoch: provider.ProviderActivationEpoch,
			},
			requests,
		)
		if transportErr != nil {
			var heartbeatErr *durableImportLeaseHeartbeatError
			if errors.As(transportErr, &heartbeatErr) {
				return false, heartbeatErr
			}
			if err := ctx.Err(); err != nil {
				return false, err
			}
			return false, nil
		}
		if len(results) != len(requests) {
			return false, nil
		}
		byPosition := make(map[int]TargetedSTATResult, len(results))
		for _, observation := range results {
			if _, ok := requested[observation.Position]; !ok {
				return false, nil
			}
			if _, duplicate := byPosition[observation.Position]; duplicate ||
				!validTerminalTargetedSTAT(observation.Result) {
				return false, nil
			}
			byPosition[observation.Position] = observation.Result
		}
		batch := make([]durableSTATObservation, 0, len(requests))
		for _, request := range requests {
			result, ok := byPosition[request.Position]
			if !ok {
				return false, nil
			}
			batch = append(batch, durableSTATObservation{position: request.Position, result: result})
		}
		if err := v.commitDurableSTATBatch(
			ctx, run, owner, providers, provider, stage, coverage, batch,
		); err != nil {
			return false, err
		}
		if _, err := v.repository.RenewHealthRunLease(
			ctx, run.ID, owner, run.FencingToken, v.leaseTTL,
		); err != nil {
			return false, err
		}
		first = last
	}
	return true, nil
}

func (v *DurableFinalLayoutValidator) targetedSTATWithLeaseHeartbeat(
	ctx context.Context,
	run *database.HealthRun,
	owner string,
	provider TargetedSTATProvider,
	requests []TargetedSTATRequest,
) ([]TargetedSTATObservation, error) {
	dispatchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	heartbeatDone := make(chan error, 1)
	interval := v.leaseTTL / 3
	if interval <= 0 {
		interval = time.Millisecond
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				heartbeatDone <- nil
				return
			case <-dispatchCtx.Done():
				heartbeatDone <- nil
				return
			case <-ticker.C:
				if _, err := v.repository.RenewHealthRunLease(
					dispatchCtx, run.ID, owner, run.FencingToken, v.leaseTTL,
				); err != nil {
					heartbeatDone <- err
					cancel()
					return
				}
			}
		}
	}()

	results, transportErr := v.transport.TargetedSTAT(dispatchCtx, provider, requests)
	close(done)
	heartbeatErr := <-heartbeatDone
	if heartbeatErr != nil {
		return nil, &durableImportLeaseHeartbeatError{cause: heartbeatErr}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return results, transportErr
}

func validTerminalTargetedSTAT(result TargetedSTATResult) bool {
	if result.AdmissionWait < 0 || result.PoolQueue < 0 || result.PipelineWait < 0 || result.ResponseService < 0 ||
		!validTargetedSTATCause(result.CauseClass) ||
		result.CauseClass == TargetedSTATCauseBreakerOpen ||
		(result.ResponseCode != 0 && (result.ResponseCode < 100 || result.ResponseCode > 599)) {
		return false
	}
	switch result.Outcome {
	case nntppool.OutcomeSuccess, nntppool.OutcomeHardArticleAbsence,
		nntppool.OutcomeTemporaryFailure, nntppool.OutcomeProviderUnavailable,
		nntppool.OutcomeTransportFailure, nntppool.OutcomeInconclusive:
	default:
		return false
	}
	switch result.ResponseCode {
	case 423, 430:
		if result.Outcome != nntppool.OutcomeHardArticleAbsence {
			return false
		}
	case 451:
		if result.Outcome != nntppool.OutcomeTemporaryFailure {
			return false
		}
	default:
		if result.Outcome == nntppool.OutcomeHardArticleAbsence {
			return false
		}
	}
	switch result.CompletionDisposition {
	case ImportCheckDispositionAttempted:
		return result.Outcome != nntppool.OutcomeProviderUnavailable
	case ImportCheckDispositionExplicitUnavailable:
		return result.Outcome == nntppool.OutcomeProviderUnavailable
	default:
		return false
	}
}

func validTargetedSTATCause(cause TargetedSTATCauseClass) bool {
	switch cause {
	case TargetedSTATCauseNone, TargetedSTATCauseAuthentication, TargetedSTATCauseQuota,
		TargetedSTATCauseServiceUnavailable, TargetedSTATCauseBreakerOpen,
		TargetedSTATCauseTimeout, TargetedSTATCauseTransport, TargetedSTATCauseUnknown:
		return true
	default:
		return false
	}
}

func (v *DurableFinalLayoutValidator) commitDurableSTATBatch(
	ctx context.Context,
	run *database.HealthRun,
	owner string,
	providers []database.ProviderSnapshotEntry,
	provider database.ProviderSnapshotEntry,
	stage string,
	coverage *durableImportCoverage,
	observations []durableSTATObservation,
) error {
	start := int64(observations[0].position)
	end := int64(observations[len(observations)-1].position + 1)
	count := end - start
	bitmapLength := durableBitmapLength(count)
	commit := database.HealthChunkCommit{
		ChunkID: durableImportIdentity(
			"chunk", run.ID, stage, provider.ProviderID,
			fmt.Sprint(provider.ProviderGeneration), fmt.Sprint(provider.ProviderActivationEpoch),
			fmt.Sprint(start), fmt.Sprint(count),
		),
		RunID: run.ID, LeaseOwner: owner, FencingToken: run.FencingToken,
		ProviderID: provider.ProviderID, ProviderGeneration: provider.ProviderGeneration,
		ProviderActivationEpoch: provider.ProviderActivationEpoch,
		Stage:                   stage, ObservationKind: database.HealthObservationSTAT,
		SegmentStart: start, SegmentCount: count, CursorSegment: end,
		TestedBitmap: make([]byte, bitmapLength), PresentBitmap: make([]byte, bitmapLength),
		AbsentBitmap: make([]byte, bitmapLength), CorruptBitmap: make([]byte, bitmapLength),
		TemporaryBitmap: make([]byte, bitmapLength), InconclusiveBitmap: make([]byte, bitmapLength),
		ResolvedBitmap: make([]byte, bitmapLength),
		CommittedAt:    v.now().UTC(),
	}
	for _, observation := range observations {
		relative := int64(observation.position) - start
		if relative < 0 || relative >= count {
			return fmt.Errorf("durable import target is outside its committed range")
		}
		durableSetBitmap(commit.TestedBitmap, relative)
		commit.ProviderChecksDelta++
		switch observation.result.Outcome {
		case nntppool.OutcomeSuccess:
			durableSetBitmap(commit.PresentBitmap, relative)
			durableSetBitmap(commit.ResolvedBitmap, relative)
			commit.ResolvedDelta++
		case nntppool.OutcomeHardArticleAbsence:
			durableSetBitmap(commit.AbsentBitmap, relative)
			if durableImportAllProvidersAbsentAfter(
				coverage, providers, provider, observation.position,
			) {
				durableSetBitmap(commit.ResolvedBitmap, relative)
				commit.ResolvedDelta++
			}
			commit.MissingCandidatesDelta++
		case nntppool.OutcomeTemporaryFailure:
			durableSetBitmap(commit.TemporaryBitmap, relative)
			commit.InconclusiveDelta++
		default:
			durableSetBitmap(commit.InconclusiveBitmap, relative)
			commit.InconclusiveDelta++
		}
		var responseCode *int
		if observation.result.ResponseCode != 0 {
			code := observation.result.ResponseCode
			responseCode = &code
		}
		commit.Attempts = append(commit.Attempts, database.HealthAttemptEvidence{
			IdempotencyKey: durableImportIdentity(
				"attempt", commit.ChunkID, fmt.Sprint(observation.position),
			),
			SegmentIndex: int64(observation.position), Operation: string(nntppool.OperationStat),
			Outcome: string(observation.result.Outcome), ResponseCode: responseCode,
			BodyValidation: string(nntppool.BodyValidationNotApplicable),
			CauseClass:     string(observation.result.CauseClass),
			AdmissionWait:  observation.result.AdmissionWait, PoolQueue: observation.result.PoolQueue,
			PipelineWait:    observation.result.PipelineWait,
			ResponseService: observation.result.ResponseService, ObservedAt: commit.CommittedAt,
		})
	}
	if _, err := v.repository.CommitHealthChunk(ctx, commit); err != nil {
		return err
	}
	v.notifyHealthChanged()
	key := durableProviderKey(provider)
	durableOrRelative(coverage.tested[key], commit.TestedBitmap, start, count)
	durableOrRelative(coverage.present[key], commit.PresentBitmap, start, count)
	durableOrRelative(coverage.absent[key], commit.AbsentBitmap, start, count)
	if stage == coverage.baseStage {
		durableOrRelative(coverage.baseTested[key], commit.TestedBitmap, start, count)
	}
	return nil
}

func durableImportAllProvidersAbsentAfter(
	coverage *durableImportCoverage,
	providers []database.ProviderSnapshotEntry,
	current database.ProviderSnapshotEntry,
	position int,
) bool {
	if coverage == nil || len(providers) == 0 {
		return false
	}
	currentKey := durableProviderKey(current)
	for _, provider := range providers {
		key := durableProviderKey(provider)
		if key == currentKey {
			continue
		}
		if !durableBitmapSet(coverage.absent[key], int64(position)) {
			return false
		}
	}
	return true
}

func (v *DurableFinalLayoutValidator) advanceValidation(
	ctx context.Context,
	validation *database.ImportValidation,
	run *database.HealthRun,
	phase database.ImportValidationPhase,
	unresolved []byte,
	initialComplete, secondComplete bool,
	due *time.Time,
) (*database.ImportValidation, error) {
	write := database.ImportValidationWrite{
		ID: validation.ID, QueueItemID: validation.QueueItemID,
		FileRevisionID: validation.FileRevisionID, RunID: validation.RunID,
		Phase: phase, DamagePolicy: validation.DamagePolicy,
		ConfirmationDueAt: due, UnresolvedSegments: durableBitmapPopulation(unresolved),
		UnresolvedBitmap:    append([]byte(nil), unresolved...),
		InitialPassComplete: initialComplete, SecondPassComplete: secondComplete,
		LeaseOwner: dereferenceString(run.LeaseOwner), FencingToken: run.FencingToken,
		CreatedAt: validation.CreatedAt, UpdatedAt: v.now().UTC(),
	}
	updated, err := v.repository.UpsertImportValidation(ctx, write)
	if err == nil {
		v.notifyHealthChanged()
	}
	return updated, err
}

func (v *DurableFinalLayoutValidator) parkHealthRun(
	ctx context.Context,
	runID string,
	owner string,
	fencingToken int64,
	notBefore time.Time,
	at time.Time,
) error {
	err := v.repository.ParkHealthRun(
		ctx, runID, owner, fencingToken, notBefore, at,
	)
	if err == nil {
		v.notifyHealthChanged()
	}
	return err
}

func dereferenceString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func initialDurableUnresolved(
	coverage *durableImportCoverage,
	_ []database.ProviderSnapshotEntry,
) []int {
	unresolved := make([]int, 0)
	for position := 0; position < int(coverage.total); position++ {
		if !coverage.anyPresent(position) {
			unresolved = append(unresolved, position)
		}
	}
	return unresolved
}

func validateInitialDurableCoverage(
	coverage *durableImportCoverage,
	providers []database.ProviderSnapshotEntry,
	unresolved []int,
) error {
	for position := 0; position < int(coverage.total); position++ {
		if !coverage.testedPosition(providers[0], position) {
			return fmt.Errorf("initial import pass does not cover the first provider")
		}
	}
	for _, position := range unresolved {
		for _, provider := range providers {
			if !coverage.testedPosition(provider, position) {
				return fmt.Errorf("initial import pass omits an unresolved provider target")
			}
		}
	}
	return nil
}

func validateConfirmationDurableCoverage(
	coverage *durableImportCoverage,
	providers []database.ProviderSnapshotEntry,
	initialUnresolved []int,
) error {
	target := durablePositionsBitmap(initialUnresolved, coverage.total)
	for _, provider := range providers {
		tested := coverage.tested[durableProviderKey(provider)]
		if !durableSameBitmap(tested, target) {
			return fmt.Errorf("confirmation pass must test the exact unresolved set on every provider")
		}
	}
	return nil
}

func (v *DurableFinalLayoutValidator) terminalPhase(
	damagePolicy config.ImportDamagePolicy,
	finalVirtualPath string,
	finalMetadata *metapb.FileMetadata,
	provenance FinalLayoutProvenance,
	unresolved []int,
	spans []int64,
	fileSize int64,
) (database.ImportValidationPhase, holes.Impact, error) {
	if len(unresolved) == 0 {
		return database.ImportValidationPhaseAccepted, holes.Impact{Verdict: holes.VerdictClean}, nil
	}
	if damagePolicy != config.ImportDamagePolicyTolerant ||
		!uncomplicatedStandaloneLayout(finalMetadata, provenance) ||
		!holes.EligibleFile(finalVirtualPath) {
		return database.ImportValidationPhaseRejected, holes.Impact{}, nil
	}
	impact, err := holes.ClassifyPositions(unresolved, spans, fileSize)
	if err != nil {
		return "", holes.Impact{}, err
	}
	if impact.Verdict == holes.VerdictDegraded {
		return database.ImportValidationPhaseHealthPending, impact, nil
	}
	return database.ImportValidationPhaseRejected, impact, nil
}

func uncomplicatedStandaloneLayout(meta *metapb.FileMetadata, provenance FinalLayoutProvenance) bool {
	return provenance.Kind == FinalLayoutProvenanceStandalone && meta != nil &&
		meta.Encryption == metapb.Encryption_NONE && len(meta.AesKey) == 0 && len(meta.AesIv) == 0 &&
		len(meta.NestedSources) == 0 && len(meta.SharedOuterSources) == 0 &&
		len(meta.ClipBoundaries) == 0
}

func terminalDurableImportResult(
	base DurableFinalLayoutValidationResult,
	validation *database.ImportValidation,
) (DurableFinalLayoutValidationResult, bool) {
	switch validation.Phase {
	case database.ImportValidationPhaseAccepted,
		database.ImportValidationPhaseHealthPending,
		database.ImportValidationPhaseRejected:
		base.Status = durablePhaseStatus(validation.Phase)
		return base, true
	default:
		return base, false
	}
}

func durablePhaseStatus(phase database.ImportValidationPhase) ImportAdmissionStatus {
	switch phase {
	case database.ImportValidationPhaseAccepted:
		return ImportAdmissionAccept
	case database.ImportValidationPhaseHealthPending:
		return ImportAdmissionHealthPending
	case database.ImportValidationPhaseRejected:
		return ImportAdmissionReject
	default:
		return ImportAdmissionAwaitConfirmation
	}
}

func durableImportIdentity(kind string, parts ...string) string {
	digest := sha256.New()
	for _, part := range append([]string{kind}, parts...) {
		fmt.Fprintf(digest, "%d:", len(part))
		_, _ = digest.Write([]byte(part))
	}
	return "import-" + kind + "-" + hex.EncodeToString(digest.Sum(nil))
}

func durableBitmapLength(total int64) int {
	if total <= 0 {
		return 0
	}
	return int((total + 7) / 8)
}

func durableSetBitmap(bitmap []byte, position int64) {
	bitmap[position/8] |= 1 << uint(position%8)
}

func durableBitmapSet(bitmap []byte, position int64) bool {
	return position >= 0 && position/8 < int64(len(bitmap)) &&
		bitmap[position/8]&(1<<uint(position%8)) != 0
}

func durableOrRelative(destination, source []byte, start, count int64) {
	for relative := int64(0); relative < count; relative++ {
		if durableBitmapSet(source, relative) {
			durableSetBitmap(destination, start+relative)
		}
	}
}

func durablePositionsBitmap(positions []int, total int64) []byte {
	bitmap := make([]byte, durableBitmapLength(total))
	for _, position := range positions {
		if position >= 0 && int64(position) < total {
			durableSetBitmap(bitmap, int64(position))
		}
	}
	return bitmap
}

func durableBitmapPositions(bitmap []byte, total int64) []int {
	positions := make([]int, 0)
	for position := int64(0); position < total; position++ {
		if durableBitmapSet(bitmap, position) {
			positions = append(positions, int(position))
		}
	}
	return positions
}

func durableBitmapPopulation(bitmap []byte) int64 {
	var count int64
	for position := int64(0); position < int64(len(bitmap))*8; position++ {
		if durableBitmapSet(bitmap, position) {
			count++
		}
	}
	return count
}

func durableSameBitmap(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
