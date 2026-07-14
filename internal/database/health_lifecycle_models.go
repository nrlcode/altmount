package database

import "time"

// HealthRunPriority orders durable run admission. Larger values are admitted
// first so promotion can be implemented monotonically without postponing work.
type HealthRunPriority int

const (
	HealthRunPriorityLow HealthRunPriority = iota
	HealthRunPriorityNormal
	HealthRunPriorityHigh
)

// ScheduledHealthRunSpec creates or promotes one active run for a logical
// target. DedupeKey is scoped by the caller and remains reusable after the
// prior run reaches a terminal state.
type ScheduledHealthRunSpec struct {
	Run                           HealthRunSpec
	DedupeKey                     string
	Priority                      HealthRunPriority
	NotBefore                     time.Time
	TargetProviderID              string
	TargetProviderGeneration      int64
	TargetProviderActivationEpoch int64
	TargetGapID                   string
}

// HealthRunSchedule is the durable admission metadata associated with a run.
type HealthRunSchedule struct {
	RunID                         string
	DedupeKey                     string
	Active                        bool
	TargetProviderID              string
	TargetProviderGeneration      int64
	TargetProviderActivationEpoch int64
	TargetGapID                   string
	Priority                      HealthRunPriority
	NotBefore                     time.Time
	CreatedAt                     time.Time
	UpdatedAt                     time.Time
}

// HealthRunChunkState is the committed unit used to reconstruct progress
// after restart. Message IDs and provider credentials are deliberately absent.
type HealthRunChunkState struct {
	ID                      string
	RunID                   string
	ProviderID              string
	ProviderGeneration      int64
	ProviderActivationEpoch int64
	Stage                   string
	ObservationKind         HealthObservationKind
	FreshTransport          bool
	SegmentStart            int64
	SegmentCount            int64
	TestedBitmap            []byte
	PresentBitmap           []byte
	AbsentBitmap            []byte
	CorruptBitmap           []byte
	TemporaryBitmap         []byte
	InconclusiveBitmap      []byte
	ResolvedBitmap          []byte
	FencingToken            int64
	ResolvedDelta           int64
	ProviderChecksDelta     int64
	MissingCandidatesDelta  int64
	InconclusiveDelta       int64
	CommittedAt             time.Time
}

// HealthProviderCoverageState is durable positive/tested coverage attributable
// to a committed run chunk.
type HealthProviderCoverageState struct {
	ID                      string
	FileRevisionID          string
	ProviderID              string
	ProviderGeneration      int64
	ProviderActivationEpoch int64
	ObservationKind         HealthObservationKind
	SegmentStart            int64
	SegmentCount            int64
	TestedBitmap            []byte
	PresentBitmap           []byte
	ResolvedBitmap          []byte
	SourceChunkID           string
	ObservedAt              time.Time
}

// HealthRunRetryState is the restart-safe staged retry state associated with a
// committed run. Provider identity is the AltMount durable identity only.
type HealthRunRetryState struct {
	RetryKey                string
	SourceChunkID           string
	FileRevisionID          string
	ProviderID              string
	ProviderGeneration      int64
	ProviderActivationEpoch int64
	SegmentStart            int64
	SegmentCount            int64
	Outcome                 string
	Attempt                 int
	NextAttemptAt           time.Time
	Exhausted               bool
	UpdatedAt               time.Time
}

// HealthRunResumeState contains only atomically committed state. Workers must
// not reconstruct progress from in-memory counters after a restart.
type HealthRunResumeState struct {
	Run      HealthRun
	Chunks   []HealthRunChunkState
	Coverage []HealthProviderCoverageState
	Retries  []HealthRunRetryState
}

type ImportDamagePolicy string

const (
	ImportDamagePolicyStrict   ImportDamagePolicy = "strict"
	ImportDamagePolicyTolerant ImportDamagePolicy = "tolerant"
)

type ImportValidationPhase string

const (
	ImportValidationPhaseInitialPass      ImportValidationPhase = "initial_pass"
	ImportValidationPhaseConfirmationWait ImportValidationPhase = "confirmation_wait"
	ImportValidationPhaseConfirmationPass ImportValidationPhase = "confirmation_pass"
	ImportValidationPhaseAccepted         ImportValidationPhase = "accepted"
	ImportValidationPhaseHealthPending    ImportValidationPhase = "health_pending"
	ImportValidationPhaseRejected         ImportValidationPhase = "rejected"
)

// ImportValidationWrite records one final canonical file produced by a queue
// item. DamagePolicy and the run/layout binding are immutable after creation.
type ImportValidationWrite struct {
	ID                  string
	QueueItemID         int64
	FileRevisionID      string
	RunID               string
	Phase               ImportValidationPhase
	DamagePolicy        ImportDamagePolicy
	ConfirmationDueAt   *time.Time
	UnresolvedSegments  int64
	UnresolvedBitmap    []byte
	InitialPassComplete bool
	SecondPassComplete  bool
	LeaseOwner          string
	FencingToken        int64
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type ImportValidation struct {
	ID                  string
	QueueItemID         int64
	FileRevisionID      string
	RunID               string
	Phase               ImportValidationPhase
	DamagePolicy        ImportDamagePolicy
	ConfirmationDueAt   *time.Time
	UnresolvedSegments  int64
	UnresolvedBitmap    []byte
	InitialPassComplete bool
	SecondPassComplete  bool
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// CompletedImportSTATCoverage distinguishes clean reusable import coverage
// from a tolerant import whose exact unresolved positions need immediate
// background confirmation.
type CompletedImportSTATCoverage struct {
	ValidationID        string
	RunID               string
	ProviderSnapshotID  string
	Reusable            bool
	HealthPending       bool
	UnresolvedPositions []int64
}

type HealthPendingFinalization struct {
	Settled   bool
	Recovered bool
}
