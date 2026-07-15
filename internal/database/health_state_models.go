package database

import (
	"time"
)

// ProviderRole is the durable configured provider tier. Later primaries remain
// primary; backups are a distinct failure-only tier.
type ProviderRole string

const (
	ProviderRolePrimary ProviderRole = "primary"
	ProviderRoleBackup  ProviderRole = "backup"
)

type FileRevisionSpec struct {
	FilePath          string
	LayoutFingerprint string
	VirtualSize       int64
	SegmentCount      int64
}

type HealthFileRevision struct {
	ID                string
	FileHealthID      int64
	LayoutFingerprint string
	VirtualSize       int64
	SegmentCount      int64
	Active            bool
	CreatedAt         time.Time
	ActivatedAt       time.Time
}

type ProviderSpec struct {
	// StableID accepts an already-issued ID, including pre-PR4 compatibility
	// IDs. When empty, the registry creates a UUID or unambiguously relinks a
	// retained provider with the same normalized endpoint/account identity.
	StableID    string
	DisplayName string
	Endpoint    string
	Port        int
	Account     string
	Role        ProviderRole
	Order       int
}

type HealthProvider struct {
	ID                string
	DisplayName       string
	Role              ProviderRole
	Order             int
	Active            bool
	CurrentGeneration int64
	ActivationEpoch   int64
	ActivatedAt       time.Time
	TombstonedAt      *time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type HealthProviderGeneration struct {
	ProviderID          string
	Generation          int64
	Endpoint            string
	Port                int
	Account             string
	IdentityFingerprint string
	CreatedAt           time.Time
}

type ProviderSnapshotEntry struct {
	ProviderID              string
	ProviderGeneration      int64
	ProviderActivationEpoch int64
	Role                    ProviderRole
	Order                   int
}

type ProviderSnapshot struct {
	ID        string
	CreatedAt time.Time
	Entries   []ProviderSnapshotEntry
}

type HealthRunStatus string

const (
	HealthRunPending   HealthRunStatus = "pending"
	HealthRunRunning   HealthRunStatus = "running"
	HealthRunPaused    HealthRunStatus = "paused"
	HealthRunCanceled  HealthRunStatus = "canceled"
	HealthRunCompleted HealthRunStatus = "completed"
	HealthRunFailed    HealthRunStatus = "failed"
)

type HealthRunSpec struct {
	ID                 string
	FileRevisionID     string
	ProviderSnapshotID string
	Trigger            string
	Mode               string
	TotalSegments      int64
	CreatedAt          time.Time
}

type HealthRun struct {
	ID                        string
	FileRevisionID            string
	ProviderSnapshotID        string
	Trigger                   string
	Mode                      string
	Status                    HealthRunStatus
	LeaseOwner                *string
	LeaseExpiresAt            *time.Time
	FencingToken              int64
	TotalSegments             int64
	ResolvedSegments          int64
	ProviderChecks            int64
	MissingCandidates         int64
	InconclusiveCount         int64
	Stage                     string
	CurrentProviderID         *string
	CurrentProviderGeneration *int64
	CursorSegment             int64
	PauseRequested            bool
	CancelRequested           bool
	CreatedAt                 time.Time
	StartedAt                 *time.Time
	UpdatedAt                 time.Time
	CompletedAt               *time.Time
	LastError                 string
}

type HealthAttemptEvidence struct {
	IdempotencyKey  string
	SegmentIndex    int64
	Operation       string
	Outcome         string
	ResponseCode    *int
	BodyValidation  string
	CauseClass      string
	AdmissionWait   time.Duration
	PoolQueue       time.Duration
	PipelineWait    time.Duration
	ResponseService time.Duration
	ObservedAt      time.Time
}

type GapCause string

const (
	GapCauseAbsent  GapCause = "absent"
	GapCauseCorrupt GapCause = "corrupt"
)

type HealthConfirmationEvent struct {
	IdempotencyKey string
	SegmentIndex   int64
	Cause          GapCause
	ObservedAt     time.Time
}

type HealthRetryState struct {
	RetryKey      string
	SegmentStart  int64
	SegmentCount  int64
	Outcome       string
	Attempt       int
	NextAttemptAt time.Time
	Exhausted     bool
}

// HealthObservationKind distinguishes a STAT presence hint from a fully
// decoded and integrity-validated BODY. The distinction is durable because a
// STAT-present result must never clear corrupt-BODY evidence.
type HealthObservationKind string

const (
	HealthObservationSTAT          HealthObservationKind = "stat"
	HealthObservationValidatedBody HealthObservationKind = "validated_body"
)

// Health-run stages used by import admission are a durable database contract:
// phase transitions are derived from chunks committed under these exact names.
const (
	HealthRunStageImportInitialSTAT      = "import_initial_stat"
	HealthRunStageImportConfirmationSTAT = "import_confirmation_stat"
)

type HealthChunkCommit struct {
	ChunkID                 string
	RunID                   string
	LeaseOwner              string
	FencingToken            int64
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
	CursorSegment           int64
	ResolvedDelta           int64
	ProviderChecksDelta     int64
	MissingCandidatesDelta  int64
	InconclusiveDelta       int64
	CommittedAt             time.Time
	Attempts                []HealthAttemptEvidence
	Confirmations           []HealthConfirmationEvent
	Retry                   *HealthRetryState
}

type GapKind string

const (
	GapKindProvisional       GapKind = "provisional"
	GapKindConfirmedAbsent   GapKind = "confirmed_absent"
	GapKindConfirmedUnusable GapKind = "confirmed_unusable"
	GapKindLegacyUnverified  GapKind = "legacy_unverified"
)

type GapStatus string

const (
	GapStatusActive  GapStatus = "active"
	GapStatusCleared GapStatus = "cleared"
	GapStatusDormant GapStatus = "dormant"
)

type GapProviderCause struct {
	ProviderID              string
	ProviderGeneration      int64
	ProviderActivationEpoch int64
	Cause                   GapCause
	ConfirmationCount       int
	ConfirmedAt             time.Time
}

type GapRangeWrite struct {
	ID             string
	FileRevisionID string
	Kind           GapKind
	StartSegment   int64
	SegmentCount   int64
	Status         GapStatus
	CreatedAt      time.Time
	ConfirmedAt    *time.Time
	ClearedAt      *time.Time
	Causes         []GapProviderCause
	RunID          string
	LeaseOwner     string
	FencingToken   int64
}

type HealthGapRange struct {
	ID                 string
	FileRevisionID     string
	Kind               GapKind
	StartSegment       int64
	SegmentCount       int64
	Episode            int64
	Status             GapStatus
	CreatedAt          time.Time
	ConfirmedAt        *time.Time
	ClearedAt          *time.Time
	RevalidationStep   int
	NextRevalidationAt *time.Time
	LastRevalidationAt *time.Time
	Causes             []GapProviderCause
}

// ImportActivationRollback identifies one exact candidate/prior revision swap
// without carrying metadata bytes, article identities, or provider secrets.
// FilePath is the virtual library path needed by the filesystem journal.
type ImportActivationRollback struct {
	QueueItemID                int64
	FilePath                   string
	CandidateRevisionID        string
	CandidateLayoutFingerprint string
	PriorRevisionID            string
	PriorLayoutFingerprint     string
}

// InactiveImportCandidate identifies an admitted queue-owned revision that
// has not crossed DB activation. It carries only structural recovery identity;
// private metadata bytes and StoreRef paths remain in the filesystem journal.
type InactiveImportCandidate struct {
	QueueItemID         int64
	FilePath            string
	CandidateRevisionID string
	LayoutFingerprint   string
}

// GapRevalidationWork is one absolute aging milestone for a confirmed gap.
// Step is the number of conclusive milestones already completed.
type GapRevalidationWork struct {
	Gap           HealthGapRange
	TotalSegments int64
	Step          int
	NotBefore     time.Time
}

type GapRevalidationFinalization struct {
	Gap      HealthGapRange
	Advanced bool
	Dormant  bool
}

// ProviderActivationWork identifies one restart-discoverable sparse sweep.
// GapID is set for known-gap work; otherwise the worker derives only globally
// unresolved positions not yet tested by Provider.
type ProviderActivationWork struct {
	RevisionID    string
	TotalSegments int64
	Provider      ProviderSnapshotEntry
	GapID         string
}

type SyntheticOutputWrite struct {
	ID             string
	GapID          string
	FileRevisionID string
	ByteStart      int64
	ByteEnd        int64
	EmittedAt      time.Time
}

type CacheRecoveryStatus string

const (
	CacheRecoveryClean      CacheRecoveryStatus = "clean"
	CacheRecoverySynthetic  CacheRecoveryStatus = "synthetic"
	CacheRecoveryPending    CacheRecoveryStatus = "pending"
	CacheRecoveryInProgress CacheRecoveryStatus = "in_progress"
	CacheRecoveryFailed     CacheRecoveryStatus = "failed"
)

type CacheRecoveryState struct {
	FileRevisionID  string
	Status          CacheRecoveryStatus
	RetryCount      int
	NextRetryAt     *time.Time
	LastError       *string
	ContentRevision int64
	UpdatedAt       time.Time
}
