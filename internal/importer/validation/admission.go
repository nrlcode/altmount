package validation

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/holes"
	"github.com/javi11/nntppool/v4"
)

const importConfirmationDelay = 30 * time.Second

// ImportAvailabilityProvider identifies one active provider activation in its
// immutable dispatch order. Generation scopes endpoint/account changes;
// ActivationEpoch scopes removal and reactivation of the same generation.
type ImportAvailabilityProvider struct {
	ID              string
	Generation      int64
	ActivationEpoch int64
}

// ImportAvailabilityAttempt is the pre-PR5 flattened attempt shape. It remains
// source-compatible for in-tree callers while they migrate, but admission never
// treats it as terminal evidence because a completed raw attempt may be followed
// by a canceled retry.
type ImportAvailabilityAttempt struct {
	ProviderID         string
	ProviderGeneration int64
	Outcome            nntppool.OutcomeKind
	ResponseCode       *int
	CauseClass         string
	Completed          bool
}

// ImportCheckDisposition says whether the complete logical provider check may
// close one provider/pass target. Raw transport attempts are diagnostic only.
type ImportCheckDisposition string

const (
	// ImportCheckDispositionAttempted means the intended provider operation
	// reached a terminal transport result.
	ImportCheckDispositionAttempted ImportCheckDisposition = "attempted"
	// ImportCheckDispositionExplicitUnavailable means a typed provider-level
	// state such as authentication, quota, or service unavailability conclusively
	// prevented the operation. It is distinct from breaker-skipped work.
	ImportCheckDispositionExplicitUnavailable ImportCheckDisposition = "explicit_unavailable"
	// ImportCheckDispositionIncomplete covers cancellation, omitted results,
	// breaker-skipped work, and any target that must be resumed.
	ImportCheckDispositionIncomplete ImportCheckDisposition = "incomplete"
)

// ImportProviderCheck is the single policy-authoritative result for one logical
// position/pass/provider target. RawAttempts retain transport diagnostics but
// can neither complete a pass nor authorize acceptance or rejection.
type ImportProviderCheck struct {
	ProviderID              string
	ProviderGeneration      int64
	ProviderActivationEpoch int64
	Operation               nntppool.Operation
	Outcome                 nntppool.OutcomeKind
	ResponseCode            *int
	BodyValidation          nntppool.BodyValidationStatus
	CompletionDisposition   ImportCheckDisposition
	RawAttempts             []nntppool.AttemptEvidence
}

// ImportAvailabilityPosition retains both passes' terminal evidence for one
// canonical article position. The legacy offset and attempt fields are retained
// only for source compatibility; exact spans come exclusively from the final
// fingerprint-bound CanonicalUsableBytes domain.
type ImportAvailabilityPosition struct {
	Index int

	InitialChecks      []ImportProviderCheck
	ConfirmationChecks []ImportProviderCheck

	StartOffset          int64
	EndOffset            int64
	InitialAttempts      []ImportAvailabilityAttempt
	ConfirmationAttempts []ImportAvailabilityAttempt
}

// ImportAdmissionInput is a complete, side-effect-free view of one final file's
// import availability lifecycle.
type ImportAdmissionInput struct {
	DamagePolicy string
	Filename     string
	FileSize     int64

	FinalLayoutFingerprint    string
	EvidenceLayoutFingerprint string
	CanonicalUsableBytes      []int64
	// UncomplicatedStandalone is derived from final import provenance. Its zero
	// value is intentionally fail-closed for tolerant admission.
	UncomplicatedStandalone bool

	// Completion flags remain durable progress hints for callers, but policy
	// derives completeness from terminal checks and never trusts these booleans.
	InitialPassComplete bool
	SecondPassComplete  bool

	ActiveProviders []ImportAvailabilityProvider
	Positions       []ImportAvailabilityPosition
}

// ImportConfirmationTarget is durable follow-up work for one unresolved
// canonical position and active provider activation.
type ImportConfirmationTarget struct {
	Position                int
	ProviderID              string
	ProviderGeneration      int64
	ProviderActivationEpoch int64
}

// ImportAdmissionStatus is the policy-only import outcome.
type ImportAdmissionStatus string

const (
	ImportAdmissionAccept            ImportAdmissionStatus = "accept"
	ImportAdmissionAwaitConfirmation ImportAdmissionStatus = "await_confirmation"
	ImportAdmissionHealthPending     ImportAdmissionStatus = "health_pending"
	ImportAdmissionReject            ImportAdmissionStatus = "reject"
)

// ImportAdmissionDecision contains no inferred transport classification:
// unresolved evidence is copied without rewriting 451 or any other outcome.
type ImportAdmissionDecision struct {
	Status              ImportAdmissionStatus
	RetryAfter          time.Duration
	Unresolved          []ImportAvailabilityPosition
	ConfirmationTargets []ImportConfirmationTarget
	Impact              holes.Impact
}

type importProviderSnapshot struct {
	ordered []ImportAvailabilityProvider
	byID    map[string]ImportAvailabilityProvider
}

// DecideImportAdmission applies the strict-by-default two-pass import policy.
// It does not wait, dispatch network work, mutate input, or infer transport
// classifications. Any current provider success resolves a position. Rejection
// is possible only after every current provider activation has an authoritative
// terminal confirmation for every still-unresolved position.
func DecideImportAdmission(ctx context.Context, input ImportAdmissionInput) (ImportAdmissionDecision, error) {
	if err := ctx.Err(); err != nil {
		return ImportAdmissionDecision{}, err
	}

	policy, err := validateImportDamagePolicy(input.DamagePolicy)
	if err != nil {
		return ImportAdmissionDecision{}, err
	}
	if strings.TrimSpace(input.FinalLayoutFingerprint) == "" ||
		strings.TrimSpace(input.EvidenceLayoutFingerprint) == "" {
		return ImportAdmissionDecision{}, fmt.Errorf("final and evidence layout fingerprints are required")
	}
	if input.FinalLayoutFingerprint != input.EvidenceLayoutFingerprint {
		return ImportAdmissionDecision{}, fmt.Errorf("import evidence does not match the final canonical layout")
	}
	if input.FileSize <= 0 {
		return ImportAdmissionDecision{}, fmt.Errorf("import file size must be positive")
	}
	canonicalSpans := append([]int64(nil), input.CanonicalUsableBytes...)
	if err := validateCanonicalPositionSpans(canonicalSpans); err != nil {
		return ImportAdmissionDecision{}, err
	}
	// Plain standalone files have one directly comparable virtual-byte domain,
	// so tolerant admission must prove that every positional contribution sums
	// to the final file size. Nested/encrypted files retain complete physical
	// article positions, but their outer spans need not equal decrypted bytes;
	// they are never eligible for degraded admission.
	if input.UncomplicatedStandalone {
		if _, err := holes.ClassifyPositions(nil, canonicalSpans, input.FileSize); err != nil {
			return ImportAdmissionDecision{}, fmt.Errorf("invalid canonical usable-byte domain: %w", err)
		}
	}

	snapshot, err := validateImportProviders(input.ActiveProviders)
	if err != nil {
		return ImportAdmissionDecision{}, err
	}
	positions, err := cloneAndValidateImportPositions(input.Positions, canonicalSpans, snapshot)
	if err != nil {
		return ImportAdmissionDecision{}, err
	}

	initialUnresolved, initialComplete := unresolvedAfterPass(positions, snapshot, false)
	if !initialComplete {
		return ImportAdmissionDecision{
			Status:     ImportAdmissionAwaitConfirmation,
			Unresolved: unresolvedAfterEitherPass(positions, snapshot),
		}, nil
	}
	if len(initialUnresolved) == 0 {
		return ImportAdmissionDecision{Status: ImportAdmissionAccept}, nil
	}

	confirmationUnresolved := make([]ImportAvailabilityPosition, 0, len(initialUnresolved))
	for _, position := range initialUnresolved {
		success, _ := passStatus(position.ConfirmationChecks, snapshot)
		if !success {
			confirmationUnresolved = append(confirmationUnresolved, position)
		}
	}
	if len(confirmationUnresolved) == 0 {
		return ImportAdmissionDecision{Status: ImportAdmissionAccept}, nil
	}

	targets := incompleteConfirmationTargets(confirmationUnresolved, snapshot)
	if len(targets) != 0 {
		retryAfter := time.Duration(0)
		if !hasAnyCurrentConfirmationCheck(initialUnresolved, snapshot) {
			retryAfter = importConfirmationDelay
		}
		return ImportAdmissionDecision{
			Status:              ImportAdmissionAwaitConfirmation,
			RetryAfter:          retryAfter,
			Unresolved:          confirmationUnresolved,
			ConfirmationTargets: targets,
		}, nil
	}

	decision := ImportAdmissionDecision{
		Status:     ImportAdmissionReject,
		Unresolved: confirmationUnresolved,
	}
	if policy == config.ImportDamagePolicyStrict {
		return decision, nil
	}
	if !input.UncomplicatedStandalone || !holes.EligibleFile(input.Filename) {
		// Preserve an exact diagnostic impact when the physical and virtual byte
		// domains happen to be comparable, but never reject clean evidence or
		// return an input error merely because a complicated layout is not.
		if impact, classifyErr := classifyUnresolvedPositions(
			confirmationUnresolved, canonicalSpans, input.FileSize,
		); classifyErr == nil {
			decision.Impact = impact
		}
		return decision, nil
	}

	impact, err := classifyUnresolvedPositions(confirmationUnresolved, canonicalSpans, input.FileSize)
	if err != nil {
		return ImportAdmissionDecision{}, err
	}
	decision.Impact = impact
	if impact.Verdict == holes.VerdictDegraded {
		decision.Status = ImportAdmissionHealthPending
	}
	return decision, nil
}

func validateCanonicalPositionSpans(spans []int64) error {
	if len(spans) == 0 {
		return fmt.Errorf("canonical position domain must not be empty")
	}
	var total int64
	for position, span := range spans {
		if span <= 0 {
			return fmt.Errorf("canonical position %d has no exact physical span", position)
		}
		if span > int64(^uint64(0)>>1)-total {
			return fmt.Errorf("canonical physical span total overflows")
		}
		total += span
	}
	return nil
}

func validateImportDamagePolicy(value string) (config.ImportDamagePolicy, error) {
	policy := config.ImportDamagePolicy(value)
	if policy == "" {
		policy = config.ImportDamagePolicyStrict
	}
	if policy != config.ImportDamagePolicyStrict && policy != config.ImportDamagePolicyTolerant {
		return "", fmt.Errorf("import damage_policy must be one of: strict, tolerant")
	}
	return policy, nil
}

func validateImportProviders(input []ImportAvailabilityProvider) (importProviderSnapshot, error) {
	providers := append([]ImportAvailabilityProvider(nil), input...)
	if len(providers) == 0 {
		return importProviderSnapshot{}, fmt.Errorf("at least one active provider is required")
	}
	snapshot := importProviderSnapshot{
		ordered: providers,
		byID:    make(map[string]ImportAvailabilityProvider, len(providers)),
	}
	for i, provider := range providers {
		if provider.ID == "" {
			return importProviderSnapshot{}, fmt.Errorf("active provider %d has no stable identity", i)
		}
		if provider.Generation <= 0 {
			return importProviderSnapshot{}, fmt.Errorf("active provider %d has an invalid generation", i)
		}
		if provider.ActivationEpoch <= 0 {
			return importProviderSnapshot{}, fmt.Errorf("active provider %d has an invalid activation epoch", i)
		}
		if _, exists := snapshot.byID[provider.ID]; exists {
			return importProviderSnapshot{}, fmt.Errorf("active provider %d duplicates an earlier identity", i)
		}
		snapshot.byID[provider.ID] = provider
	}
	return snapshot, nil
}

func cloneAndValidateImportPositions(
	input []ImportAvailabilityPosition,
	canonicalSpans []int64,
	snapshot importProviderSnapshot,
) ([]ImportAvailabilityPosition, error) {
	if len(input) != len(canonicalSpans) {
		return nil, fmt.Errorf("availability evidence does not cover every canonical position")
	}
	positions := make([]ImportAvailabilityPosition, len(input))
	for i, position := range input {
		if position.Index != i {
			return nil, fmt.Errorf("availability evidence is not in complete canonical order at position %d", i)
		}
		if len(position.InitialAttempts) != 0 || len(position.ConfirmationAttempts) != 0 {
			return nil, fmt.Errorf("flattened raw attempts cannot serve as terminal import evidence at position %d", i)
		}

		initial, err := cloneAndValidateProviderChecks(position.InitialChecks, snapshot, i, "initial")
		if err != nil {
			return nil, err
		}
		confirmation, err := cloneAndValidateProviderChecks(position.ConfirmationChecks, snapshot, i, "confirmation")
		if err != nil {
			return nil, err
		}
		position.InitialChecks = initial
		position.ConfirmationChecks = confirmation
		positions[i] = position
	}
	return positions, nil
}

func cloneAndValidateProviderChecks(
	input []ImportProviderCheck,
	snapshot importProviderSnapshot,
	position int,
	pass string,
) ([]ImportProviderCheck, error) {
	checks := make([]ImportProviderCheck, len(input))
	seenCurrent := make(map[string]struct{}, len(snapshot.ordered))
	for i, check := range input {
		check = cloneImportProviderCheck(check)
		provider, known := snapshot.byID[check.ProviderID]
		if !known {
			return nil, fmt.Errorf("%s check %d at position %d references a provider outside the active snapshot", pass, i, position)
		}
		if !checkMatchesProvider(check, provider) {
			// Historical generations and activation epochs remain diagnostic but
			// cannot complete current work. The current activation is retargeted.
			checks[i] = check
			continue
		}
		if _, duplicate := seenCurrent[check.ProviderID]; duplicate {
			return nil, fmt.Errorf("%s evidence duplicates a terminal provider check at position %d", pass, position)
		}
		seenCurrent[check.ProviderID] = struct{}{}
		if err := validateCurrentProviderCheck(check, position, pass); err != nil {
			return nil, err
		}
		checks[i] = check
	}
	return checks, nil
}

func cloneImportProviderCheck(check ImportProviderCheck) ImportProviderCheck {
	if check.ResponseCode != nil {
		code := *check.ResponseCode
		check.ResponseCode = &code
	}
	check.RawAttempts = append([]nntppool.AttemptEvidence(nil), check.RawAttempts...)
	return check
}

func validateCurrentProviderCheck(check ImportProviderCheck, position int, pass string) error {
	if !knownImportOutcome(check.Outcome) {
		return fmt.Errorf("%s check at position %d has an unknown outcome", pass, position)
	}
	if err := validateImportCheckOperation(check, position, pass); err != nil {
		return err
	}

	switch check.CompletionDisposition {
	case ImportCheckDispositionAttempted:
		if check.Outcome == nntppool.OutcomeCancellation {
			return fmt.Errorf("%s check at position %d marks cancellation as completed", pass, position)
		}
	case ImportCheckDispositionExplicitUnavailable:
		if check.Outcome != nntppool.OutcomeProviderUnavailable {
			return fmt.Errorf("%s check at position %d has an invalid unavailable disposition", pass, position)
		}
	case ImportCheckDispositionIncomplete:
		if check.Outcome == nntppool.OutcomeSuccess {
			return fmt.Errorf("%s check at position %d marks success as incomplete", pass, position)
		}
	default:
		return fmt.Errorf("%s check at position %d has an unknown completion disposition", pass, position)
	}
	return nil
}

func validateImportCheckOperation(check ImportProviderCheck, position int, pass string) error {
	switch check.Operation {
	case nntppool.OperationStat:
		if check.BodyValidation != nntppool.BodyValidationNotApplicable {
			return fmt.Errorf("%s STAT check at position %d has invalid BODY validation state", pass, position)
		}
		if check.Outcome == nntppool.OutcomeCorruptBody {
			return fmt.Errorf("%s STAT check at position %d has a BODY-only outcome", pass, position)
		}
	case nntppool.OperationBody:
		if !knownBodyValidation(check.BodyValidation) ||
			check.BodyValidation == nntppool.BodyValidationNotApplicable {
			return fmt.Errorf("%s BODY check at position %d has an unknown validation state", pass, position)
		}
		if check.Outcome == nntppool.OutcomeSuccess &&
			check.BodyValidation != nntppool.BodyValidationValid {
			return fmt.Errorf("%s BODY check at position %d is provisional or incomplete", pass, position)
		}
	default:
		return fmt.Errorf("%s check at position %d has an unsupported operation", pass, position)
	}
	return nil
}

func knownImportOutcome(outcome nntppool.OutcomeKind) bool {
	switch outcome {
	case nntppool.OutcomeSuccess,
		nntppool.OutcomeHardArticleAbsence,
		nntppool.OutcomeTemporaryFailure,
		nntppool.OutcomeProviderUnavailable,
		nntppool.OutcomeCorruptBody,
		nntppool.OutcomeCancellation,
		nntppool.OutcomeTransportFailure,
		nntppool.OutcomeInconclusive:
		return true
	default:
		return false
	}
}

func knownBodyValidation(status nntppool.BodyValidationStatus) bool {
	switch status {
	case nntppool.BodyValidationNotApplicable,
		nntppool.BodyValidationNotRequested,
		nntppool.BodyValidationValid,
		nntppool.BodyValidationInvalid,
		nntppool.BodyValidationIncomplete:
		return true
	default:
		return false
	}
}

func checkMatchesProvider(check ImportProviderCheck, provider ImportAvailabilityProvider) bool {
	return check.ProviderID == provider.ID &&
		check.ProviderGeneration == provider.Generation &&
		check.ProviderActivationEpoch == provider.ActivationEpoch
}

func checkIsAuthoritative(check ImportProviderCheck) bool {
	return check.CompletionDisposition == ImportCheckDispositionAttempted ||
		check.CompletionDisposition == ImportCheckDispositionExplicitUnavailable
}

func passStatus(checks []ImportProviderCheck, snapshot importProviderSnapshot) (success, complete bool) {
	completed := 0
	for _, check := range checks {
		provider, known := snapshot.byID[check.ProviderID]
		if !known || !checkMatchesProvider(check, provider) {
			continue
		}
		if !checkIsAuthoritative(check) {
			continue
		}
		if check.Outcome == nntppool.OutcomeSuccess {
			return true, true
		}
		completed++
	}
	return false, completed == len(snapshot.ordered)
}

func unresolvedAfterPass(
	positions []ImportAvailabilityPosition,
	snapshot importProviderSnapshot,
	confirmation bool,
) ([]ImportAvailabilityPosition, bool) {
	unresolved := make([]ImportAvailabilityPosition, 0, len(positions))
	complete := true
	for _, position := range positions {
		checks := position.InitialChecks
		if confirmation {
			checks = position.ConfirmationChecks
		}
		success, positionComplete := passStatus(checks, snapshot)
		if success {
			continue
		}
		unresolved = append(unresolved, position)
		if !positionComplete {
			complete = false
		}
	}
	return unresolved, complete
}

func unresolvedAfterEitherPass(
	positions []ImportAvailabilityPosition,
	snapshot importProviderSnapshot,
) []ImportAvailabilityPosition {
	unresolved := make([]ImportAvailabilityPosition, 0, len(positions))
	for _, position := range positions {
		initialSuccess, _ := passStatus(position.InitialChecks, snapshot)
		confirmationSuccess, _ := passStatus(position.ConfirmationChecks, snapshot)
		if !initialSuccess && !confirmationSuccess {
			unresolved = append(unresolved, position)
		}
	}
	return unresolved
}

func incompleteConfirmationTargets(
	positions []ImportAvailabilityPosition,
	snapshot importProviderSnapshot,
) []ImportConfirmationTarget {
	targets := make([]ImportConfirmationTarget, 0, len(positions)*len(snapshot.ordered))
	for _, position := range positions {
		for _, provider := range snapshot.ordered {
			if confirmationCompletedForProvider(position.ConfirmationChecks, provider) {
				continue
			}
			targets = append(targets, ImportConfirmationTarget{
				Position:                position.Index,
				ProviderID:              provider.ID,
				ProviderGeneration:      provider.Generation,
				ProviderActivationEpoch: provider.ActivationEpoch,
			})
		}
	}
	return targets
}

func confirmationCompletedForProvider(
	checks []ImportProviderCheck,
	provider ImportAvailabilityProvider,
) bool {
	for _, check := range checks {
		if checkMatchesProvider(check, provider) && checkIsAuthoritative(check) {
			return true
		}
	}
	return false
}

func hasAnyCurrentConfirmationCheck(
	positions []ImportAvailabilityPosition,
	snapshot importProviderSnapshot,
) bool {
	for _, position := range positions {
		for _, check := range position.ConfirmationChecks {
			provider, known := snapshot.byID[check.ProviderID]
			if known && checkMatchesProvider(check, provider) {
				return true
			}
		}
	}
	return false
}

func classifyUnresolvedPositions(
	positions []ImportAvailabilityPosition,
	canonicalSpans []int64,
	fileSize int64,
) (holes.Impact, error) {
	missing := make([]int, len(positions))
	for i, position := range positions {
		missing[i] = position.Index
	}
	return holes.ClassifyPositions(missing, canonicalSpans, fileSize)
}
