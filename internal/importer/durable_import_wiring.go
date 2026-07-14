package importer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/javi11/altmount/internal/config"
	"github.com/javi11/altmount/internal/database"
	"github.com/javi11/altmount/internal/importer/admissionctx"
	"github.com/javi11/altmount/internal/importer/validation"
	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/altmount/internal/pool"
	"github.com/javi11/nntppool/v4"
)

var errDurableImportSTATIncomplete = errors.New("durable import targeted STAT is incomplete")

var errDurableImportActivationRollback = errors.New("durable import activation rollback is incomplete")

const (
	durablePrelayoutConfirmationDelay              = 30 * time.Second
	durablePrelayoutWaitingMarker                  = "pr5:prelayout_confirmation_wait"
	durablePrelayoutDueMarker                      = "pr5:prelayout_confirmation_due"
	durablePrelayoutInitialIncompleteWaitingMarker = "pr5:prelayout_initial_incomplete_wait"
	durablePrelayoutInitialRetryDueMarker          = "pr5:prelayout_initial_retry_due"
	durablePrelayoutConfirmIncompleteWaitingMarker = "pr5:prelayout_confirmation_incomplete_wait"
	durableFinalLayoutIncompleteWaitingMarker      = "pr5:final_layout_incomplete_wait"
	durableFinalLayoutIncompleteRetryDueMarker     = "pr5:final_layout_incomplete_retry_due"
	durableTerminalRollbackWaitingMarker           = "pr5:terminal_rollback_wait"
	durableTerminalRollbackDueMarker               = "pr5:terminal_rollback_due"
	durableSuccessFinalizationWaitingMarker        = "pr5:success_finalization_wait"
	durableSuccessFinalizationDueMarker            = "pr5:success_finalization_due"
)

type finalLayoutAdmissionValidator interface {
	ValidateFinalLayout(
		context.Context,
		int64,
		string,
		*metapb.FileMetadata,
		validation.FinalLayoutProvenance,
	) (validation.DurableFinalLayoutValidationResult, error)
}

type finalLayoutRevisionActivator interface {
	ActivateFileRevision(context.Context, int64, string) error
}

type durableImportHealthBroadcaster interface {
	BroadcastHealthChanged()
}

// withDurableImportIntent binds one metadata write to the queue item and the
// importer-owned structural origin that produced its final layout. The value
// is deliberately in-memory only; article identities never cross this seam.
func withDurableImportIntent(
	ctx context.Context,
	queueItemID int64,
	provenance validation.FinalLayoutProvenanceKind,
) context.Context {
	return admissionctx.WithIntent(ctx, queueItemID, provenance)
}

func withDurableImportReusableLayouts(
	ctx context.Context,
	queueItemID int64,
	layouts map[string]string,
) context.Context {
	return admissionctx.WithReusableLayouts(ctx, queueItemID, layouts)
}

func withDurableImportReusableLayoutBindings(
	ctx context.Context,
	queueItemID int64,
	layouts map[string]admissionctx.ReusableLayoutBinding,
) context.Context {
	return admissionctx.WithReusableLayoutBindings(ctx, queueItemID, layouts)
}

type durableImportWriteValidator struct {
	validator finalLayoutAdmissionValidator
	activator finalLayoutRevisionActivator
	journal   *durableImportRollbackJournal
	health    durableImportHealthBroadcaster
}

func newDurableImportWriteValidator(validator finalLayoutAdmissionValidator) *durableImportWriteValidator {
	activator, _ := validator.(finalLayoutRevisionActivator)
	return &durableImportWriteValidator{validator: validator, activator: activator}
}

// durableImportHoldError tells Service.HandleFailure that processing stopped at
// a resumable admission boundary, not at an import failure. Its error text is
// intentionally constant so transport/database details cannot reach queue
// failure logs, fallback clients, or persistent error fields.
type durableImportHoldError struct {
	retryAt             *time.Time
	resumeRequired      bool
	terminalRollback    bool
	successFinalization bool
}

func (e *durableImportHoldError) Error() string {
	return "import final-layout validation is awaiting resumable work"
}

type durableImportRejectedError struct{}

func (e *durableImportRejectedError) Error() string {
	return "import final-layout validation rejected unresolved content"
}

// durablePrelayoutValidationError represents work needed to construct the
// canonical final layout itself. It never carries article IDs, filenames, or
// provider errors, and it is intentionally distinct from reusable health
// coverage because no final fingerprint exists at this boundary.
type durablePrelayoutValidationError struct {
	confirmationRequired bool
	resumeRequired       bool
}

func (e *durablePrelayoutValidationError) Error() string {
	if e != nil && e.resumeRequired {
		return "import final-layout prerequisite checking is incomplete"
	}
	return "import final-layout prerequisites remained unavailable after confirmation"
}

func (v *durableImportWriteValidator) PrepareMetadataWrite(
	ctx context.Context,
	virtualPath string,
	meta *metapb.FileMetadata,
) (metadata.MetadataWritePermit, error) {
	intent, ok := admissionctx.FromContext(ctx)
	if !ok || v == nil || v.validator == nil {
		return nil, nil
	}

	provenance := finalLayoutProvenance(intent.Provenance, meta)
	result, err := v.validator.ValidateFinalLayout(
		ctx,
		intent.QueueItemID,
		virtualPath,
		meta,
		validation.FinalLayoutProvenance{Kind: provenance},
	)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, &durableImportHoldError{resumeRequired: true}
	}
	switch result.Status {
	case validation.ImportAdmissionAccept, validation.ImportAdmissionHealthPending:
		if result.FileRevisionID == "" || v.activator == nil {
			return nil, &durableImportHoldError{resumeRequired: true}
		}
		layout, layoutErr := metadata.ResolveCanonicalSegmentLayout(meta)
		if v.journal != nil && (layoutErr != nil || layout.Fingerprint == "") {
			return nil, &durableImportHoldError{resumeRequired: true}
		}
		layoutFingerprint := ""
		if layoutErr == nil {
			layoutFingerprint = layout.Fingerprint
		}
		reuseVisible := false
		if binding, reusable := admissionctx.ReusableLayout(ctx, virtualPath); reusable && binding.ActivationPending {
			reuseVisible = layoutFingerprint != "" && layoutFingerprint == binding.Fingerprint
		}
		return &durableImportWritePermit{
			activator: v.activator, revisionID: result.FileRevisionID,
			queueItemID: intent.QueueItemID, virtualPath: virtualPath,
			layoutFingerprint: layoutFingerprint, reuseVisible: reuseVisible,
			journal: v.journal, health: v.health,
		}, nil
	case validation.ImportAdmissionReject:
		return nil, &durableImportRejectedError{}
	case validation.ImportAdmissionAwaitConfirmation:
		return nil, &durableImportHoldError{
			retryAt: result.RetryAt, resumeRequired: result.ResumeRequired,
		}
	default:
		return nil, &durableImportHoldError{resumeRequired: true}
	}
}

type durableImportWritePermit struct {
	activator   finalLayoutRevisionActivator
	revisionID  string
	queueItemID int64
	virtualPath string
	// layoutFingerprint binds the private candidate snapshot to the exact
	// canonical layout admitted by the validator before its DB activation.
	layoutFingerprint string
	reuseVisible      bool
	journal           *durableImportRollbackJournal
	health            durableImportHealthBroadcaster
}

func (p *durableImportWritePermit) ReuseVisibleMetadata() bool {
	return p != nil && p.reuseVisible
}

func (p *durableImportWritePermit) DurableCandidateMetadata() bool {
	return p != nil && p.journal != nil
}

func (p *durableImportWritePermit) PriorMetadataSnapshot(
	_ context.Context,
	virtualPath string,
) ([]byte, bool, error) {
	if p == nil || p.journal == nil || virtualPath != p.virtualPath {
		return nil, false, errDurableImportRollbackState
	}
	snapshot, err := p.journal.Load(p.queueItemID, virtualPath)
	if err != nil {
		return nil, false, err
	}
	return append([]byte(nil), snapshot.priorBytes...), snapshot.priorExists, nil
}

func (p *durableImportWritePermit) JournalPriorMetadata(
	ctx context.Context,
	virtualPath string,
	priorBytes []byte,
	priorExists bool,
	priorFingerprint string,
	priorStoreRef string,
) error {
	if p == nil || p.journal == nil {
		return nil
	}
	if virtualPath != p.virtualPath || p.queueItemID <= 0 {
		return errDurableImportRollbackState
	}
	return p.journal.Record(
		ctx, p.queueItemID, virtualPath, priorBytes, priorExists,
		priorFingerprint, priorStoreRef,
	)
}

func (p *durableImportWritePermit) ValidateMetadataRollbackJournal(
	_ context.Context,
	virtualPath string,
) error {
	if p == nil || p.journal == nil {
		return nil
	}
	if virtualPath != p.virtualPath {
		return errDurableImportRollbackState
	}
	return p.journal.Validate(p.queueItemID, virtualPath)
}

func (p *durableImportWritePermit) PrepareCandidateMetadata(
	ctx context.Context,
	virtualPath string,
	storeRef string,
) error {
	if p == nil {
		return errDurableImportRollbackState
	}
	if p.journal == nil {
		return nil
	}
	if virtualPath != p.virtualPath ||
		p.queueItemID <= 0 || p.revisionID == "" || p.layoutFingerprint == "" {
		return errDurableImportRollbackState
	}
	if err := p.journal.RecordCandidateIntent(
		ctx, p.queueItemID, virtualPath, p.revisionID, p.layoutFingerprint, storeRef,
	); err != nil {
		return err
	}
	return p.journal.ApplyCandidateStoreRefIncrement(
		ctx, p.queueItemID, p.revisionID, storeRef,
	)
}

func (p *durableImportWritePermit) FinalizeMetadataWrite(ctx context.Context) error {
	if p == nil || p.activator == nil || p.revisionID == "" ||
		(p.journal != nil && p.layoutFingerprint == "") {
		return &durableImportHoldError{resumeRequired: true}
	}
	// Persist the exact candidate bytes before making its revision active. This
	// closes the only recovery gap in which Begin rollback could switch SQL to
	// the prior revision but be unable to republish the visible candidate if a
	// filesystem restore subsequently failed.
	if p.journal != nil {
		candidate, err := p.journal.RecordCandidate(p.queueItemID, p.virtualPath)
		if err != nil || !candidate.priorExists ||
			candidate.priorFingerprint != p.layoutFingerprint {
			return &durableImportHoldError{resumeRequired: true}
		}
	}
	stateCtx, cancel := durableImportStateContext(ctx)
	defer cancel()
	if err := p.activator.ActivateFileRevision(
		stateCtx, p.queueItemID, p.revisionID,
	); err != nil {
		return &durableImportHoldError{resumeRequired: true}
	}
	if p.health != nil {
		p.health.BroadcastHealthChanged()
	}
	return nil
}

func finalLayoutProvenance(
	base validation.FinalLayoutProvenanceKind,
	meta *metapb.FileMetadata,
) validation.FinalLayoutProvenanceKind {
	if meta != nil && len(meta.ClipBoundaries) > 0 {
		return validation.FinalLayoutProvenanceVirtualConcatenation
	}
	// Bare-ISO expansion is known directly by its dispatch path. Preserve that
	// stronger origin even though expanded metadata commonly has nested spans.
	if base == validation.FinalLayoutProvenanceISOExpansion {
		return base
	}
	if meta != nil && (len(meta.NestedSources) > 0 || len(meta.SharedOuterSources) > 0) {
		return validation.FinalLayoutProvenanceNestedArchive
	}
	return base
}

type targetedSTATPoolGetter interface {
	GetPool() (pool.NntpClient, error)
}

// targetedSTATConnectionAdmission is an additive production capability.  It
// deliberately stays out of pool.Manager so existing v4-facing manager test
// doubles remain source compatible while the concrete manager can place
// import validation STAT work on the same playback-aware connection budget as
// import BODY work.
type targetedSTATConnectionAdmission interface {
	AcquireImportConnections(context.Context, int) (func(), int, error)
}

type targetedSTATProviderRegistry interface {
	ListProviders(context.Context, bool) ([]database.HealthProvider, error)
	ListProviderGenerations(context.Context, string) ([]database.HealthProviderGeneration, error)
}

type nntppoolTargetedSTATTransport struct {
	poolGetter   targetedSTATPoolGetter
	registry     targetedSTATProviderRegistry
	configGetter config.ConfigGetter
}

func newNntppoolTargetedSTATTransport(
	poolGetter targetedSTATPoolGetter,
	registry targetedSTATProviderRegistry,
	configGetter config.ConfigGetter,
) *nntppoolTargetedSTATTransport {
	return &nntppoolTargetedSTATTransport{
		poolGetter: poolGetter, registry: registry, configGetter: configGetter,
	}
}

func (t *nntppoolTargetedSTATTransport) TargetedSTAT(
	ctx context.Context,
	provider validation.TargetedSTATProvider,
	requests []validation.TargetedSTATRequest,
) ([]validation.TargetedSTATObservation, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if provider.ID == "" || provider.Generation <= 0 || provider.ActivationEpoch <= 0 ||
		len(requests) == 0 {
		return nil, errDurableImportSTATIncomplete
	}

	providerConfig, err := t.resolveCurrentProvider(ctx, provider)
	if err != nil {
		return nil, errDurableImportSTATIncomplete
	}
	client, err := t.poolGetter.GetPool()
	if err != nil || client == nil {
		return nil, errDurableImportSTATIncomplete
	}

	messageIDs := make([]string, 0, len(requests))
	positionsByID := make(map[string][]int, len(requests))
	seenPositions := make(map[int]struct{}, len(requests))
	for index, request := range requests {
		if request.MessageID == "" {
			return nil, errDurableImportSTATIncomplete
		}
		if _, duplicate := seenPositions[request.Position]; duplicate {
			return nil, errDurableImportSTATIncomplete
		}
		seenPositions[request.Position] = struct{}{}
		if _, seen := positionsByID[request.MessageID]; !seen {
			// StatMany correlates only by MessageID and may complete out of
			// order. A canonical layout may intentionally reuse one article,
			// so issue exactly one wire check and fan that typed observation to
			// every owning position instead of relying on result order.
			messageIDs = append(messageIDs, request.MessageID)
		}
		positionsByID[request.MessageID] = append(positionsByID[request.MessageID], index)
	}

	translated := make(map[int]validation.TargetedSTATResult, len(requests))
	statConcurrency := len(messageIDs)
	if cfg := t.configGetter(); cfg != nil {
		statConcurrency = min(statConcurrency, cfg.GetMaxConnectionsForHealthChecks())
	}
	if statConcurrency <= 0 {
		return nil, errDurableImportSTATIncomplete
	}
	if admission, ok := t.poolGetter.(targetedSTATConnectionAdmission); ok {
		release, granted, acquireErr := admission.AcquireImportConnections(ctx, statConcurrency)
		if acquireErr != nil {
			if release != nil {
				release()
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			return nil, errDurableImportSTATIncomplete
		}
		if release == nil || granted <= 0 || granted > statConcurrency {
			if release != nil {
				release()
			}
			return nil, errDurableImportSTATIncomplete
		}
		defer release()
		statConcurrency = granted
	}
	seenResultIDs := make(map[string]struct{}, len(messageIDs))
	for result := range client.StatMany(ctx, messageIDs, nntppool.StatManyOptions{
		Concurrency: statConcurrency,
		Provider:    providerConfig.NNTPPoolName(),
	}) {
		owners := positionsByID[result.MessageID]
		if len(owners) == 0 {
			return nil, errDurableImportSTATIncomplete
		}
		if _, duplicate := seenResultIDs[result.MessageID]; duplicate {
			return nil, errDurableImportSTATIncomplete
		}
		seenResultIDs[result.MessageID] = struct{}{}
		translatedResult := translateTargetedSTATResult(result)
		if !targetedSTATResultMatchesProvider(result, provider.ID) {
			translatedResult = incompleteTargetedSTAT(validation.TargetedSTATCauseUnknown)
		}
		for _, owner := range owners {
			if _, duplicate := translated[owner]; duplicate {
				return nil, errDurableImportSTATIncomplete
			}
			translated[owner] = translatedResult
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(seenResultIDs) != len(messageIDs) {
		// One omitted wire result makes every owning canonical position in
		// this batch incomplete. The validator will retry the batch without
		// committing partial provider evidence.
		return nil, errDurableImportSTATIncomplete
	}

	observations := make([]validation.TargetedSTATObservation, 0, len(translated))
	for index, request := range requests {
		result, ok := translated[index]
		if !ok {
			continue
		}
		observations = append(observations, validation.TargetedSTATObservation{
			Position: request.Position,
			Result:   result,
		})
	}
	return observations, nil
}

func targetedSTATResultMatchesProvider(result nntppool.StatManyResult, providerID string) bool {
	if providerID == "" {
		return false
	}
	if result.Err == nil && result.Result != nil {
		return result.Result.ProviderID == providerID &&
			targetedSTATAttemptsMatchProvider(result.Result.Attempts, providerID)
	}
	var transportError *nntppool.TransportError
	if errors.As(result.Err, &transportError) {
		return transportError.ProviderID == providerID &&
			targetedSTATAttemptsMatchProvider(transportError.Attempts, providerID)
	}
	// Untyped/sentinel errors carry no serving-provider identity. Even though
	// the call was targeted, they cannot become provider-specific evidence.
	return false
}

func targetedSTATAttemptsMatchProvider(attempts []nntppool.AttemptEvidence, providerID string) bool {
	for _, attempt := range attempts {
		if attempt.ProviderID != providerID ||
			(attempt.Operation != nntppool.OperationStat && attempt.Operation != nntppool.OperationUnknown) {
			return false
		}
	}
	return true
}

func (t *nntppoolTargetedSTATTransport) resolveCurrentProvider(
	ctx context.Context,
	target validation.TargetedSTATProvider,
) (*config.ProviderConfig, error) {
	if t == nil || t.poolGetter == nil || t.registry == nil || t.configGetter == nil {
		return nil, errDurableImportSTATIncomplete
	}
	cfg := t.configGetter()
	if cfg == nil {
		return nil, errDurableImportSTATIncomplete
	}
	var matched *config.ProviderConfig
	for index := range cfg.Providers {
		candidate := &cfg.Providers[index]
		if candidate.Enabled == nil || !*candidate.Enabled ||
			durableProviderStableID(candidate) != target.ID {
			continue
		}
		if matched != nil {
			return nil, errDurableImportSTATIncomplete
		}
		matched = candidate
	}
	if matched == nil {
		return nil, errDurableImportSTATIncomplete
	}

	providers, err := t.registry.ListProviders(ctx, true)
	if err != nil {
		return nil, errDurableImportSTATIncomplete
	}
	registryMatch := false
	for _, current := range providers {
		if current.ID != target.ID {
			continue
		}
		registryMatch = current.Active &&
			current.CurrentGeneration == target.Generation &&
			current.ActivationEpoch == target.ActivationEpoch
		break
	}
	if !registryMatch {
		return nil, errDurableImportSTATIncomplete
	}

	generations, err := t.registry.ListProviderGenerations(ctx, target.ID)
	if err != nil {
		return nil, errDurableImportSTATIncomplete
	}
	generationMatch := false
	for _, generation := range generations {
		if generation.Generation != target.Generation {
			continue
		}
		generationMatch = normalizedProviderEndpoint(generation.Endpoint) == normalizedProviderEndpoint(matched.Host) &&
			generation.Port == matched.Port && generation.Account == strings.TrimSpace(matched.Username)
		break
	}
	if !generationMatch {
		return nil, errDurableImportSTATIncomplete
	}
	return matched, nil
}

func normalizedProviderEndpoint(value string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(value)), ".")
}

func translateTargetedSTATResult(result nntppool.StatManyResult) validation.TargetedSTATResult {
	if result.Err == nil && result.Result != nil {
		translated := validation.TargetedSTATResult{
			Outcome:               nntppool.OutcomeSuccess,
			ResponseCode:          223,
			CompletionDisposition: validation.ImportCheckDispositionAttempted,
		}
		if attempt, ok := finalSTATAttempt(result.Result.Attempts); ok {
			applySTATAttemptEvidence(&translated, attempt)
			translated.Outcome = nntppool.OutcomeSuccess
			translated.CompletionDisposition = validation.ImportCheckDispositionAttempted
		}
		return translated
	}
	if result.Err == nil || result.Result != nil {
		return incompleteTargetedSTAT(validation.TargetedSTATCauseUnknown)
	}

	var transportError *nntppool.TransportError
	if errors.As(result.Err, &transportError) {
		translated := validation.TargetedSTATResult{
			Outcome:      transportError.Kind,
			ResponseCode: transportError.ResponseCode,
			CauseClass:   targetedSTATCauseClass(transportError.Cause),
		}
		if attempt, ok := finalSTATAttempt(transportError.Attempts); ok {
			applySTATAttemptEvidence(&translated, attempt)
		}
		if errors.Is(transportError.Cause, nntppool.ErrCircuitBreakerOpen) ||
			translated.CauseClass == validation.TargetedSTATCauseBreakerOpen ||
			transportError.Kind == nntppool.OutcomeCancellation ||
			transportError.Kind == nntppool.OutcomeCorruptBody {
			translated.CompletionDisposition = validation.ImportCheckDispositionIncomplete
			return translated
		}
		if transportError.Kind == nntppool.OutcomeProviderUnavailable {
			translated.CompletionDisposition = validation.ImportCheckDispositionExplicitUnavailable
			return normalizedTargetedSTATClassification(translated)
		}
		switch transportError.Kind {
		case nntppool.OutcomeHardArticleAbsence, nntppool.OutcomeTemporaryFailure,
			nntppool.OutcomeTransportFailure, nntppool.OutcomeInconclusive:
			translated.CompletionDisposition = validation.ImportCheckDispositionAttempted
			return normalizedTargetedSTATClassification(translated)
		default:
			translated.CompletionDisposition = validation.ImportCheckDispositionIncomplete
			return translated
		}
	}

	if errors.Is(result.Err, context.Canceled) || errors.Is(result.Err, context.DeadlineExceeded) ||
		errors.Is(result.Err, nntppool.ErrCircuitBreakerOpen) {
		return incompleteTargetedSTAT(targetedSTATCauseClass(result.Err))
	}
	if errors.Is(result.Err, nntppool.ErrArticleNotFound) {
		return validation.TargetedSTATResult{
			Outcome: nntppool.OutcomeHardArticleAbsence, ResponseCode: 430,
			CompletionDisposition: validation.ImportCheckDispositionAttempted,
		}
	}
	if explicitProviderUnavailable(result.Err) {
		return validation.TargetedSTATResult{
			Outcome:               nntppool.OutcomeProviderUnavailable,
			CompletionDisposition: validation.ImportCheckDispositionExplicitUnavailable,
			CauseClass:            targetedSTATCauseClass(result.Err),
		}
	}
	var protocolError *nntppool.Error
	if errors.As(result.Err, &protocolError) {
		switch protocolError.Code {
		case 423, 430:
			return validation.TargetedSTATResult{
				Outcome: nntppool.OutcomeHardArticleAbsence, ResponseCode: protocolError.Code,
				CompletionDisposition: validation.ImportCheckDispositionAttempted,
			}
		case 451:
			return validation.TargetedSTATResult{
				Outcome: nntppool.OutcomeTemporaryFailure, ResponseCode: 451,
				CompletionDisposition: validation.ImportCheckDispositionAttempted,
				CauseClass:            validation.TargetedSTATCauseUnknown,
			}
		}
	}
	return incompleteTargetedSTAT(validation.TargetedSTATCauseUnknown)
}

func finalSTATAttempt(attempts []nntppool.AttemptEvidence) (nntppool.AttemptEvidence, bool) {
	for index := len(attempts) - 1; index >= 0; index-- {
		if attempts[index].Operation == nntppool.OperationStat ||
			attempts[index].Operation == nntppool.OperationUnknown {
			return attempts[index], true
		}
	}
	return nntppool.AttemptEvidence{}, false
}

func applySTATAttemptEvidence(
	result *validation.TargetedSTATResult,
	attempt nntppool.AttemptEvidence,
) {
	result.Outcome = attempt.Outcome
	result.ResponseCode = attempt.ResponseCode
	result.CauseClass = targetedSTATCauseClass(attempt.Cause)
	result.PoolQueue = attempt.PoolQueueDuration
	result.PipelineWait = attempt.PipelineHeadWaitDuration
	result.ResponseService = attempt.ResponseServiceDuration
}

func normalizedTargetedSTATClassification(
	result validation.TargetedSTATResult,
) validation.TargetedSTATResult {
	switch result.ResponseCode {
	case 423, 430:
		result.Outcome = nntppool.OutcomeHardArticleAbsence
	case 451:
		result.Outcome = nntppool.OutcomeTemporaryFailure
	default:
		if result.Outcome == nntppool.OutcomeHardArticleAbsence {
			result.ResponseCode = 430
		}
	}
	return result
}

func incompleteTargetedSTAT(cause validation.TargetedSTATCauseClass) validation.TargetedSTATResult {
	return validation.TargetedSTATResult{
		Outcome:               nntppool.OutcomeInconclusive,
		CompletionDisposition: validation.ImportCheckDispositionIncomplete,
		CauseClass:            cause,
	}
}

func explicitProviderUnavailable(err error) bool {
	return errors.Is(err, nntppool.ErrAuthRequired) ||
		errors.Is(err, nntppool.ErrAuthRejected) ||
		errors.Is(err, nntppool.ErrQuotaExceeded) ||
		errors.Is(err, nntppool.ErrServiceUnavailable) ||
		errors.Is(err, nntppool.ErrInvalidProviderConfiguration) ||
		errors.Is(err, nntppool.ErrMaxConnections)
}

func targetedSTATCauseClass(err error) validation.TargetedSTATCauseClass {
	switch {
	case err == nil:
		return validation.TargetedSTATCauseNone
	case errors.Is(err, nntppool.ErrCircuitBreakerOpen):
		return validation.TargetedSTATCauseBreakerOpen
	case errors.Is(err, nntppool.ErrAuthRequired), errors.Is(err, nntppool.ErrAuthRejected):
		return validation.TargetedSTATCauseAuthentication
	case errors.Is(err, nntppool.ErrQuotaExceeded):
		return validation.TargetedSTATCauseQuota
	case errors.Is(err, nntppool.ErrServiceUnavailable):
		return validation.TargetedSTATCauseServiceUnavailable
	case errors.Is(err, context.DeadlineExceeded):
		return validation.TargetedSTATCauseTimeout
	case errors.Is(err, nntppool.ErrConnectionDied), errors.Is(err, nntppool.ErrProtocolDesync):
		return validation.TargetedSTATCauseTransport
	default:
		return validation.TargetedSTATCauseUnknown
	}
}

func durableProviderStableID(provider *config.ProviderConfig) string {
	if provider == nil {
		return ""
	}
	return strings.TrimSpace(provider.ID)
}

func durableImportProviderSpecs(cfg *config.Config) []database.ProviderSpec {
	if cfg == nil {
		return nil
	}
	specs := make([]database.ProviderSpec, 0, len(cfg.Providers))
	for index := range cfg.Providers {
		provider := &cfg.Providers[index]
		if provider.Enabled == nil || !*provider.Enabled {
			continue
		}
		role := database.ProviderRolePrimary
		if provider.IsBackupProvider != nil && *provider.IsBackupProvider {
			role = database.ProviderRoleBackup
		}
		displayName := strings.TrimSpace(provider.Name)
		if displayName == "" {
			displayName = strings.TrimSpace(provider.Host)
		}
		specs = append(specs, database.ProviderSpec{
			StableID: durableProviderStableID(provider), DisplayName: displayName,
			Endpoint: provider.Host, Port: provider.Port, Account: provider.Username,
			Role: role, Order: len(specs),
		})
	}
	return specs
}

type dueImportValidationRepository interface {
	ListDueImportValidations(context.Context, time.Time, int) ([]database.ImportValidation, error)
}

func (s *Service) cleanupTerminalImportArtifacts(ctx context.Context, queueItemID int64) error {
	if s == nil || queueItemID <= 0 {
		return nil
	}
	var writtenPaths []string
	if paths, ok := s.writtenPathsCache.LoadAndDelete(queueItemID); ok {
		writtenPaths, _ = paths.([]string)
	}
	s.mu.RLock()
	enabled := s.durableImportAdmissionEnabled
	repository := s.durableImportStateRepository
	journal := s.durableImportRollbackJournal
	s.mu.RUnlock()
	if !enabled {
		s.cleanupWrittenPaths(ctx, queueItemID, writtenPaths)
		return nil
	}
	if repository == nil || journal == nil {
		return errDurableImportActivationRollback
	}

	// The filesystem journal is the only authority that may restore raw
	// metadata. Never fall back to deleting a path in durable mode: that could
	// erase a healthy pre-queue revision after a failed replacement.
	if _, err := os.Stat(journal.queueDirectory(queueItemID)); os.IsNotExist(err) {
		stateCtx, cancel := durableImportStateContext(ctx)
		defer cancel()
		records, beginErr := repository.BeginImportQueueActivationRollback(
			stateCtx, queueItemID, time.Now().UTC(),
		)
		if beginErr != nil || len(records) != 0 {
			return errDurableImportActivationRollback
		}
		return nil
	} else if err != nil || journal.ValidateQueue(queueItemID) != nil {
		return errDurableImportActivationRollback
	}

	stateCtx, cancel := durableImportStateContext(ctx)
	defer cancel()
	records, err := repository.BeginImportQueueActivationRollback(
		stateCtx, queueItemID, time.Now().UTC(),
	)
	if err != nil {
		return errDurableImportActivationRollback
	}
	s.broadcastDurableHealthChanged()
	if len(records) == 0 {
		return s.cleanupUnactivatedImportCandidates(
			stateCtx, repository, journal, queueItemID,
		)
	}

	candidates := make(map[string]durableImportRollbackSnapshot, len(records))
	priors := make(map[string]durableImportRollbackSnapshot, len(records))
	for _, record := range records {
		candidate, loadErr := journal.LoadCandidate(queueItemID, record.FilePath)
		if loadErr != nil {
			visible, inspectErr := journal.inspect(record.FilePath)
			if inspectErr != nil || !visible.Exists ||
				visible.LayoutFingerprint != record.CandidateLayoutFingerprint {
				return errDurableImportActivationRollback
			}
			candidate, loadErr = journal.RecordCandidate(queueItemID, record.FilePath)
		}
		if loadErr != nil || !candidate.priorExists ||
			candidate.priorFingerprint != record.CandidateLayoutFingerprint {
			return errDurableImportActivationRollback
		}
		candidates[record.CandidateRevisionID] = candidate
	}
	for _, record := range records {
		prior, loadErr := journal.Load(queueItemID, record.FilePath)
		priorExists := record.PriorRevisionID != ""
		if loadErr != nil || prior.priorExists != priorExists ||
			(priorExists && prior.priorFingerprint != record.PriorLayoutFingerprint) ||
			(!priorExists && (record.PriorLayoutFingerprint != "" || len(prior.priorBytes) != 0)) {
			return s.compensateDurableImportRollback(
				stateCtx, repository, journal, queueItemID, records, candidates,
			)
		}
		priors[record.CandidateRevisionID] = prior
	}

	var restored, failed []database.ImportActivationRollback
	for _, record := range records {
		prior := priors[record.CandidateRevisionID]
		_ = journal.restore(record.FilePath, prior.priorBytes, prior.priorExists)
		visible, inspectErr := journal.inspect(record.FilePath)
		expected := inspectErr == nil && visible.Exists == prior.priorExists
		if expected && prior.priorExists {
			expected = visible.LayoutFingerprint == prior.priorFingerprint
		}
		if expected {
			restored = append(restored, record)
			continue
		}
		failed = append(failed, record)
	}

	for _, record := range restored {
		intent, loadErr := journal.LoadCandidateIntent(queueItemID, record.FilePath)
		if loadErr != nil || string(intent.priorBytes) != record.CandidateRevisionID ||
			intent.priorFingerprint != record.CandidateLayoutFingerprint {
			return errDurableImportActivationRollback
		}
		if err := journal.ReleaseCandidateStoreRef(
			stateCtx, queueItemID, record.CandidateRevisionID, intent.priorStoreRef,
		); err != nil {
			return errDurableImportActivationRollback
		}
	}
	if len(restored) > 0 {
		if err := repository.CompleteImportQueueActivationRollback(
			stateCtx, queueItemID, durableImportCandidateIDs(restored), time.Now().UTC(),
		); err != nil {
			return errDurableImportActivationRollback
		}
		s.broadcastDurableHealthChanged()
	}
	if len(failed) > 0 {
		if err := s.compensateDurableImportRollback(
			stateCtx, repository, journal, queueItemID, failed, candidates,
		); err != nil {
			return err
		}
		return errDurableImportActivationRollback
	}
	if err := journal.DiscardQueue(queueItemID); err != nil {
		return errDurableImportActivationRollback
	}
	return nil
}

func (s *Service) cleanupUnactivatedImportCandidates(
	ctx context.Context,
	repository *database.HealthStateRepository,
	journal *durableImportRollbackJournal,
	queueItemID int64,
) error {
	candidates, err := repository.ListInactiveImportQueueCandidates(ctx, queueItemID)
	if err != nil {
		return errDurableImportActivationRollback
	}
	claimedCandidateIDs := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		hasIntent, err := journal.CandidateIntentExists(queueItemID, candidate.FilePath)
		if err != nil {
			return errDurableImportActivationRollback
		}
		if !hasIntent {
			// Admission may have committed immediately before a crash that
			// preceded the first metadata snapshot/write. There is no visible
			// candidate or ref ownership to undo in that state.
			continue
		}
		intent, err := journal.LoadCandidateIntent(queueItemID, candidate.FilePath)
		if err != nil || string(intent.priorBytes) != candidate.CandidateRevisionID ||
			intent.priorFingerprint != candidate.LayoutFingerprint {
			return errDurableImportActivationRollback
		}
		prior, err := journal.Load(queueItemID, candidate.FilePath)
		if err != nil {
			return errDurableImportActivationRollback
		}
		visible, err := journal.inspect(candidate.FilePath)
		if err != nil {
			return errDurableImportActivationRollback
		}
		candidateVisible := visible.Exists &&
			visible.LayoutFingerprint == candidate.LayoutFingerprint
		priorVisible := visible.Exists == prior.priorExists
		if priorVisible && prior.priorExists {
			priorVisible = visible.LayoutFingerprint == prior.priorFingerprint
		}
		if candidateVisible {
			// Persist exact candidate bytes before the SQL cleanup claim. If the
			// process stops after claiming, the ordinary cleanup_pending recovery
			// path can either finish the restore or compensate safely.
			candidateSnapshot, snapshotErr := journal.RecordCandidate(
				queueItemID, candidate.FilePath,
			)
			if snapshotErr != nil || !candidateSnapshot.priorExists ||
				candidateSnapshot.priorFingerprint != candidate.LayoutFingerprint {
				return errDurableImportActivationRollback
			}
			claimed, claimErr := repository.ClaimInactiveImportCandidateCleanup(
				ctx, queueItemID, candidate.CandidateRevisionID,
				prior.priorFingerprint, prior.priorExists, time.Now().UTC(),
			)
			if claimErr != nil {
				return errDurableImportActivationRollback
			}
			if claimed {
				if err := journal.restore(
					candidate.FilePath, prior.priorBytes, prior.priorExists,
				); err != nil {
					return errDurableImportActivationRollback
				}
				visible, err = journal.inspect(candidate.FilePath)
				if err != nil || visible.Exists != prior.priorExists ||
					(prior.priorExists && visible.LayoutFingerprint != prior.priorFingerprint) {
					return errDurableImportActivationRollback
				}
				claimedCandidateIDs = append(
					claimedCandidateIDs, candidate.CandidateRevisionID,
				)
			}
		} else if !priorVisible {
			return errDurableImportActivationRollback
		}
		if err := journal.ReleaseCandidateStoreRef(
			ctx, queueItemID, candidate.CandidateRevisionID, intent.priorStoreRef,
		); err != nil {
			return errDurableImportActivationRollback
		}
	}
	if len(claimedCandidateIDs) > 0 {
		if err := repository.CompleteImportQueueActivationRollback(
			ctx, queueItemID, claimedCandidateIDs, time.Now().UTC(),
		); err != nil {
			return errDurableImportActivationRollback
		}
		s.broadcastDurableHealthChanged()
	}
	if err := journal.CleanupUnreferencedStores(ctx, queueItemID); err != nil {
		return errDurableImportActivationRollback
	}
	if err := journal.DiscardQueue(queueItemID); err != nil {
		return errDurableImportActivationRollback
	}
	return nil
}

func (s *Service) commitDurableImportArtifacts(ctx context.Context, queueItemID int64) error {
	if s == nil || queueItemID <= 0 {
		return nil
	}
	s.mu.RLock()
	enabled := s.durableImportAdmissionEnabled
	repository := s.durableImportStateRepository
	journal := s.durableImportRollbackJournal
	s.mu.RUnlock()
	if !enabled {
		return nil
	}
	if repository == nil || journal == nil {
		return errDurableImportActivationRollback
	}
	stateCtx, cancel := durableImportStateContext(ctx)
	defer cancel()
	if err := repository.CommitImportQueueActivations(
		stateCtx, queueItemID, time.Now().UTC(),
	); err != nil {
		return errDurableImportActivationRollback
	}
	s.broadcastDurableHealthChanged()
	if err := journal.CommitQueue(stateCtx, queueItemID); err != nil {
		return errDurableImportActivationRollback
	}
	return nil
}

// recoverDurableImportRollbackJournals closes only journals whose queue state
// proves the intended terminal direction. Active/pending imports retain their
// first visibility snapshot for ordinary resume; corrupt/ambiguous state stops
// startup before a worker can expose a mismatched DB/filesystem revision.
func (s *Service) recoverDurableImportRollbackJournals(ctx context.Context) error {
	if s == nil || !s.durableImportAdmissionEnabled || s.durableImportRollbackJournal == nil ||
		s.durableImportStateRepository == nil || s.database == nil || s.database.Repository == nil {
		return nil
	}
	queueIDs, err := s.durableImportRollbackJournal.PendingQueueIDs()
	if err != nil {
		return errDurableImportActivationRollback
	}
	for _, queueItemID := range queueIDs {
		item, err := s.database.Repository.GetQueueItem(ctx, queueItemID)
		if err != nil || item == nil {
			return errDurableImportActivationRollback
		}
		switch item.Status {
		case database.QueueStatusCompleted:
			if err := s.commitDurableImportArtifacts(ctx, queueItemID); err != nil {
				return err
			}
		case database.QueueStatusFailed:
			if err := s.cleanupTerminalImportArtifacts(ctx, queueItemID); err != nil {
				return err
			}
		case database.QueueStatusPaused, database.QueueStatusPending:
			if item.ErrorMessage == nil ||
				(*item.ErrorMessage != durableTerminalRollbackWaitingMarker &&
					*item.ErrorMessage != durableTerminalRollbackDueMarker) {
				continue
			}
			if err := s.cleanupTerminalImportArtifacts(ctx, queueItemID); err != nil {
				return err
			}
			message := (&durableImportRejectedError{}).Error()
			if err := s.database.Repository.UpdateQueueItemStatus(
				ctx, queueItemID, database.QueueStatusFailed, &message,
			); err != nil {
				return errDurableImportActivationRollback
			}
		}
	}
	return nil
}

func (s *Service) compensateDurableImportRollback(
	ctx context.Context,
	repository *database.HealthStateRepository,
	journal *durableImportRollbackJournal,
	queueItemID int64,
	records []database.ImportActivationRollback,
	candidates map[string]durableImportRollbackSnapshot,
) error {
	if len(records) == 0 {
		return errDurableImportActivationRollback
	}
	for _, record := range records {
		if _, ok := candidates[record.CandidateRevisionID]; !ok {
			return errDurableImportActivationRollback
		}
	}
	if err := repository.CompensateImportQueueActivationRollback(
		ctx, queueItemID, durableImportCandidateIDs(records), time.Now().UTC(),
	); err != nil {
		return errDurableImportActivationRollback
	}
	s.broadcastDurableHealthChanged()
	for _, record := range records {
		candidate := candidates[record.CandidateRevisionID]
		_ = journal.restore(record.FilePath, candidate.priorBytes, true)
		visible, err := journal.inspect(record.FilePath)
		if err != nil || !visible.Exists ||
			visible.LayoutFingerprint != candidate.priorFingerprint {
			return errDurableImportActivationRollback
		}
	}
	return nil
}

func durableImportCandidateIDs(records []database.ImportActivationRollback) []string {
	ids := make([]string, 0, len(records))
	for _, record := range records {
		if record.CandidateRevisionID != "" {
			ids = append(ids, record.CandidateRevisionID)
		}
	}
	return ids
}

func (s *Service) broadcastDurableHealthChanged() {
	if s != nil && s.broadcaster != nil {
		s.broadcaster.BroadcastHealthChanged()
	}
}

func (s *Service) durableImportReusableLayouts(
	ctx context.Context,
	queueItemID int64,
) (map[string]admissionctx.ReusableLayoutBinding, error) {
	if s == nil || s.database == nil || queueItemID <= 0 {
		return nil, nil
	}
	query := durableImportReusableLayoutsQuery(s.database.Dialect())
	rows, err := s.database.Connection().QueryContext(ctx, query, queueItemID)
	if err != nil {
		return nil, errDurableImportSTATIncomplete
	}
	defer rows.Close()
	layouts := make(map[string]admissionctx.ReusableLayoutBinding)
	for rows.Next() {
		var path, fingerprint string
		var active bool
		if err := rows.Scan(&path, &fingerprint, &active); err != nil {
			return nil, errDurableImportSTATIncomplete
		}
		layouts[path] = admissionctx.ReusableLayoutBinding{
			Fingerprint: fingerprint, ActivationPending: !active,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, errDurableImportSTATIncomplete
	}
	return layouts, nil
}

func durableImportReusableLayoutsQuery(dialect database.Dialect) string {
	query := `
		SELECT f.file_path, r.layout_fingerprint,
		       r.active AND NOT EXISTS (
			   SELECT 1 FROM health_import_activation_journal owner
			   WHERE owner.file_health_id = r.file_health_id
			     AND owner.queue_item_id <> v.queue_item_id
			     AND owner.state IN ('active', 'cleanup_pending', 'compensated')
		       ) AS stable_active
		FROM health_import_validations v
		JOIN health_file_revisions r ON r.id = v.file_revision_id
		JOIN file_health f ON f.id = r.file_health_id
		WHERE v.queue_item_id = ? AND v.phase IN ('accepted', 'health_pending')
	`
	if dialect == database.DialectPostgres {
		query = strings.Replace(query, "?", "$1", 1)
	}
	return query
}

func (s *Service) configureDurableImportAdmission(cfg *config.Config) error {
	if s == nil || s.database == nil || s.metadataService == nil ||
		s.poolManager == nil || cfg == nil {
		return nil
	}

	s.mu.RLock()
	repository := s.durableImportStateRepository
	alreadyEnabled := s.durableImportAdmissionEnabled
	rollbackJournal := s.durableImportRollbackJournal
	s.mu.RUnlock()
	if repository == nil {
		repository = database.NewHealthStateRepository(
			s.database.Connection(), s.database.Dialect(),
		)
		s.mu.Lock()
		s.durableImportStateRepository = repository
		s.durableImportRepository = repository
		s.mu.Unlock()
	}
	if rollbackJournal == nil {
		var storeRefs durableStoreRefOperationLedger
		if s.database.StoreRefRepo != nil {
			storeRefs, _ = any(s.database.StoreRefRepo).(durableStoreRefOperationLedger)
		}
		rollbackJournal = newDurableImportRollbackJournal(s.metadataService, storeRefs)
		s.mu.Lock()
		s.durableImportRollbackJournal = rollbackJournal
		s.mu.Unlock()
	}

	providerSpecs := durableImportProviderSpecs(cfg)
	if len(providerSpecs) == 0 {
		// Never replace an active gate with an unvalidated write path merely
		// because all providers were disabled during a configuration change.
		if alreadyEnabled {
			return nil
		}
		return nil
	}
	transport := newNntppoolTargetedSTATTransport(
		s.poolManager, repository, s.configGetter,
	)
	validator, err := validation.NewDurableFinalLayoutValidator(
		repository,
		transport,
		validation.DurableFinalLayoutValidatorOptions{
			ProviderSpecs: providerSpecs,
			DamagePolicy:  config.ImportDamagePolicy(cfg.Import.DamagePolicy),
			OnHealthChanged: func() {
				if s.broadcaster != nil {
					s.broadcaster.BroadcastHealthChanged()
				}
			},
		},
	)
	if err != nil {
		return fmt.Errorf("configure durable import admission: %w", err)
	}

	writeValidator := newDurableImportWriteValidator(validator)
	writeValidator.journal = rollbackJournal
	writeValidator.health = s.broadcaster
	s.metadataService.SetWriteValidator(writeValidator)
	s.processor.SetDurableRollbackJournal(rollbackJournal)
	s.processor.SetDurableAdmissionEnabled(true)
	s.postProcessor.SetReuseDurableImportCoverage(true)
	s.mu.Lock()
	s.durableImportAdmissionEnabled = true
	s.mu.Unlock()
	return nil
}

func (s *Service) handleDurableImportHold(
	ctx context.Context,
	item *database.ImportQueueItem,
	hold *durableImportHoldError,
) {
	if item == nil || hold == nil || s.database == nil || s.database.Repository == nil {
		return
	}
	// Drop only transient bookkeeping. Previously accepted metadata stays
	// visible and the next attempt reconstructs its own written-path list.
	s.writtenPathsCache.LoadAndDelete(item.ID)

	status := database.QueueStatusPending
	var marker *string
	stage := "Resuming final-layout validation"
	if hold.terminalRollback {
		status = database.QueueStatusPaused
		value := durableTerminalRollbackWaitingMarker
		marker = &value
		stage = "Waiting to resume terminal import rollback"
	} else if hold.successFinalization {
		status = database.QueueStatusPaused
		value := durableSuccessFinalizationWaitingMarker
		marker = &value
		stage = "Waiting to retry import success finalization"
	} else if hold.retryAt != nil {
		status = database.QueueStatusPaused
		if hold.resumeRequired {
			value := durableFinalLayoutIncompleteWaitingMarker
			marker = &value
			stage = "Waiting to retry incomplete final-layout validation"
		} else {
			stage = "Waiting for final-layout confirmation"
		}
	}
	stateCtx, cancel := durableImportStateContext(ctx)
	defer cancel()
	if err := s.database.Repository.UpdateQueueItemStatus(stateCtx, item.ID, status, marker); err != nil {
		if s.log != nil {
			s.log.ErrorContext(ctx, "Failed to park resumable import validation", "queue_id", item.ID)
		}
		return
	}
	if s.broadcaster != nil {
		s.broadcaster.UpdateProgressWithStage(int(item.ID), 95, stage)
		s.broadcaster.BroadcastQueueChanged()
	}
}

func (s *Service) markDurableTerminalRollback(
	ctx context.Context,
	item *database.ImportQueueItem,
) error {
	if s == nil || item == nil || s.database == nil || s.database.Repository == nil {
		return nil
	}
	s.mu.RLock()
	enabled := s.durableImportAdmissionEnabled
	s.mu.RUnlock()
	if !enabled {
		return nil
	}
	marker := durableTerminalRollbackWaitingMarker
	stateCtx, cancel := durableImportStateContext(ctx)
	defer cancel()
	if err := s.database.Repository.UpdateQueueItemStatus(
		stateCtx, item.ID, database.QueueStatusPaused, &marker,
	); err != nil {
		return errDurableImportActivationRollback
	}
	if s.broadcaster != nil {
		s.broadcaster.BroadcastQueueChanged()
	}
	return nil
}

// handleDurablePrelayoutFailure parks first-pass hard absence for exactly one
// delayed confirmation, and keeps incomplete work immediately resumable. It
// returns false only when a due confirmation again reached a conclusive failure,
// allowing the ordinary sanitized rejection path to finish the queue item.
func (s *Service) handleDurablePrelayoutFailure(
	ctx context.Context,
	item *database.ImportQueueItem,
	failure *durablePrelayoutValidationError,
) bool {
	if item == nil || failure == nil || s == nil || s.database == nil || s.database.Repository == nil {
		return false
	}
	if failure.confirmationRequired && item.ErrorMessage != nil && *item.ErrorMessage == durablePrelayoutDueMarker {
		return false
	}

	s.writtenPathsCache.LoadAndDelete(item.ID)
	status := database.QueueStatusPaused
	var marker *string
	stage := "Waiting to resume final-layout prerequisite check"
	if failure.confirmationRequired {
		value := durablePrelayoutWaitingMarker
		marker = &value
		stage = "Waiting for final-layout prerequisite confirmation"
	} else if failure.resumeRequired {
		value := durablePrelayoutInitialIncompleteWaitingMarker
		if item.ErrorMessage != nil && *item.ErrorMessage == durablePrelayoutDueMarker {
			// The intended confirmation did not complete. Preserve that phase
			// across the bounded retry so a later conclusive second failure can
			// reject, while this omitted/incomplete attempt never can.
			value = durablePrelayoutConfirmIncompleteWaitingMarker
		}
		marker = &value
	}

	stateCtx, cancel := durableImportStateContext(ctx)
	defer cancel()
	if err := s.database.Repository.UpdateQueueItemStatus(stateCtx, item.ID, status, marker); err != nil {
		if s.log != nil {
			s.log.ErrorContext(stateCtx, "Failed to park pre-layout import validation", "queue_id", item.ID)
		}
		return true
	}
	if s.broadcaster != nil {
		s.broadcaster.UpdateProgressWithStage(int(item.ID), 8, stage)
		s.broadcaster.BroadcastQueueChanged()
	}
	return true
}

func durableImportStateContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
}

func (s *Service) resumeDueImportValidations(ctx context.Context) error {
	if s == nil || s.database == nil || s.database.Repository == nil {
		return nil
	}
	now := time.Now().UTC()
	prelayoutChanged, err := s.resumeDuePrelayoutConfirmations(ctx, now)
	if err != nil {
		return err
	}
	s.mu.RLock()
	repository := s.durableImportRepository
	s.mu.RUnlock()
	if repository == nil {
		if prelayoutChanged && s.broadcaster != nil {
			s.broadcaster.BroadcastQueueChanged()
		}
		return nil
	}
	validations, err := repository.ListDueImportValidations(ctx, now, 128)
	if err != nil {
		return err
	}
	changed := prelayoutChanged
	for _, validationState := range validations {
		resumed, err := s.resumePausedFinalLayoutQueueItem(ctx, validationState.QueueItemID, now)
		if err != nil {
			return err
		}
		changed = changed || resumed
	}
	if changed && s.broadcaster != nil {
		s.broadcaster.BroadcastQueueChanged()
	}
	return nil
}

func (s *Service) resumeDuePrelayoutConfirmations(ctx context.Context, now time.Time) (bool, error) {
	cutoff := now.Add(-durablePrelayoutConfirmationDelay)
	transitions := [][2]string{
		{durablePrelayoutWaitingMarker, durablePrelayoutDueMarker},
		{durablePrelayoutInitialIncompleteWaitingMarker, durablePrelayoutInitialRetryDueMarker},
		{durablePrelayoutConfirmIncompleteWaitingMarker, durablePrelayoutDueMarker},
		{durableFinalLayoutIncompleteWaitingMarker, durableFinalLayoutIncompleteRetryDueMarker},
		{durableTerminalRollbackWaitingMarker, durableTerminalRollbackDueMarker},
		{durableSuccessFinalizationWaitingMarker, durableSuccessFinalizationDueMarker},
	}
	changed := false
	for _, transition := range transitions {
		resumed, err := s.resumeDuePrelayoutMarker(ctx, now, cutoff, transition[0], transition[1])
		if err != nil {
			return false, err
		}
		changed = changed || resumed
	}
	return changed, nil
}

func (s *Service) resumeDuePrelayoutMarker(
	ctx context.Context,
	now time.Time,
	cutoff time.Time,
	waitingMarker string,
	dueMarker string,
) (bool, error) {
	var (
		query string
		args  []any
	)
	if s.database.Dialect() == database.DialectPostgres {
		query = `
			UPDATE import_queue
			SET status = $1, error_message = $2, started_at = NULL, updated_at = $3
			WHERE status = $4 AND error_message = $5 AND updated_at <= $6
		`
	} else {
		query = `
			UPDATE import_queue
			SET status = ?, error_message = ?, started_at = NULL, updated_at = ?
			WHERE status = ? AND error_message = ?
			  AND datetime(updated_at) <= datetime(?)
		`
	}
	args = []any{
		database.QueueStatusPending, dueMarker, now,
		database.QueueStatusPaused, waitingMarker, cutoff,
	}
	result, err := s.database.Connection().ExecContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("resume due pre-layout work: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inspect due pre-layout work: %w", err)
	}
	return rows > 0, nil
}

func (s *Service) resumePausedFinalLayoutQueueItem(ctx context.Context, id int64, now time.Time) (bool, error) {
	query := `
		UPDATE import_queue
		SET status = ?, error_message = NULL, started_at = NULL, updated_at = ?
		WHERE id = ? AND status = ? AND error_message IS NULL
	`
	if s.database.Dialect() == database.DialectPostgres {
		query = `
			UPDATE import_queue
			SET status = $1, error_message = NULL, started_at = NULL, updated_at = $2
			WHERE id = $3 AND status = $4 AND error_message IS NULL
		`
	}
	result, err := s.database.Connection().ExecContext(
		ctx, query, database.QueueStatusPending, now, id, database.QueueStatusPaused,
	)
	if err != nil {
		return false, fmt.Errorf("resume paused final-layout queue item: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("inspect resumed final-layout queue item: %w", err)
	}
	return rows > 0, nil
}

func (s *Service) runDurableImportValidationResumer(ctx context.Context) {
	if err := s.resumeDueImportValidations(ctx); err != nil && s.log != nil && ctx.Err() == nil {
		s.log.WarnContext(ctx, "Failed to resume due import validation")
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.resumeDueImportValidations(ctx); err != nil && s.log != nil && ctx.Err() == nil {
				s.log.WarnContext(ctx, "Failed to resume due import validation")
			}
		}
	}
}
