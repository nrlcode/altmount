// Package admissionctx carries importer-owned, in-memory final-layout intent
// across nested processor packages. It contains no article identities.
package admissionctx

import (
	"context"
	"strings"

	"github.com/javi11/altmount/internal/importer/validation"
)

// ReusableLayoutBinding identifies one final layout admitted by this queue on
// an earlier attempt. ActivationPending distinguishes a published revision
// from metadata that became visible before its candidate revision could be
// activated.
type ReusableLayoutBinding struct {
	Fingerprint       string
	ActivationPending bool
}

// Intent binds a metadata write to one queue item and structural provenance.
// ReusableLayouts contains only normalized virtual paths and structural
// fingerprints; it never carries article identities.
type Intent struct {
	QueueItemID     int64
	Provenance      validation.FinalLayoutProvenanceKind
	ReusableLayouts map[string]ReusableLayoutBinding
}

type contextKey struct{}

// WithIntent adds or updates queue/provenance while retaining restart layout
// bindings installed earlier by WithReusableLayouts.
func WithIntent(
	ctx context.Context,
	queueItemID int64,
	provenance validation.FinalLayoutProvenanceKind,
) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if queueItemID <= 0 {
		return ctx
	}
	prior, _ := FromContext(ctx)
	if prior.QueueItemID != queueItemID {
		prior.ReusableLayouts = nil
	}
	prior.QueueItemID = queueItemID
	prior.Provenance = provenance
	return context.WithValue(ctx, contextKey{}, prior)
}

// WithReusableLayouts installs the terminal layout bindings captured before a
// restarted queue attempt. The input is cloned so callers cannot race writes.
func WithReusableLayouts(
	ctx context.Context,
	queueItemID int64,
	layouts map[string]string,
) context.Context {
	bindings := make(map[string]ReusableLayoutBinding, len(layouts))
	for path, fingerprint := range layouts {
		bindings[path] = ReusableLayoutBinding{Fingerprint: fingerprint}
	}
	return WithReusableLayoutBindings(ctx, queueItemID, bindings)
}

// WithReusableLayoutBindings installs terminal layout bindings, including
// whether a prior atomic metadata write still needs post-write activation.
func WithReusableLayoutBindings(
	ctx context.Context,
	queueItemID int64,
	layouts map[string]ReusableLayoutBinding,
) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if queueItemID <= 0 {
		return ctx
	}
	cloned := make(map[string]ReusableLayoutBinding, len(layouts))
	for path, binding := range layouts {
		path = NormalizePath(path)
		binding.Fingerprint = strings.TrimSpace(binding.Fingerprint)
		if path != "" && binding.Fingerprint != "" {
			cloned[path] = binding
		}
	}
	prior, _ := FromContext(ctx)
	prior.QueueItemID = queueItemID
	prior.ReusableLayouts = cloned
	return context.WithValue(ctx, contextKey{}, prior)
}

// FromContext returns a defensive copy of the intent header. The layouts map
// is immutable after installation and may be read concurrently.
func FromContext(ctx context.Context) (Intent, bool) {
	if ctx == nil {
		return Intent{}, false
	}
	intent, ok := ctx.Value(contextKey{}).(Intent)
	return intent, ok && intent.QueueItemID > 0
}

// ReusableLayoutFingerprint returns a prior terminal fingerprint for path.
func ReusableLayoutFingerprint(ctx context.Context, path string) (string, bool) {
	binding, ok := ReusableLayout(ctx, path)
	return binding.Fingerprint, ok
}

// ReusableLayout returns the prior terminal binding for path.
func ReusableLayout(ctx context.Context, path string) (ReusableLayoutBinding, bool) {
	intent, ok := FromContext(ctx)
	if !ok || len(intent.ReusableLayouts) == 0 {
		return ReusableLayoutBinding{}, false
	}
	binding, ok := intent.ReusableLayouts[NormalizePath(path)]
	return binding, ok && binding.Fingerprint != ""
}

// NormalizePath mirrors the durable health path key without importing the
// database package's private helper.
func NormalizePath(path string) string {
	path = strings.ReplaceAll(path, `\`, "/")
	return strings.TrimLeft(path, "/")
}
