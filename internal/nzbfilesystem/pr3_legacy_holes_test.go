package nzbfilesystem

import (
	"context"
	"testing"

	"github.com/javi11/altmount/internal/metadata"
	metapb "github.com/javi11/altmount/internal/metadata/proto"
	"github.com/javi11/altmount/internal/testsupport/fakepool"
	"github.com/stretchr/testify/require"
)

func TestPR3LegacyMetaHolesDoNotAuthorizePrePadding(t *testing.T) {
	mvf := newTestMVF(t, context.Background(), fakepool.New(), 2, 16, 1)
	mvf.name = "movie.mkv"
	mvf.meta.Encryption = metapb.Encryption_NONE
	mvf.meta.KnownHoles = metadata.KnownHolesToProto(nil)
	mvf.meta.KnownHoles = append(mvf.meta.KnownHoles, &metapb.HoleRun{StartSegment: 0, Count: 1})

	hooks := mvf.holeHooks()
	require.NotNil(t, hooks)
	if hooks.KnownHoles != nil && hooks.KnownHoles(0) {
		t.Fatal("legacy .meta hole authorized a fetch-free zero fill")
	}
}
