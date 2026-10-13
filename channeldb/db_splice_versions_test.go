package channeldb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOpenableDBVersions checks the versions a copying tool must accept:
// the latest, which carries the splicing mark, and the one before it.
func TestOpenableDBVersions(t *testing.T) {
	t.Parallel()

	versions := OpenableDBVersions()
	require.Equal(t, []uint32{splicingDBVersion - 1, splicingDBVersion},
		versions)
	require.Equal(t, LatestDBVersion(), versions[1])
}
