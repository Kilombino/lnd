//go:build bridgerpc
// +build bridgerpc

package bridgerpc

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/btcsuite/btclog/v2"
	"github.com/paulscode/lightning-fork-bridge/store"
	"github.com/paulscode/lightning-fork-bridge/swap"
	"github.com/stretchr/testify/require"
)

// A journal whose last record was torn by a crash is repaired at open, and the
// operator is told: only the first open after the crash can tell.
//
// Not parallel: it swaps the package's logger.
func TestATornJournalIsLogged(t *testing.T) {
	var out bytes.Buffer
	before := log
	UseLogger(btclog.NewSLogger(btclog.NewDefaultHandler(&out)))
	t.Cleanup(func() { UseLogger(before) })

	path := filepath.Join(t.TempDir(), "swaps.journal")
	j, err := openJournal(path)
	require.NoError(t, err)
	now := time.Now()
	require.NoError(t, j.Put(context.Background(), store.Record{
		Hash: [32]byte{1}, State: swap.Quoted, Invoice: "lnbc1payme",
		OutgoingCLTVLimit: 40, Created: now, Updated: now,
	}))
	require.NoError(t, j.Close())
	require.NotContains(t, out.String(), "torn")

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = f.WriteString(`{"hash":"01`)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	j, err = openJournal(path)
	require.NoError(t, err)
	require.NoError(t, j.Close())
	require.Contains(t, out.String(), "torn record")
	require.Equal(t, 1, strings.Count(out.String(), "torn record"))

	// Repaired on disk, so the next open finds it whole and says nothing.
	j, err = openJournal(path)
	require.NoError(t, err)
	require.NoError(t, j.Close())
	require.Equal(t, 1, strings.Count(out.String(), "torn record"))
}
