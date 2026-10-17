//go:build bridgerpc
// +build bridgerpc

package bridgerpc

import (
	"github.com/paulscode/lightning-fork-bridge/store"
)

// openJournal opens the swap journal, and says so in the log when opening it
// dropped a torn record from its end.
//
// That means the process died while writing a record. Nothing reported
// durable was lost, but an operator should know it happened, and only the
// first open after the crash can tell: the repair is written back, so every
// later open finds the journal whole.
func openJournal(path string) (*store.Journal, error) {
	j, err := store.Open(path)
	if err != nil {
		return nil, err
	}
	if j.Repaired {
		log.Warnf("Bridge swap journal %s ended in a torn record, "+
			"which was dropped (%d bytes): the node stopped while "+
			"writing it. No swap's recorded state was lost", path,
			j.RepairedBytes)
	}

	return j, nil
}
