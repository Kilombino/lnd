// Package migration38 marks a database that has run with splicing on.
package migration38

import (
	"github.com/lightningnetwork/lnd/kvdb"
)

// MarkSplicing changes nothing in the data. Its version number is the point:
// a channel can be spliced only once the database is at this version, and an
// older binary refuses a database at a version it doesn't know. Without the
// bump, an older binary would read a spliced channel, skip its splice records
// as unknown, watch the funding output the splice already spent, and miss a
// breach on the new one.
//
// It runs the first time lnd starts with splicing on; a node that never turns
// splicing on stays at the version before and can still go back to an older
// release.
func MarkSplicing(_ kvdb.RwTx) error {
	return nil
}
