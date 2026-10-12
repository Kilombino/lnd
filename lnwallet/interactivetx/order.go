package interactivetx

import (
	"bytes"

	"github.com/btcsuite/btcd/btcutil"
)

// Contributed returns the total value of the inputs each side added, as BOLT
// 2 counts it to decide who sends tx_signatures first: the shared input counts
// in full as the initiator's.
func Contributed(res *Result) (initiator, acceptor btcutil.Amount) {
	for _, in := range res.Inputs {
		if in.Shared || addedBy(in.SerialID) == Initiator {
			initiator += in.Value
		} else {
			acceptor += in.Value
		}
	}

	return initiator, acceptor
}

// SignsFirst reports whether this node sends tx_signatures first: the side
// that contributed less signs first, and on a tie the side with the lower
// node id.
//
// Note what this means for a splice the peer starts while this node adds
// nothing: this node contributed zero and always signs first, before it sees
// any of the peer's witnesses. Everything this node refuses must be decided
// before its commitment_signed.
func SignsFirst(res *Result, us Role, ourNodeID, theirNodeID []byte) bool {
	init, acc := Contributed(res)

	ours, theirs := init, acc
	if us == Acceptor {
		ours, theirs = acc, init
	}

	switch {
	case ours < theirs:
		return true
	case ours > theirs:
		return false
	default:
		return bytes.Compare(ourNodeID, theirNodeID) < 0
	}
}
