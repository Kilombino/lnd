//go:build bridgerpc
// +build bridgerpc

package bridgerpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/store"
	"github.com/paulscode/lightning-fork-bridge/swap"
)

var (
	// errNotStopped is returned when the operator asks to resolve a swap
	// the bridge has not stopped for them in paying.
	errNotStopped = errors.New("the swap is not stopped in paying")

	// errStillInFlight is returned when the paying node says the payment
	// of a stopped swap is still going.
	errStillInFlight = errors.New("the payment is still in flight")
)

// resolve decides a swap the runner stopped in paying, for the operator, by
// asking the paying node again what became of its payment.
//
// The runner stops there when the node has no record of a payment the journal
// says was started, because "never sent" and "sent and lost track of" call for
// opposite actions and only a person can rule out the second (a node database
// restored from a backup, payments deleted by hand). Asking for this is the
// operator saying they have. What the node says then decides it:
//
//   - No record, or failed: the swap fails, and the payer's held payment is
//     cancelled, which refunds them.
//   - Succeeded: the preimage is recorded and the swap is paid, which settles
//     the payer's held payment with it.
//   - In flight: refused. The payment may yet go either way, and the runner
//     would not have stopped on it.
//
// Only a swap the runner stopped is decided here. One it is still driving
// learns a failure or a success by itself, and recording one for it from here
// as well would have two writers on one record.
func (s *service) resolve(ctx context.Context, hash node.Hash) (store.Record,
	error) {

	s.resolveMu.Lock()
	defer s.resolveMu.Unlock()

	rec, err := s.journal.Get(ctx, hash)
	if err != nil {
		return store.Record{}, err
	}
	if rec.State != swap.Paying {
		return rec, fmt.Errorf("%w: it is %v", errNotStopped, rec.State)
	}

	s.stoppedMu.Lock()
	_, stopped := s.stopped[hash]
	s.stoppedMu.Unlock()
	if !stopped {
		return rec, fmt.Errorf("%w: the bridge is still driving it, "+
			"and stops a swap for the operator only when it must "+
			"not decide it alone", errNotStopped)
	}

	sd, _, err := s.route(ctx, rec.Invoice)
	if err != nil {
		return rec, fmt.Errorf("finding the direction that pays it: %w",
			err)
	}

	pmt, err := sd.out.LookupPayment(ctx, rec.Outgoing())
	if err != nil {
		return rec, fmt.Errorf("asking the paying node: %w", err)
	}

	var event swap.Event
	switch pmt.State {
	case node.PaymentInFlight:
		return rec, errStillInFlight

	case node.PaymentSucceeded:
		if pmt.Preimage == (node.Preimage{}) {
			return rec, fmt.Errorf("%w: the paying node reports the "+
				"payment succeeded with no preimage",
				ErrNodeContradiction)
		}

		// As the driver files it: the preimage is what claims the
		// payer's payment when the two legs share a hash.
		rec.OutgoingPreimage = pmt.Preimage
		if rec.Binding == swap.HashLinked {
			rec.Preimage = pmt.Preimage
		}
		event = swap.OutgoingSucceeded

	case node.PaymentFailed, node.PaymentNone:
		event = swap.OutgoingFailed

	default:
		return rec, fmt.Errorf("the paying node reports payment state "+
			"%v, which this build does not know", pmt.State)
	}

	// Recorded before anything is done about it, as the runner does, and
	// then carried out by the runner: it reissues what a paid or failing
	// swap needs until the payer's payment is settled or cancelled.
	if _, err := sd.runner.Driver.Apply(ctx, &rec, event); err != nil {
		return rec, fmt.Errorf("recording the outcome: %w", err)
	}

	s.stoppedMu.Lock()
	delete(s.stopped, hash)
	s.stoppedMu.Unlock()

	log.Infof("Bridge swap %x resolved by the operator: the paying node "+
		"reports %v, so the swap is %v", hash, pmt.State, rec.State)

	s.drive(sd, hash)

	return rec, nil
}
