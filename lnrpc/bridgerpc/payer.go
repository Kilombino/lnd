//go:build bridgerpc
// +build bridgerpc

package bridgerpc

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/macaroons"
	"github.com/paulscode/lightning-fork-bridge/quote"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"gopkg.in/macaroon-bakery.v2/bakery"
	"gopkg.in/macaroon.v2"
)

// PayerAPIVersion is the version of the payer API this bridge speaks: the
// calls a wallet makes, the codes it branches on, and the bridge code it is
// given. It changes only when one of those does.
const PayerAPIVersion = 1

// operatorRootKeyID is the root key every macaroon the node creates for
// itself is baked under, and the participant every such macaroon is counted as
// unless it is the operator's own (see participantOf).
const operatorRootKeyID = "0"

// unknownParticipant stands for a caller whose macaroon cannot be read. Such a
// caller is limited rather than taken for the operator: authentication has
// already passed, so this should not happen, and if it does the cautious
// reading is the one that bounds what they can reserve.
const unknownParticipant = "unknown"

// participantOf is who is asking, for the limits each participant has on their
// own: the root key id of the caller's macaroon, or "" for the operator.
//
// A participant's macaroon is baked with a root key of its own, which is what
// makes it revocable alone (lncli deletemacaroonid) and what identifies it
// here. Nothing secret is read: the id is the macaroon's public identifier,
// and the macaroon itself was verified before this call was reached.
//
// The operator is a macaroon under the node's own root key that can also make
// macaroons: admin.macaroon, or one baked as capable. Every other macaroon
// under that root key (invoice.macaroon, one baked for an app with offchain
// write) is a participant, and they share one set of limits: being able to
// ask for a quote is not being the operator, and an app holding one of those
// should not be able to reserve the bridge's liquidity without limit.
func participantOf(ctx context.Context) string {
	raw, err := macaroons.RawMacaroonFromContext(ctx)
	if err != nil {
		// No macaroon at all is a node running without them, where
		// every caller is the operator.
		return ""
	}

	bytes, err := hex.DecodeString(raw)
	if err != nil {
		return unknownParticipant
	}
	mac := &macaroon.Macaroon{}
	if err := mac.UnmarshalBinary(bytes); err != nil {
		return unknownParticipant
	}
	id := mac.Id()
	if len(id) < 2 || id[0] != byte(bakery.LatestVersion) {
		return unknownParticipant
	}
	decoded := &lnrpc.MacaroonId{}
	if err := proto.Unmarshal(id[1:], decoded); err != nil {
		return unknownParticipant
	}

	storage := string(decoded.GetStorageId())
	if storage != operatorRootKeyID && storage != "" {
		return storage
	}
	if canGenerate(decoded.GetOps()) {
		return ""
	}

	return operatorRootKeyID
}

// canGenerate reports whether a macaroon's operations include making
// macaroons, which only the operator's own carries.
//
// Read from the macaroon's id, which its signature covers, so a holder cannot
// add the operation without the root key.
func canGenerate(ops []*lnrpc.Op) bool {
	for _, op := range ops {
		if op.GetEntity() != "macaroon" {
			continue
		}
		for _, action := range op.GetActions() {
			if action == "generate" {
				return true
			}
		}
	}

	return false
}

// refusal turns an error from quoting into a status a payer can act on: a
// stable code at the front of the message, then a fixed sentence for that
// code, and an HTTP status through the REST gateway that says whether to try
// again.
//
// The sentence is the code's own, never the error's. The error can carry what
// is the operator's business alone: how much the paying side has left, the
// route budget a quote was sized to, the SHA256 node's address in a
// connection error, the state of a swap someone else quoted for the same hash,
// and paths and database detail. It goes to the log; the payer gets the code,
// which is what a wallet branches on, and a sentence that says no more.
func refusal(err error) error {
	code := quote.CodeOf(err)
	if code == quote.CodeInternal {
		log.Errorf("Bridge could not quote: %v", err)
	} else {
		log.Infof("Bridge refused a quote (%s): %v", code, err)
	}
	msg := string(code) + ": " + refusalSentence(code)

	switch code {
	case quote.CodeInProgress, quote.CodeAlreadyPaid,
		quote.CodeNeedsOperator, quote.CodeFundedBefore:

		return status.Error(codes.AlreadyExists, msg)

	case quote.CodeLimit:
		return status.Error(codes.ResourceExhausted, msg)

	case quote.CodeUnavailable:
		return status.Error(codes.Unavailable, msg)

	case quote.CodeInternal:
		return status.Error(codes.Internal, msg)
	}

	return status.Error(codes.FailedPrecondition, msg)
}

// refusalSentences is what a payer is told for each refusal code.
var refusalSentences = map[quote.Code]string{
	quote.CodeInvalidInvoice: "the invoice cannot be read, or the " +
		"paying node would not pay it",
	quote.CodeNoDirection: "this bridge does not pay invoices on that " +
		"chain",
	quote.CodeNoAmount: quote.ErrNoAmount.Error(),
	quote.CodeTooSmall: "the amount is below this bridge's minimum; " +
		"its info gives the bounds",
	quote.CodeTooLarge: "the amount is above what this bridge can pay " +
		"now; its info gives the bounds",
	quote.CodeExpiresSoon: "the invoice expires too soon to be paid; " +
		"ask the recipient for a new one",
	quote.CodeRouteBudget: "the invoice's final CLTV delta leaves no " +
		"room for a route within this bridge's limits",
	quote.CodeNoRoute: "the bridge has no route to the destination " +
		"within its limits",
	quote.CodeSelfPayment:   quote.ErrSelfPayment.Error(),
	quote.CodeNoLiquidity:   "not enough liquidity to pay this now",
	quote.CodeInProgress:    quote.ErrInProgress.Error(),
	quote.CodeAlreadyPaid:   quote.ErrAlreadyPaid.Error(),
	quote.CodeNeedsOperator: quote.ErrNeedsOperator.Error(),
	quote.CodeFundedBefore: "an earlier attempt to pay this invoice " +
		"was funded; ask the recipient for a new invoice",
	quote.CodeLimit: "this bridge code has reached its limit of " +
		"unpaid quotes, swaps in progress or quotes a minute; try " +
		"again later",
	quote.CodePriceUnavailable: "the bridge has no usable rate right " +
		"now; try again later",
	quote.CodeChainUnmeasured: "the bridge cannot size a swap against " +
		"the chains right now; try again later",
	quote.CodeUnavailable: "the bridge cannot quote this right now; " +
		"try again later",
	quote.CodeInternal: "the bridge could not quote this invoice; try " +
		"again later",
}

// refusalSentence is the fixed sentence for a refusal code, and a general one
// for a code this build does not know.
func refusalSentence(code quote.Code) string {
	if s, ok := refusalSentences[code]; ok {
		return s
	}

	return "the bridge declined to quote this invoice"
}

// forParticipant is an Info answer as a participant sees it: each shut
// direction's refusal as its code's fixed sentence, as a quote's would be,
// and the most a direction can pay rounded down.
//
// What a direction can pay is bounded by what is left on its paying side, so
// the exact number would tell every participant the bridge's outbound to the
// satoshi, and how it moves with each swap. Two significant figures say how
// large a payment to try without that.
func forParticipant(resp *InfoResponse) *InfoResponse {
	for _, d := range resp.GetDirections() {
		if d.GetRefusalCode() != "" {
			d.Refusal = refusalSentence(quote.Code(d.GetRefusalCode()))
		}
		d.MaxMsat = max(coarseMsat(d.GetMaxMsat()), d.GetMinMsat())
	}

	return resp
}

// coarseMsat rounds an amount down to whole satoshis and then to two
// significant figures of them.
func coarseMsat(msat uint64) uint64 {
	sat := msat / 1000
	step := uint64(1)
	for sat/step >= 100 {
		step *= 10
	}

	return sat / step * step * 1000
}

// coded is an error with a code the quote package does not produce, in the
// same form as refusal.
func coded(c codes.Code, code, sentence string) error {
	return status.Error(c, code+": "+sentence)
}

// isUntracked reports whether a quote failed after its swap was recorded, so
// the swap must be driven anyway.
func isUntracked(err error) bool {
	return errors.Is(err, quote.ErrUntracked)
}
