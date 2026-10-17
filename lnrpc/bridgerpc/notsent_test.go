//go:build bridgerpc
// +build bridgerpc

package bridgerpc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/rpcperms"
	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// What gRPC and lnd actually say when a call never reached a handler, and
// what they say when it may have. Only the first may be called unsent: the
// second, read as unsent, could have a swap paid twice.
func TestNeverDelivered(t *testing.T) {
	t.Parallel()

	unsent := []error{
		// gRPC with no transport, as the lab saw with the node down.
		status.Error(codes.Unavailable, `connection error: desc = `+
			`"transport: Error while dialing: dial tcp `+
			`127.0.0.1:10019: connect: connection refused"`),
		status.Error(codes.Unavailable, "name resolver error: produced "+
			"zero addresses"),
		// lnd before the router is registered.
		status.Error(codes.Unimplemented, "unknown service "+
			"routerrpc.Router"),
		// lnd's interceptor, before any handler.
		status.Error(codes.Unknown, rpcperms.ErrWaitingToStart.Error()),
		status.Error(codes.Unknown, rpcperms.ErrWalletLocked.Error()),
		status.Error(codes.Unknown, rpcperms.ErrNoWallet.Error()),
		status.Error(codes.Unknown, rpcperms.ErrRPCStarting.Error()),
		status.Error(codes.Unknown, "verification failed: signature "+
			"mismatch after caveat verification"),
		status.Error(codes.Unknown, "permission denied"),
		status.Error(codes.Unknown, "cannot get macaroon: root key "+
			"with id 1 doesn't exist"),
	}
	for _, err := range unsent {
		require.True(t, neverDelivered(err), "%v", err)
	}

	ambiguous := []error{
		status.Error(codes.Unavailable, "transport is closing"),
		status.Error(codes.Unavailable, "error reading from server: EOF"),
		status.Error(codes.Unavailable, "connection error: desc = "+
			`"error reading server preface: read: connection reset"`),
		status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
		status.Error(codes.Canceled, "context canceled"),
		// Failures after the payment exists that carry the same words:
		// a database's, an RPC middleware's, a refusal under another
		// code. Only the exact sentences from before the handler count.
		status.Error(codes.Unknown, "pq: permission denied for table "+
			"payments"),
		status.Error(codes.Unknown, "middleware rejected: permission "+
			"denied"),
		status.Error(codes.Internal, "permission denied"),
		status.Error(codes.Unknown, "the RPC server is in the process "+
			"of starting up"),
		status.Error(codes.Unknown, "wallet locked, unlock it to "+
			"enable full RPC access: and then some"),
		status.Error(codes.Unimplemented, "unknown method "+
			"SendPaymentV2 for service routerrpc.Router"),
		errors.New("connection refused"), // not a gRPC status
		context.DeadlineExceeded,
		nil,
	}
	for _, err := range ambiguous {
		require.False(t, neverDelivered(err), "%v", err)
	}
}

func TestRemotePaySaysWhenNothingWasSent(t *testing.T) {
	t.Parallel()

	req := node.PayRequest{
		Invoice: "lnbc1payme", CLTVLimit: 80, Timeout: time.Minute,
	}
	dial := status.Error(codes.Unavailable, `connection error: desc = `+
		`"transport: Error while dialing: dial tcp 127.0.0.1:10019: `+
		`connect: connection refused"`)

	// The stream could not be opened at all.
	_, err := remote(nil, nil, &fakeRouter{sendErr: dial}).Pay(
		context.Background(), req)
	require.ErrorIs(t, err, node.ErrNotSent)

	// It opened and lnd refused before saying anything about the payment.
	_, err = remote(nil, nil, &fakeRouter{send: &fakeStream{
		err: status.Error(codes.Unknown,
			rpcperms.ErrWalletLocked.Error()),
	}}).Pay(context.Background(), req)
	require.ErrorIs(t, err, node.ErrNotSent)

	// A refusal after an update is not a refusal of the payment: the
	// node has already said it is working on it.
	got, err := remote(nil, nil, &fakeRouter{send: &fakeStream{
		updates: []*lnrpc.Payment{{Status: lnrpc.Payment_IN_FLIGHT}},
		err: status.Error(codes.Unknown,
			rpcperms.ErrWalletLocked.Error()),
	}}).Pay(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, node.PaymentInFlight, got.State)

	// An ambiguous failure of the stream is in flight, as before.
	got, err = remote(nil, nil, &fakeRouter{send: &fakeStream{
		err: status.Error(codes.Unavailable, "transport is closing"),
	}}).Pay(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, node.PaymentInFlight, got.State)

	// And an ambiguous failure to open it is an error that is not
	// ErrNotSent, which leaves the swap in Paying.
	_, err = remote(nil, nil, &fakeRouter{
		sendErr: status.Error(codes.Unavailable, "transport is closing"),
	}).Pay(context.Background(), req)
	require.Error(t, err)
	require.NotErrorIs(t, err, node.ErrNotSent)
}

// A refusal the SendPaymentV2 handler sends before writing a payment record is
// unsent, which the node confirms by having no record of the hash. Read as in
// flight it would leave the swap paying with nothing to look up.
func TestRemotePayRefusedBeforeARecordIsNotSent(t *testing.T) {
	t.Parallel()

	req := node.PayRequest{
		Invoice: "lnbc1payme", CLTVLimit: 80, Timeout: time.Minute,
	}
	payReq := &lnrpc.PayReq{PaymentHash: strings.Repeat("ab", 32)}
	notFound := status.Error(codes.NotFound, "payment isn't initiated")
	cltv := status.Error(codes.Unknown, "invalid payment request: cltv "+
		"limit 80 should be greater than 83")

	for _, test := range []struct {
		name string
		err  error
	}{
		{"a CLTV limit too tight for the invoice", cltv},
		{"an invalid argument", status.Error(codes.InvalidArgument,
			"time preference out of range")},
		{"a payment that already exists", status.Error(
			codes.AlreadyExists, "payment is in transition")},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := remote(&fakeMain{payReq: payReq}, nil,
				&fakeRouter{
					send:     &fakeStream{err: test.err},
					trackErr: notFound,
				},
			).Pay(context.Background(), req)
			require.ErrorIs(t, err, node.ErrNotSent)
		})
	}

	// Refused on opening the stream, as a client could see it too.
	_, err := remote(&fakeMain{payReq: payReq}, nil, &fakeRouter{
		sendErr: cltv, trackErr: notFound,
	}).Pay(context.Background(), req)
	require.ErrorIs(t, err, node.ErrNotSent)
}

// Anything that leaves a doubt keeps the payment in flight: a record the node
// does hold, a lookup that fails, an invoice it cannot read back, a refusal
// after the node has spoken about the payment, and the codes that come from
// the transport or a failure partway.
func TestRemotePayRefusedWithARecordStaysInFlight(t *testing.T) {
	t.Parallel()

	req := node.PayRequest{
		Invoice: "lnbc1payme", CLTVLimit: 80, Timeout: time.Minute,
	}
	payReq := &lnrpc.PayReq{PaymentHash: strings.Repeat("ab", 32)}
	notFound := status.Error(codes.NotFound, "payment isn't initiated")
	refused := status.Error(codes.Unknown, "invalid payment request")

	for _, test := range []struct {
		name   string
		main   *fakeMain
		router *fakeRouter
	}{
		{
			name: "the node holds a record",
			main: &fakeMain{payReq: payReq},
			router: &fakeRouter{
				send: &fakeStream{err: refused},
				track: &fakeStream{updates: []*lnrpc.Payment{
					{Status: lnrpc.Payment_IN_FLIGHT},
				}},
			},
		},
		{
			name: "the lookup fails",
			main: &fakeMain{payReq: payReq},
			router: &fakeRouter{
				send: &fakeStream{err: refused},
				trackErr: status.Error(codes.Unavailable,
					"transport is closing"),
			},
		},
		{
			name: "the invoice cannot be read back",
			main: &fakeMain{payReqErr: status.Error(
				codes.Unavailable, "transport is closing")},
			router: &fakeRouter{
				send:     &fakeStream{err: refused},
				trackErr: notFound,
			},
		},
		{
			name: "the refusal follows an update",
			main: &fakeMain{payReq: payReq},
			router: &fakeRouter{
				send: &fakeStream{
					updates: []*lnrpc.Payment{
						{Status: lnrpc.Payment_IN_FLIGHT},
					},
					err: refused,
				},
				trackErr: notFound,
			},
		},
		{
			name: "an internal error",
			main: &fakeMain{payReq: payReq},
			router: &fakeRouter{
				send: &fakeStream{err: status.Error(
					codes.Internal, "invalid payment request")},
				trackErr: notFound,
			},
		},
		{
			name: "a cancelled stream",
			main: &fakeMain{payReq: payReq},
			router: &fakeRouter{
				send: &fakeStream{err: status.Error(
					codes.Canceled, "context canceled")},
				trackErr: notFound,
			},
		},
		{
			name: "a passed deadline",
			main: &fakeMain{payReq: payReq},
			router: &fakeRouter{
				send: &fakeStream{err: status.Error(
					codes.DeadlineExceeded,
					"context deadline exceeded")},
				trackErr: notFound,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := remote(test.main, nil, test.router).Pay(
				context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, node.PaymentInFlight, got.State)
		})
	}
}
