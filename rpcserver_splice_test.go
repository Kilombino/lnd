package lnd

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/macaroon-bakery.v2/bakery"
)

// TestSplicePermissions pins the permissions of the splice RPCs. Starting a
// splice moves funds between the wallet and a channel, so it needs on-chain
// and off-chain write, as opening and closing do: a macaroon with
// offchain:write alone (the dashboard's "Lightning wallet" preset promises it
// can't send on-chain) can't splice. Estimating is read-only.
func TestSplicePermissions(t *testing.T) {
	t.Parallel()

	perms := MainRPCServerPermissions()

	require.ElementsMatch(t, []bakery.Op{
		{Entity: "onchain", Action: "write"},
		{Entity: "offchain", Action: "write"},
	}, perms["/lnrpc.Lightning/SpliceChannel"])

	require.ElementsMatch(t, []bakery.Op{
		{Entity: "onchain", Action: "read"},
		{Entity: "offchain", Action: "read"},
	}, perms["/lnrpc.Lightning/EstimateSplice"])

	// The same as opening and closing a channel.
	require.ElementsMatch(t, perms["/lnrpc.Lightning/OpenChannel"],
		perms["/lnrpc.Lightning/SpliceChannel"])
	require.ElementsMatch(t, perms["/lnrpc.Lightning/CloseChannel"],
		perms["/lnrpc.Lightning/SpliceChannel"])
}
