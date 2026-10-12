package lnd

import (
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainreg"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/channelnotifier"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/shachain"
	"github.com/stretchr/testify/require"
)

// TestChannelBackupRefreshOnSplice checks a splice step sends the channel to
// the backup again, read afresh, named by the outpoint it was opened with.
func TestChannelBackupRefreshOnSplice(t *testing.T) {
	db, err := channeldb.MakeTestDB(t)
	require.NoError(t, err)
	cdb := db.ChannelStateDB()

	priv, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	remote := keychain.KeyDescriptor{PubKey: priv.PubKey()}
	identity := wire.OutPoint{Hash: chainhash.Hash{4}}
	require.NoError(t, cdb.RestoreChannelShells(&channeldb.ChannelShell{
		Chan: &channeldb.OpenChannel{
			ChainHash:               chainreg.BitcoinMainNetParams.ChainHash,
			FundingOutpoint:         identity,
			IdentityPub:             priv.PubKey(),
			RemoteCurrentRevocation: priv.PubKey(),
			RevocationStore:         shachain.NewRevocationStore(),
			RevocationProducer: shachain.NewRevocationProducer(
				chainhash.Hash{},
			),
			LocalChanCfg: channeldb.ChannelConfig{
				MultiSigKey: remote,
			},
			RemoteChanCfg: channeldb.ChannelConfig{
				MultiSigKey:         remote,
				RevocationBasePoint: remote,
				PaymentBasePoint:    remote,
				DelayBasePoint:      remote,
				HtlcBasePoint:       remote,
			},
		},
	}))

	notifier := channelnotifier.New(cdb)
	require.NoError(t, notifier.Start())
	t.Cleanup(func() { _ = notifier.Stop() })

	proxy := &channelNotifier{
		chanNotifier: notifier, addrs: db, chanDB: cdb,
	}
	sub, err := proxy.SubscribeChans(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(sub.Cancel)

	// The proxy subscribes from its own goroutine: send until it hears.
	event := channelnotifier.SpliceEvent{
		ChannelPoint:         wire.OutPoint{Hash: chainhash.Hash{5}},
		OriginalChannelPoint: identity,
		Kind:                 "locked",
	}
	deadline := time.After(5 * time.Second)
	for {
		notifier.NotifySpliceEvent(event)
		select {
		case update := <-sub.ChanUpdates:
			require.Len(t, update.NewChans, 1)
			require.Equal(t, identity,
				update.NewChans[0].FundingOutpoint)
			require.Empty(t, update.ClosedChans)

			return

		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			t.Fatal("no backup update for the splice")
		}
	}
}
