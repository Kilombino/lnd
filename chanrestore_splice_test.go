package lnd

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/chainreg"
	"github.com/lightningnetwork/lnd/chanbackup"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lntest/mock"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/stretchr/testify/require"
)

// locatorKeyRing derives a distinct key per locator and records the key each
// ECDH was made with.
type locatorKeyRing struct {
	mock.SecretKeyRing

	mu   sync.Mutex
	ecdh []*btcec.PublicKey
}

// priv returns the locator's key.
func (r *locatorKeyRing) priv(loc keychain.KeyLocator) *btcec.PrivateKey {
	var b [8]byte
	binary.BigEndian.PutUint32(b[:4], uint32(loc.Family))
	binary.BigEndian.PutUint32(b[4:], loc.Index)
	seed := sha256.Sum256(b[:])
	priv, _ := btcec.PrivKeyFromBytes(seed[:])

	return priv
}

func (r *locatorKeyRing) DeriveKey(
	loc keychain.KeyLocator) (keychain.KeyDescriptor, error) {

	return keychain.KeyDescriptor{
		KeyLocator: loc, PubKey: r.priv(loc).PubKey(),
	}, nil
}

func (r *locatorKeyRing) ECDH(_ keychain.KeyDescriptor,
	pub *btcec.PublicKey) ([32]byte, error) {

	r.mu.Lock()
	r.ecdh = append(r.ecdh, pub)
	r.mu.Unlock()

	return sha256.Sum256(pub.SerializeCompressed()), nil
}

// multiSigLoc is a funding key's locator.
func multiSigLoc(i uint32) keychain.KeyDescriptor {
	return keychain.KeyDescriptor{KeyLocator: keychain.KeyLocator{
		Family: keychain.KeyFamilyMultiSig, Index: i,
	}}
}

// TestRestoreSplicedChannel checks a channel restored from the backup of a
// channel a splice moved, with another splice pending: it is put on the
// funding output it was on, with that output's keys and capacity, the
// pending splice is recorded so a spend by it is followed, the shachain root
// comes from the original funding key, and the restored channel's own backup
// is the one it came from.
func TestRestoreSplicedChannel(t *testing.T) {
	db, err := channeldb.MakeTestDB(t)
	require.NoError(t, err)
	cdb := db.ChannelStateDB()
	ring := &locatorKeyRing{}

	peerKey := func() *btcec.PublicKey {
		priv, err := btcec.NewPrivateKey()
		require.NoError(t, err)
		return priv.PubKey()
	}
	remote := peerKey()
	loc := func(f keychain.KeyFamily, i uint32) keychain.KeyDescriptor {
		return keychain.KeyDescriptor{KeyLocator: keychain.KeyLocator{
			Family: f, Index: i,
		}}
	}

	chainHash := chainreg.BitcoinMainNetParams.ChainHash
	identity := wire.OutPoint{Hash: chainhash.Hash{1}, Index: 0}
	current := chanbackup.SpliceFunding{
		OutPoint:       wire.OutPoint{Hash: chainhash.Hash{2}, Index: 0},
		ShortChannelID: lnwire.NewShortChanIDFromInt(900_000 << 40),
		Capacity:       2_000_000,
		LocalKey:       multiSigLoc(5),
		RemoteKey:      peerKey(),
	}
	candidate := chanbackup.SpliceFunding{
		OutPoint:  wire.OutPoint{Hash: chainhash.Hash{3}, Index: 1},
		Capacity:  2_500_000,
		LocalKey:  multiSigLoc(6),
		RemoteKey: peerKey(),
	}
	single := chanbackup.Single{
		Version:         chanbackup.AnchorsZeroFeeHtlcTxCommitVersion,
		ChainHash:       chainHash,
		FundingOutpoint: identity,
		ShortChannelID:  lnwire.NewShortChanIDFromInt(800_000 << 40),
		RemoteNodePub:   remote,
		Capacity:        1_000_000,
		LocalChanCfg: channeldb.ChannelConfig{
			MultiSigKey:         multiSigLoc(1),
			RevocationBasePoint: loc(keychain.KeyFamilyRevocationBase, 1),
			PaymentBasePoint:    loc(keychain.KeyFamilyPaymentBase, 1),
			DelayBasePoint:      loc(keychain.KeyFamilyDelayBase, 1),
			HtlcBasePoint:       loc(keychain.KeyFamilyHtlcBase, 1),
		},
		RemoteChanCfg: channeldb.ChannelConfig{
			MultiSigKey:         keychain.KeyDescriptor{PubKey: remote},
			RevocationBasePoint: keychain.KeyDescriptor{PubKey: remote},
			PaymentBasePoint:    keychain.KeyDescriptor{PubKey: remote},
			DelayBasePoint:      keychain.KeyDescriptor{PubKey: remote},
			HtlcBasePoint:       keychain.KeyDescriptor{PubKey: remote},
		},
		ShaChainRootDesc: loc(keychain.KeyFamilyRevocationRoot, 1),
		Splice: &chanbackup.SpliceBackup{
			Current:    &current,
			Candidates: []chanbackup.SpliceFunding{candidate},
		},
	}

	restorer := &chanDBRestorer{
		db: cdb, secretKeys: ring, chainHash: chainHash,
	}
	shell, err := restorer.openChannelShell(single)
	require.NoError(t, err)
	require.NoError(t, cdb.RestoreChannelShells(shell))

	// The shachain root from the original funding key.
	original := ring.priv(single.LocalChanCfg.MultiSigKey.KeyLocator)
	require.Len(t, ring.ecdh, 1)
	require.True(t, original.PubKey().IsEqual(ring.ecdh[0]))

	// On the current funding output, found by it too.
	channel, err := cdb.FetchChannel(identity)
	require.NoError(t, err)
	require.True(t, channel.HasChanStatus(channeldb.ChanStatusRestored))
	require.Equal(t, current.OutPoint, channel.CurrentFundingOutpoint())
	require.Equal(t, current.Capacity, channel.Capacity)
	require.True(t, ring.priv(current.LocalKey.KeyLocator).PubKey().IsEqual(
		channel.LocalChanCfg.MultiSigKey.PubKey,
	))
	require.True(t, current.RemoteKey.IsEqual(
		channel.RemoteChanCfg.MultiSigKey.PubKey,
	))
	byCurrent, err := cdb.FetchChannel(current.OutPoint)
	require.NoError(t, err)
	require.Equal(t, identity, byCurrent.FundingOutpoint)

	// The pending splice, with its key derived.
	records, err := channel.SpliceRecords()
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, candidate.OutPoint, records[0].OutPoint)
	require.Equal(t, candidate.Capacity, records[0].Capacity)
	require.True(t, ring.priv(candidate.LocalKey.KeyLocator).PubKey().
		IsEqual(records[0].LocalKey.PubKey))
	require.False(t, records[0].Retired)

	// The restored channel's own backup is the one it came from.
	again, err := chanbackup.BuildSingle(channel, nil)
	require.NoError(t, err)
	require.Equal(t, single.Capacity, again.Capacity)
	require.Equal(t, single.LocalChanCfg.MultiSigKey.KeyLocator,
		again.LocalChanCfg.MultiSigKey.KeyLocator)
	require.True(t, remote.IsEqual(again.RemoteChanCfg.MultiSigKey.PubKey))
	require.NotNil(t, again.Splice)
	require.NotNil(t, again.Splice.Current)
	require.Equal(t, current.OutPoint, again.Splice.Current.OutPoint)
	require.Equal(t, current.ShortChannelID,
		again.Splice.Current.ShortChannelID)
	require.Equal(t, current.LocalKey.KeyLocator,
		again.Splice.Current.LocalKey.KeyLocator)
	require.Len(t, again.Splice.Candidates, 1)
	require.Equal(t, candidate.OutPoint, again.Splice.Candidates[0].OutPoint)
}

// TestRestoreUnsplicedChannel checks a backup without splices restores as
// before: on the funding output it names.
func TestRestoreUnsplicedChannel(t *testing.T) {
	db, err := channeldb.MakeTestDB(t)
	require.NoError(t, err)
	cdb := db.ChannelStateDB()

	priv, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	remote := priv.PubKey()
	chainHash := chainreg.BitcoinMainNetParams.ChainHash
	identity := wire.OutPoint{Hash: chainhash.Hash{7}}
	single := chanbackup.Single{
		Version:         chanbackup.AnchorsZeroFeeHtlcTxCommitVersion,
		ChainHash:       chainHash,
		FundingOutpoint: identity,
		RemoteNodePub:   remote,
		Capacity:        1_000_000,
		LocalChanCfg:    channeldb.ChannelConfig{MultiSigKey: multiSigLoc(1)},
		RemoteChanCfg: channeldb.ChannelConfig{
			MultiSigKey:         keychain.KeyDescriptor{PubKey: remote},
			RevocationBasePoint: keychain.KeyDescriptor{PubKey: remote},
			PaymentBasePoint:    keychain.KeyDescriptor{PubKey: remote},
			DelayBasePoint:      keychain.KeyDescriptor{PubKey: remote},
			HtlcBasePoint:       keychain.KeyDescriptor{PubKey: remote},
		},
	}
	restorer := &chanDBRestorer{
		db: cdb, secretKeys: &locatorKeyRing{}, chainHash: chainHash,
	}
	shell, err := restorer.openChannelShell(single)
	require.NoError(t, err)
	require.Nil(t, shell.Splice)
	require.NoError(t, cdb.RestoreChannelShells(shell))

	channel, err := cdb.FetchChannel(identity)
	require.NoError(t, err)
	require.Equal(t, identity, channel.CurrentFundingOutpoint())
	records, err := channel.SpliceRecords()
	require.NoError(t, err)
	require.Empty(t, records)
}

// TestPendingSplicesRestored checks a restored channel's splice records don't
// keep splicing advertised: they only say what its backup knew. (A restored
// channel isn't among the open channels read.)
func TestPendingSplicesRestored(t *testing.T) {
	db, err := channeldb.MakeTestDB(t)
	require.NoError(t, err)
	cdb := db.ChannelStateDB()

	priv, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	remote := priv.PubKey()
	chainHash := chainreg.BitcoinMainNetParams.ChainHash
	candidate := chanbackup.SpliceFunding{
		OutPoint:  wire.OutPoint{Hash: chainhash.Hash{8}},
		Capacity:  2_000_000,
		LocalKey:  multiSigLoc(6),
		RemoteKey: remote,
	}
	single := chanbackup.Single{
		Version:         chanbackup.AnchorsZeroFeeHtlcTxCommitVersion,
		ChainHash:       chainHash,
		FundingOutpoint: wire.OutPoint{Hash: chainhash.Hash{7}},
		RemoteNodePub:   remote,
		Capacity:        1_000_000,
		LocalChanCfg:    channeldb.ChannelConfig{MultiSigKey: multiSigLoc(1)},
		RemoteChanCfg: channeldb.ChannelConfig{
			MultiSigKey:         keychain.KeyDescriptor{PubKey: remote},
			RevocationBasePoint: keychain.KeyDescriptor{PubKey: remote},
			PaymentBasePoint:    keychain.KeyDescriptor{PubKey: remote},
			DelayBasePoint:      keychain.KeyDescriptor{PubKey: remote},
			HtlcBasePoint:       keychain.KeyDescriptor{PubKey: remote},
		},
		Splice: &chanbackup.SpliceBackup{
			Candidates: []chanbackup.SpliceFunding{candidate},
		},
	}
	restorer := &chanDBRestorer{
		db: cdb, secretKeys: &locatorKeyRing{}, chainHash: chainHash,
	}
	shell, err := restorer.openChannelShell(single)
	require.NoError(t, err)
	require.NoError(t, cdb.RestoreChannelShells(shell))

	channel, err := cdb.FetchChannel(single.FundingOutpoint)
	require.NoError(t, err)
	records, err := channel.SpliceRecords()
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.False(t, pendingSplices(cdb))
}
