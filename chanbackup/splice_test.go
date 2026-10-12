package chanbackup

import (
	"bytes"
	"net"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lnencrypt"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/shachain"
	"github.com/stretchr/testify/require"
)

// testSpliceFunding returns a splice funding with a fresh peer key.
func testSpliceFunding(t *testing.T, txid byte, scid uint64) SpliceFunding {
	t.Helper()

	priv, err := btcec.NewPrivateKey()
	require.NoError(t, err)

	return SpliceFunding{
		OutPoint:       wire.OutPoint{Hash: chainhash.Hash{txid}, Index: 1},
		ShortChannelID: lnwire.NewShortChanIDFromInt(scid),
		Capacity:       btcutil.Amount(1_000_000 + int64(txid)),
		LocalKey: keychain.KeyDescriptor{
			KeyLocator: keychain.KeyLocator{
				Family: keychain.KeyFamilyMultiSig,
				Index:  uint32(txid),
			},
		},
		RemoteKey: priv.PubKey(),
	}
}

// requireSpliceEqual checks two splice backups carry the same fundings.
func requireSpliceEqual(t *testing.T, want, got *SpliceBackup) {
	t.Helper()

	if want == nil {
		require.Nil(t, got)
		return
	}
	require.NotNil(t, got)

	check := func(w, g SpliceFunding) {
		require.Equal(t, w.OutPoint, g.OutPoint)
		require.Equal(t, w.ShortChannelID, g.ShortChannelID)
		require.Equal(t, w.Capacity, g.Capacity)
		require.Equal(t, w.LocalKey.KeyLocator, g.LocalKey.KeyLocator)
		require.True(t, w.RemoteKey.IsEqual(g.RemoteKey))
	}
	require.Equal(t, want.Current == nil, got.Current == nil)
	if want.Current != nil {
		check(*want.Current, *got.Current)
	}
	require.Len(t, got.Candidates, len(want.Candidates))
	for i := range want.Candidates {
		check(want.Candidates[i], got.Candidates[i])
	}
}

// testSplicedSingles returns three backups: one moved by a splice with
// another pending, one with a splice pending only, and one without.
func testSplicedSingles(t *testing.T) []Single {
	t.Helper()

	var singles []Single
	for range 3 {
		channel, err := genRandomOpenChannelShell()
		require.NoError(t, err)
		singles = append(singles, NewSingle(
			channel, []net.Addr{addr1, addr2},
		))
	}

	current := testSpliceFunding(t, 1, 700_000<<40)
	singles[0].Splice = &SpliceBackup{
		Current:    &current,
		Candidates: []SpliceFunding{testSpliceFunding(t, 2, 0)},
	}
	singles[1].Splice = &SpliceBackup{
		Candidates: []SpliceFunding{
			testSpliceFunding(t, 3, 0), testSpliceFunding(t, 4, 0),
		},
	}

	return singles
}

// TestSpliceTrailerMulti checks a multi-channel backup carries each channel's
// splices to the channel they belong to.
func TestSpliceTrailerMulti(t *testing.T) {
	t.Parallel()

	singles := testSplicedSingles(t)
	keyRing := &lnencrypt.MockKeyRing{}

	var b bytes.Buffer
	multi := Multi{StaticBackups: singles}
	require.NoError(t, multi.PackToWriter(&b, keyRing))

	var unpacked Multi
	require.NoError(t, unpacked.UnpackFromReader(&b, keyRing))
	require.Len(t, unpacked.StaticBackups, len(singles))
	for i := range singles {
		assertSingleEqual(t, singles[i], unpacked.StaticBackups[i])
		requireSpliceEqual(
			t, singles[i].Splice, unpacked.StaticBackups[i].Splice,
		)
	}
}

// TestSpliceTrailerSingle checks a single-channel backup carries its splices.
func TestSpliceTrailerSingle(t *testing.T) {
	t.Parallel()

	keyRing := &lnencrypt.MockKeyRing{}
	for _, single := range testSplicedSingles(t) {
		var b bytes.Buffer
		require.NoError(t, single.PackToWriter(&b, keyRing))

		var unpacked Single
		require.NoError(t, unpacked.UnpackFromReader(&b, keyRing))
		assertSingleEqual(t, single, unpacked)
		requireSpliceEqual(t, single.Splice, unpacked.Splice)
	}
}

// decrypt returns a packed backup's plaintext.
func decrypt(t *testing.T, packed []byte) []byte {
	t.Helper()

	e, err := lnencrypt.KeyRingEncrypter(&lnencrypt.MockKeyRing{})
	require.NoError(t, err)
	plaintext, err := e.DecryptPayloadFromReader(bytes.NewReader(packed))
	require.NoError(t, err)

	return plaintext
}

// encrypt packs a plaintext.
func encrypt(t *testing.T, plaintext []byte) []byte {
	t.Helper()

	e, err := lnencrypt.KeyRingEncrypter(&lnencrypt.MockKeyRing{})
	require.NoError(t, err)
	var b bytes.Buffer
	require.NoError(t, e.EncryptPayloadToWriter(plaintext, &b))

	return b.Bytes()
}

// TestSpliceTrailerOlderVersion checks a version without splices reads every
// channel of a backup that has them, each as it was opened: it reads a
// multi-channel backup by count and a single one by its fields, which is what
// the decoding here does, Deserialize being unchanged.
func TestSpliceTrailerOlderVersion(t *testing.T) {
	t.Parallel()

	singles := testSplicedSingles(t)
	keyRing := &lnencrypt.MockKeyRing{}

	var b bytes.Buffer
	multi := Multi{StaticBackups: singles}
	require.NoError(t, multi.PackToWriter(&b, keyRing))

	r := bytes.NewReader(decrypt(t, b.Bytes()))
	var (
		version byte
		count   uint32
	)
	require.NoError(t, lnwire.ReadElements(r, &version, &count))
	require.EqualValues(t, len(singles), count)
	for i := range count {
		var s Single
		require.NoError(t, s.Deserialize(r))
		assertSingleEqual(t, singles[i], s)
	}

	// What it doesn't read is the trailer.
	var magic [4]byte
	_, err := r.Read(magic[:])
	require.NoError(t, err)
	require.Equal(t, spliceTrailerMagic, magic)

	// The same for a single-channel backup.
	b.Reset()
	require.NoError(t, singles[0].PackToWriter(&b, keyRing))
	var s Single
	require.NoError(t, s.Deserialize(
		bytes.NewReader(decrypt(t, b.Bytes())),
	))
	assertSingleEqual(t, singles[0], s)
}

// TestSpliceTrailerUnknown checks data after the backups that this version
// doesn't know is skipped, and a damaged trailer skipped whole.
func TestSpliceTrailerUnknown(t *testing.T) {
	t.Parallel()

	singles := testSplicedSingles(t)
	keyRing := &lnencrypt.MockKeyRing{}

	var b bytes.Buffer
	plain := Multi{StaticBackups: singles[2:]}
	require.NoError(t, plain.PackToWriter(&b, keyRing))
	base := decrypt(t, b.Bytes())

	unpack := func(plaintext []byte) (*Multi, error) {
		var m Multi
		err := m.UnpackFromReader(
			bytes.NewReader(encrypt(t, plaintext)), keyRing,
		)

		return &m, err
	}

	// Another format's data.
	m, err := unpack(append(append([]byte{}, base...), "XYZW1234"...))
	require.NoError(t, err)
	require.Len(t, m.StaticBackups, 1)
	require.Nil(t, m.StaticBackups[0].Splice)

	// A later trailer format.
	later := append(append([]byte{}, base...), spliceTrailerMagic[:]...)
	later = append(later, spliceTrailerVersion+1, 0xff)
	m, err = unpack(later)
	require.NoError(t, err)
	require.Nil(t, m.StaticBackups[0].Splice)

	// Splice data for a channel the backup doesn't have.
	var trailer bytes.Buffer
	stranger := singles[0]
	stranger.FundingOutpoint.Index++
	require.NoError(t, writeSpliceTrailer(&trailer, []Single{stranger}))
	m, err = unpack(append(append([]byte{}, base...), trailer.Bytes()...))
	require.NoError(t, err)
	require.Len(t, m.StaticBackups, 1)
	require.Nil(t, m.StaticBackups[0].Splice)

	// A cut trailer.
	trailer.Reset()
	own := singles[2]
	own.Splice = singles[0].Splice
	require.NoError(t, writeSpliceTrailer(&trailer, []Single{own}))
	full := append(append([]byte{}, base...), trailer.Bytes()...)
	m, err = unpack(full)
	require.NoError(t, err)
	requireSpliceEqual(t, own.Splice, m.StaticBackups[0].Splice)
	m, err = unpack(full[:len(full)-3])
	require.NoError(t, err)
	require.Len(t, m.StaticBackups, 1)
	require.Nil(t, m.StaticBackups[0].Splice)

	// So is a damaged one: the channels restore without it. Here one
	// entry is fine and the next names no backup: neither is taken.
	var both bytes.Buffer
	stranger.Splice = own.Splice
	require.NoError(t, writeSpliceTrailer(&both, []Single{own, stranger}))
	m, err = unpack(append(append([]byte{}, base...), both.Bytes()...))
	require.NoError(t, err)
	require.Nil(t, m.StaticBackups[0].Splice)
}

// TestBuildSingleSpliced checks the backup of a channel a splice moved
// describes it as opened, the original keys and capacity, and carries where
// it is now and the splice pending on it.
func TestBuildSingleSpliced(t *testing.T) {
	t.Parallel()

	db, err := channeldb.MakeTestDB(t)
	require.NoError(t, err)
	cdb := db.ChannelStateDB()

	shell, err := genRandomOpenChannelShell()
	require.NoError(t, err)
	shell.ChanType = channeldb.SingleFunderTweaklessBit
	shell.LocalCommitment = channeldb.ChannelCommitment{}
	shell.Capacity = 500_000
	shell.RemoteCurrentRevocation = shell.IdentityPub
	shell.RevocationStore = shachain.NewRevocationStore()
	localPriv, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	shell.LocalChanCfg.MultiSigKey.PubKey = localPriv.PubKey()
	original := shell.LocalChanCfg.MultiSigKey
	originalRemote := shell.RemoteChanCfg.MultiSigKey.PubKey
	require.NoError(t, cdb.RestoreChannelShells(&channeldb.ChannelShell{
		Chan: shell,
	}))
	channel, err := cdb.FetchChannel(shell.FundingOutpoint)
	require.NoError(t, err)

	// Before any splice: nothing extra.
	single, err := BuildSingle(channel, nil)
	require.NoError(t, err)
	require.Nil(t, single.Splice)

	current := testSpliceFunding(t, 9, 800_000<<40)
	newLocal, err := btcec.NewPrivateKey()
	require.NoError(t, err)
	current.LocalKey.PubKey = newLocal.PubKey()
	require.NoError(t, channel.LockSplice(channeldb.SpliceLock{
		Funding: channeldb.SpliceFunding{
			OutPoint: current.OutPoint,
			Scid:     current.ShortChannelID,
		},
		Capacity:          current.Capacity,
		LocalMultiSigKey:  current.LocalKey,
		RemoteMultiSigKey: keychain.KeyDescriptor{PubKey: current.RemoteKey},
	}))

	candidate := testSpliceFunding(t, 10, 0)
	candidate.LocalKey.PubKey = newLocal.PubKey()
	require.NoError(t, channel.PutSpliceRecord(&channeldb.SpliceRecord{
		OutPoint:  candidate.OutPoint,
		Capacity:  candidate.Capacity,
		LocalKey:  candidate.LocalKey,
		RemoteKey: candidate.RemoteKey,
	}))

	// A funding a lock moved the channel away from, kept for its revoked
	// commitments, is no candidate.
	retired := testSpliceFunding(t, 11, 0)
	require.NoError(t, channel.PutSpliceRecord(&channeldb.SpliceRecord{
		OutPoint:  retired.OutPoint,
		Capacity:  retired.Capacity,
		LocalKey:  keychain.KeyDescriptor{PubKey: newLocal.PubKey()},
		RemoteKey: retired.RemoteKey,
		Retired:   true,
	}))

	single, err = BuildSingle(channel, nil)
	require.NoError(t, err)
	require.Equal(t, shell.FundingOutpoint, single.FundingOutpoint)
	require.Equal(t, btcutil.Amount(500_000), single.Capacity)
	require.Equal(t, original.KeyLocator,
		single.LocalChanCfg.MultiSigKey.KeyLocator)
	require.True(t, originalRemote.IsEqual(
		single.RemoteChanCfg.MultiSigKey.PubKey,
	))
	requireSpliceEqual(t, &SpliceBackup{
		Current:    &current,
		Candidates: []SpliceFunding{candidate},
	}, single.Splice)
}
