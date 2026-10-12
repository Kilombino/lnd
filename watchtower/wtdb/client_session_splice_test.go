package wtdb

import (
	"bytes"
	"testing"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/watchtower/blob"
	"github.com/stretchr/testify/require"
)

// TestBackupIDSplice checks a backup of a commitment on a splice's funding
// output keeps its splice through the disk queue and committed updates, and
// that one without a splice is written as before, where a version without
// splices reads both.
func TestBackupIDSplice(t *testing.T) {
	t.Parallel()

	plain := BackupID{ChanID: lnwire.ChannelID{1}, CommitHeight: 7}
	spliced := plain
	spliced.SpliceTxid = chainhash.Hash{9}
	require.False(t, plain.IsSplice())
	require.True(t, spliced.IsSplice())

	for _, id := range []BackupID{plain, spliced} {
		var b bytes.Buffer
		require.NoError(t, id.Encode(&b))
		if !id.IsSplice() {
			require.Equal(t, 32+8, b.Len())
		}

		var got BackupID
		require.NoError(t, got.Decode(bytes.NewReader(b.Bytes())))
		require.Equal(t, id, got)

		// A version without splices reads the fixed fields.
		var old BackupID
		require.NoError(t, old.decodeFixed(bytes.NewReader(b.Bytes())))
		require.Equal(t, plain, old)

		update := CommittedUpdateBody{
			BackupID:      id,
			Hint:          blob.BreachHint{3},
			EncryptedBlob: []byte{4, 5, 6},
		}
		b.Reset()
		require.NoError(t, update.Encode(&b))

		var gotUpdate CommittedUpdateBody
		require.NoError(t, gotUpdate.Decode(
			bytes.NewReader(b.Bytes()),
		))
		require.Equal(t, update, gotUpdate)

		// What a version without splices reads of it.
		r := bytes.NewReader(b.Bytes())
		var oldUpdate CommittedUpdateBody
		require.NoError(t, oldUpdate.BackupID.decodeFixed(r))
		require.NoError(t, ReadElements(r,
			&oldUpdate.Hint, &oldUpdate.EncryptedBlob,
		))
		require.Equal(t, plain, oldUpdate.BackupID)
		require.Equal(t, update.Hint, oldUpdate.Hint)
		require.Equal(t, update.EncryptedBlob, oldUpdate.EncryptedBlob)
	}

	// A cut splice txid is refused.
	var b bytes.Buffer
	require.NoError(t, spliced.Encode(&b))
	var got BackupID
	require.Error(t, got.Decode(bytes.NewReader(b.Bytes()[:b.Len()-1])))
}
