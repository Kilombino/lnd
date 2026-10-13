package interactivetx

import (
	"bytes"
	"testing"

	"github.com/btcsuite/btcd/wire"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// TestCheckTxCountsAcceptsValid checks that the walk before decoding a
// prevtx accepts every transaction btcd serializes, with witnesses or
// without, and refuses it cut short.
func TestCheckTxCountsAcceptsValid(t *testing.T) {
	t.Parallel()

	rapid.Check(t, func(rt *rapid.T) {
		tx := wire.NewMsgTx(2)
		nIn := rapid.IntRange(1, 5).Draw(rt, "inputs")
		witness := rapid.Bool().Draw(rt, "witness")
		for i := 0; i < nIn; i++ {
			in := &wire.TxIn{
				SignatureScript: rapid.SliceOfN(
					rapid.Byte(), 0, 40,
				).Draw(rt, "script"),
				Sequence: rapid.Uint32().Draw(rt, "seq"),
			}
			if witness {
				items := rapid.IntRange(0, 4).Draw(rt, "items")
				for j := 0; j < items; j++ {
					in.Witness = append(in.Witness,
						rapid.SliceOfN(
							rapid.Byte(), 0, 80,
						).Draw(rt, "item"))
				}
			}
			tx.AddTxIn(in)
		}
		nOut := rapid.IntRange(0, 5).Draw(rt, "outputs")
		for i := 0; i < nOut; i++ {
			tx.AddTxOut(&wire.TxOut{
				Value: rapid.Int64().Draw(rt, "value"),
				PkScript: rapid.SliceOfN(
					rapid.Byte(), 0, 40,
				).Draw(rt, "pk"),
			})
		}

		var b bytes.Buffer
		require.NoError(rt, tx.Serialize(&b))
		require.NoError(rt, checkTxCounts(b.Bytes()))

		cut := rapid.IntRange(0, b.Len()-1).Draw(rt, "cut")
		require.Error(rt, checkTxCounts(b.Bytes()[:cut]))
	})
}
