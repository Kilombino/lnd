package chanbackup

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lnwire"
)

// A splice moves a channel to a new funding output, with new 2-of-2 keys and
// a new capacity, while the channel keeps the name it was opened with. A
// backup of a spliced channel describes the channel as it was opened, so that
// every field of a Single means what it always has (the shachain root, for
// one, is derived from the original funding key), and carries where the
// channel is now, and where a pending splice may move it, in a trailer.
//
// The trailer follows the last Single of a Multi, or the Single of a packed
// single backup. A version that doesn't know it stops reading before it: a
// Multi is read by count, a Single by its fields. So an older version restores
// every channel of the file, a spliced one as it was opened, and the spend of
// its original funding output is then something it doesn't take for a close.

// spliceTrailerMagic starts the trailer.
var spliceTrailerMagic = [4]byte{'L', 'F', 'S', 'P'}

// spliceTrailerVersion is the trailer's format.
const spliceTrailerVersion = 0

// SpliceFunding is a funding output a splice moved the channel to, or may.
type SpliceFunding struct {
	// OutPoint is the funding output.
	OutPoint wire.OutPoint

	// ShortChannelID is its short channel id, zero while unconfirmed.
	ShortChannelID lnwire.ShortChannelID

	// Capacity is its value.
	Capacity btcutil.Amount

	// LocalKey is our key in its 2-of-2 script. Only the locator is
	// written: the key is derived again on restore.
	LocalKey keychain.KeyDescriptor

	// RemoteKey is the peer's key in it.
	RemoteKey *btcec.PublicKey
}

// SpliceBackup is what a backup carries of a channel's splices.
type SpliceBackup struct {
	// Current is the funding output the channel is on, when a splice has
	// moved it.
	Current *SpliceFunding

	// Candidates are the funding outputs of the splices pending on the
	// channel.
	Candidates []SpliceFunding
}

// spliceBackupFor reads a channel's splices for its backup. It returns nil for
// a channel without any, and the funding the channel was opened on when a
// splice has moved it.
func spliceBackupFor(channel *channeldb.OpenChannel) (*SpliceBackup,
	*channeldb.PreviousFunding, error) {

	var (
		backup   SpliceBackup
		original *channeldb.PreviousFunding
	)

	if info := channel.SpliceFundingInfo(); info.IsSome() {
		f := info.UnsafeFromSome()
		previous, err := channel.PreviousFundings()
		if err != nil {
			return nil, nil, err
		}
		for i := range previous {
			if previous[i].OutPoint == channel.FundingOutpoint {
				original = &previous[i]
				break
			}
		}
		if original == nil {
			return nil, nil, fmt.Errorf("channel %v moved by a "+
				"splice without its original funding",
				channel.FundingOutpoint)
		}

		backup.Current = &SpliceFunding{
			OutPoint:       f.OutPoint,
			ShortChannelID: f.Scid,
			Capacity:       channel.Capacity,
			LocalKey:       channel.LocalChanCfg.MultiSigKey,
			RemoteKey:      channel.RemoteChanCfg.MultiSigKey.PubKey,
		}
	}

	records, err := channel.SpliceRecords()
	if err != nil {
		return nil, nil, err
	}
	for _, r := range records {
		if r.Retired {
			continue
		}
		backup.Candidates = append(backup.Candidates, SpliceFunding{
			OutPoint:  r.OutPoint,
			Capacity:  r.Capacity,
			LocalKey:  r.LocalKey,
			RemoteKey: r.RemoteKey,
		})
	}

	if backup.Current == nil && len(backup.Candidates) == 0 {
		return nil, nil, nil
	}

	return &backup, original, nil
}

// writeSpliceFunding writes a splice funding.
func writeSpliceFunding(w *bytes.Buffer, f *SpliceFunding) error {
	if f.RemoteKey == nil {
		return errors.New("splice funding without the peer's key")
	}

	return lnwire.WriteElements(w,
		f.OutPoint, f.ShortChannelID, f.Capacity,
		uint32(f.LocalKey.Family), f.LocalKey.Index, f.RemoteKey,
	)
}

// readSpliceFunding reads a splice funding.
func readSpliceFunding(r io.Reader) (SpliceFunding, error) {
	var (
		f      SpliceFunding
		family uint32
	)
	err := lnwire.ReadElements(r,
		&f.OutPoint, &f.ShortChannelID, &f.Capacity,
		&family, &f.LocalKey.Index, &f.RemoteKey,
	)
	f.LocalKey.Family = keychain.KeyFamily(family)

	return f, err
}

// encode writes the backup as a trailer entry's body.
func (b *SpliceBackup) encode(w *bytes.Buffer) error {
	hasCurrent := b.Current != nil
	if err := lnwire.WriteElements(w, hasCurrent); err != nil {
		return err
	}
	if hasCurrent {
		if err := writeSpliceFunding(w, b.Current); err != nil {
			return err
		}
	}

	err := lnwire.WriteElements(w, uint16(len(b.Candidates)))
	if err != nil {
		return err
	}
	for i := range b.Candidates {
		if err := writeSpliceFunding(w, &b.Candidates[i]); err != nil {
			return err
		}
	}

	return nil
}

// decodeSpliceBackup reads a trailer entry's body. Bytes after what this
// version knows are left for a later one.
func decodeSpliceBackup(body []byte) (*SpliceBackup, error) {
	r := bytes.NewReader(body)

	var (
		b          SpliceBackup
		hasCurrent bool
	)
	if err := lnwire.ReadElements(r, &hasCurrent); err != nil {
		return nil, err
	}
	if hasCurrent {
		f, err := readSpliceFunding(r)
		if err != nil {
			return nil, err
		}
		b.Current = &f
	}

	var n uint16
	if err := lnwire.ReadElements(r, &n); err != nil {
		return nil, err
	}
	for range n {
		f, err := readSpliceFunding(r)
		if err != nil {
			return nil, err
		}
		b.Candidates = append(b.Candidates, f)
	}

	return &b, nil
}

// writeSpliceTrailer writes the trailer for the singles that carry splices,
// nothing if none does.
func writeSpliceTrailer(w *bytes.Buffer, singles []Single) error {
	var entries []*Single
	for i := range singles {
		if singles[i].Splice != nil {
			entries = append(entries, &singles[i])
		}
	}
	if len(entries) == 0 {
		return nil
	}

	err := lnwire.WriteElements(w,
		spliceTrailerMagic[:], uint8(spliceTrailerVersion),
		uint32(len(entries)),
	)
	if err != nil {
		return err
	}
	for _, s := range entries {
		var body bytes.Buffer
		if err := s.Splice.encode(&body); err != nil {
			return fmt.Errorf("splice backup of %v: %w",
				s.FundingOutpoint, err)
		}
		if body.Len() > 0xffff {
			return fmt.Errorf("splice backup of %v too large",
				s.FundingOutpoint)
		}
		err := lnwire.WriteElements(w,
			s.FundingOutpoint, uint16(body.Len()), body.Bytes(),
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// readSpliceTrailer reads the trailer, if r holds one, onto the singles it
// names. A trailer of a later format is skipped.
func readSpliceTrailer(r *bytes.Reader, singles []Single) error {
	if r.Len() == 0 {
		return nil
	}

	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return fmt.Errorf("backup trailer: %w", err)
	}
	if magic != spliceTrailerMagic {
		log.Warnf("Skipping unknown data after the channel backups")

		return nil
	}

	var (
		version uint8
		count   uint32
	)
	if err := lnwire.ReadElements(r, &version); err != nil {
		return err
	}
	if version != spliceTrailerVersion {
		log.Warnf("Skipping splice data of an unknown format %v in "+
			"the channel backup", version)

		return nil
	}
	if err := lnwire.ReadElements(r, &count); err != nil {
		return err
	}

	byOutpoint := make(map[wire.OutPoint]*Single, len(singles))
	for i := range singles {
		byOutpoint[singles[i].FundingOutpoint] = &singles[i]
	}
	for range count {
		var (
			op     wire.OutPoint
			length uint16
		)
		if err := lnwire.ReadElements(r, &op, &length); err != nil {
			return err
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(r, body); err != nil {
			return err
		}

		s, ok := byOutpoint[op]
		if !ok {
			return fmt.Errorf("splice data for %v, which has no "+
				"backup", op)
		}
		b, err := decodeSpliceBackup(body)
		if err != nil {
			return fmt.Errorf("splice data for %v: %w", op, err)
		}
		s.Splice = b
	}

	return nil
}
