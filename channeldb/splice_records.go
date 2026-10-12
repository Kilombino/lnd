package channeldb

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	graphdb "github.com/lightningnetwork/lnd/graph/db"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/tlv"
)

// A pending splice's state lives in the channel's own bucket: a record per
// splice transaction, and the revocation log of the peer's commitments on its
// funding output. Both are written in the same transaction as the commitment
// chain step they belong to, through the SpliceCommitWrite arguments of
// AppendRemoteCommitChain, UpdateCommitment and AdvanceCommitChainTail.

var (
	// spliceRecordsBucket holds a channel's pending splices.
	//
	// splice txid -> SpliceRecord
	spliceRecordsBucket = []byte("splice-records")

	// spliceRevocationLogBucket holds the revocation log of the peer's
	// commitments on each pending splice's funding output, with the same
	// entries as the channel's own revocation log.
	//
	// splice txid -> commitment height -> RevocationLog
	spliceRevocationLogBucket = []byte("splice-revocation-log")

	// ErrSpliceRecordNotFound is returned for an unknown splice.
	ErrSpliceRecordNotFound = errors.New("splice record not found")
)

// SpliceRecord is a pending splice of a channel: its funding output and the
// commitments on it.
type SpliceRecord struct {
	// OutPoint is the splice's funding output.
	OutPoint wire.OutPoint

	// Capacity is its value.
	Capacity btcutil.Amount

	// LocalKey is our key in its 2-of-2 script.
	LocalKey keychain.KeyDescriptor

	// RemoteKey is the peer's key in it.
	RemoteKey *btcec.PublicKey

	// LocalDelta and RemoteDelta are the changes to each side's balance.
	LocalDelta, RemoteDelta btcutil.Amount

	// LocalCommit is our latest commitment on this funding with the
	// peer's signatures: what is published if the splice confirms and
	// the channel is closed from it.
	LocalCommit *ChannelCommitment

	// RemoteTail is the peer's current commitment on this funding, as we
	// signed it.
	RemoteTail *ChannelCommitment

	// RemoteTip is the peer's next commitment on this funding, signed
	// but not yet acknowledged, and RemoteTipSig our signatures for it,
	// sent again if the batch is retransmitted.
	RemoteTip    *ChannelCommitment
	RemoteTipSig *lnwire.CommitSig

	// SpliceSig is our commitment_signed for the splice itself, sent
	// again if the peer asks for it on reconnecting.
	SpliceSig *lnwire.CommitSig
}

// Txid returns the splice transaction's id.
func (r *SpliceRecord) Txid() chainhash.Hash {
	return r.OutPoint.Hash
}

// SpliceCommitWrite is what a commitment chain step writes for one pending
// splice, in the same transaction as the step.
type SpliceCommitWrite struct {
	// Txid names the splice.
	Txid chainhash.Hash

	// LocalCommit, with UpdateCommitment, is our new commitment on the
	// splice's funding with the peer's signatures.
	LocalCommit *ChannelCommitment

	// RemoteTip and RemoteTipSig, with AppendRemoteCommitChain, are the
	// peer's new commitment on the splice's funding and our signatures.
	RemoteTip    *ChannelCommitment
	RemoteTipSig *lnwire.CommitSig

	// OurOutputIndex and TheirOutputIndex, with AdvanceCommitChainTail,
	// are the output indexes of the peer's commitment on this funding
	// that is being revoked, for its revocation log entry.
	OurOutputIndex, TheirOutputIndex uint32
}

// SpliceRecord TLV types.
const (
	spliceOutPointType     tlv.Type = 0
	spliceCapacityType     tlv.Type = 1
	spliceLocalKeyType     tlv.Type = 2
	spliceRemoteKeyType    tlv.Type = 3
	spliceLocalDeltaType   tlv.Type = 4
	spliceRemoteDeltaType  tlv.Type = 5
	spliceLocalCommitType  tlv.Type = 6
	spliceRemoteTailType   tlv.Type = 7
	spliceRemoteTipType    tlv.Type = 8
	spliceRemoteTipSigType tlv.Type = 9
	spliceSpliceSigType    tlv.Type = 10
)

// encodeSpliceRecord serializes a record as a TLV stream.
func encodeSpliceRecord(w io.Writer, r *SpliceRecord) error {
	if r.RemoteKey == nil || r.LocalKey.PubKey == nil {
		return errors.New("splice record without both keys")
	}

	var op bytes.Buffer
	if err := graphdb.WriteOutpoint(&op, &r.OutPoint); err != nil {
		return err
	}
	var localKey bytes.Buffer
	if err := WriteElements(&localKey,
		uint32(r.LocalKey.Family), r.LocalKey.Index,
		r.LocalKey.PubKey,
	); err != nil {
		return err
	}

	opBytes := op.Bytes()
	capacity := uint64(r.Capacity)
	localKeyBytes := localKey.Bytes()
	remoteKey := r.RemoteKey.SerializeCompressed()
	localDelta := uint64(r.LocalDelta)
	remoteDelta := uint64(r.RemoteDelta)

	records := []tlv.Record{
		tlv.MakePrimitiveRecord(spliceOutPointType, &opBytes),
		tlv.MakePrimitiveRecord(spliceCapacityType, &capacity),
		tlv.MakePrimitiveRecord(spliceLocalKeyType, &localKeyBytes),
		tlv.MakePrimitiveRecord(spliceRemoteKeyType, &remoteKey),
		tlv.MakePrimitiveRecord(spliceLocalDeltaType, &localDelta),
		tlv.MakePrimitiveRecord(spliceRemoteDeltaType, &remoteDelta),
	}

	commit := func(t tlv.Type, c *ChannelCommitment) error {
		if c == nil {
			return nil
		}
		var b bytes.Buffer
		if err := serializeChanCommit(&b, c); err != nil {
			return err
		}
		auxData := c.extractTlvData()
		if err := auxData.encode(&b); err != nil {
			return err
		}
		blob := b.Bytes()
		records = append(records, tlv.MakePrimitiveRecord(t, &blob))

		return nil
	}
	sig := func(t tlv.Type, s *lnwire.CommitSig) error {
		if s == nil {
			return nil
		}
		var b bytes.Buffer
		if err := s.Encode(&b, 0); err != nil {
			return err
		}
		blob := b.Bytes()
		records = append(records, tlv.MakePrimitiveRecord(t, &blob))

		return nil
	}
	if err := commit(spliceLocalCommitType, r.LocalCommit); err != nil {
		return err
	}
	if err := commit(spliceRemoteTailType, r.RemoteTail); err != nil {
		return err
	}
	if err := commit(spliceRemoteTipType, r.RemoteTip); err != nil {
		return err
	}
	if err := sig(spliceRemoteTipSigType, r.RemoteTipSig); err != nil {
		return err
	}
	if err := sig(spliceSpliceSigType, r.SpliceSig); err != nil {
		return err
	}

	stream, err := tlv.NewStream(records...)
	if err != nil {
		return err
	}

	return stream.Encode(w)
}

// decodeSpliceRecord reads a record.
func decodeSpliceRecord(rd io.Reader) (*SpliceRecord, error) {
	var (
		opBytes, localKeyBytes, remoteKey  []byte
		capacity, localDelta, remoteDelta  uint64
		localCommit, remoteTail, remoteTip []byte
		remoteTipSig, spliceSig            []byte
		r                                  SpliceRecord
	)
	stream, err := tlv.NewStream(
		tlv.MakePrimitiveRecord(spliceOutPointType, &opBytes),
		tlv.MakePrimitiveRecord(spliceCapacityType, &capacity),
		tlv.MakePrimitiveRecord(spliceLocalKeyType, &localKeyBytes),
		tlv.MakePrimitiveRecord(spliceRemoteKeyType, &remoteKey),
		tlv.MakePrimitiveRecord(spliceLocalDeltaType, &localDelta),
		tlv.MakePrimitiveRecord(spliceRemoteDeltaType, &remoteDelta),
		tlv.MakePrimitiveRecord(spliceLocalCommitType, &localCommit),
		tlv.MakePrimitiveRecord(spliceRemoteTailType, &remoteTail),
		tlv.MakePrimitiveRecord(spliceRemoteTipType, &remoteTip),
		tlv.MakePrimitiveRecord(spliceRemoteTipSigType, &remoteTipSig),
		tlv.MakePrimitiveRecord(spliceSpliceSigType, &spliceSig),
	)
	if err != nil {
		return nil, err
	}
	parsed, err := stream.DecodeWithParsedTypes(rd)
	if err != nil {
		return nil, err
	}
	for _, t := range []tlv.Type{
		spliceOutPointType, spliceCapacityType, spliceLocalKeyType,
		spliceRemoteKeyType, spliceLocalDeltaType,
		spliceRemoteDeltaType,
	} {
		if _, ok := parsed[t]; !ok {
			return nil, fmt.Errorf("splice record without type %d",
				t)
		}
	}

	err = graphdb.ReadOutpoint(bytes.NewReader(opBytes), &r.OutPoint)
	if err != nil {
		return nil, err
	}
	r.Capacity = btcutil.Amount(capacity)
	var family uint32
	err = ReadElements(bytes.NewReader(localKeyBytes),
		&family, &r.LocalKey.Index, &r.LocalKey.PubKey,
	)
	if err != nil {
		return nil, err
	}
	r.LocalKey.Family = keychain.KeyFamily(family)
	r.RemoteKey, err = btcec.ParsePubKey(remoteKey)
	if err != nil {
		return nil, err
	}
	r.LocalDelta = btcutil.Amount(int64(localDelta))
	r.RemoteDelta = btcutil.Amount(int64(remoteDelta))

	commit := func(b []byte) (*ChannelCommitment, error) {
		if b == nil {
			return nil, nil
		}
		rd := bytes.NewReader(b)
		c, err := deserializeChanCommit(rd)
		if err != nil {
			return nil, err
		}
		var auxData commitTlvData
		if err := auxData.decode(rd); err != nil {
			return nil, err
		}
		c.amendTlvData(auxData)

		return &c, nil
	}
	sig := func(b []byte) (*lnwire.CommitSig, error) {
		if b == nil {
			return nil, nil
		}
		var s lnwire.CommitSig
		if err := s.Decode(bytes.NewReader(b), 0); err != nil {
			return nil, err
		}

		return &s, nil
	}
	if r.LocalCommit, err = commit(localCommit); err != nil {
		return nil, err
	}
	if r.RemoteTail, err = commit(remoteTail); err != nil {
		return nil, err
	}
	if r.RemoteTip, err = commit(remoteTip); err != nil {
		return nil, err
	}
	if r.RemoteTipSig, err = sig(remoteTipSig); err != nil {
		return nil, err
	}
	if r.SpliceSig, err = sig(spliceSig); err != nil {
		return nil, err
	}

	return &r, nil
}

// putSpliceRecord writes a record into the channel's bucket.
func putSpliceRecord(chanBucket kvdb.RwBucket, r *SpliceRecord) error {
	bucket, err := chanBucket.CreateBucketIfNotExists(spliceRecordsBucket)
	if err != nil {
		return err
	}
	var b bytes.Buffer
	if err := encodeSpliceRecord(&b, r); err != nil {
		return err
	}
	txid := r.Txid()

	return bucket.Put(txid[:], b.Bytes())
}

// fetchSpliceRecord reads a record from the channel's bucket.
func fetchSpliceRecord(chanBucket kvdb.RBucket,
	txid chainhash.Hash) (*SpliceRecord, error) {

	bucket := chanBucket.NestedReadBucket(spliceRecordsBucket)
	if bucket == nil {
		return nil, ErrSpliceRecordNotFound
	}
	v := bucket.Get(txid[:])
	if v == nil {
		return nil, ErrSpliceRecordNotFound
	}

	return decodeSpliceRecord(bytes.NewReader(v))
}

// PutSpliceRecord stores a pending splice of the channel, replacing any
// record of the same transaction.
func (c *OpenChannel) PutSpliceRecord(r *SpliceRecord) error {
	c.Lock()
	defer c.Unlock()

	return kvdb.Update(c.Db.backend, func(tx kvdb.RwTx) error {
		chanBucket, err := fetchChanBucketRw(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}

		return putSpliceRecord(chanBucket, r)
	}, func() {})
}

// SpliceRecords returns the channel's pending splices.
func (c *OpenChannel) SpliceRecords() ([]*SpliceRecord, error) {
	c.RLock()
	defer c.RUnlock()

	// A channel held only in memory has none.
	if c.Db == nil {
		return nil, nil
	}

	var records []*SpliceRecord
	err := kvdb.View(c.Db.backend, func(tx kvdb.RTx) error {
		chanBucket, err := fetchChanBucket(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		switch {
		// A channel not yet saved (during funding) has none.
		case errors.Is(err, ErrNoChanDBExists),
			errors.Is(err, ErrNoActiveChannels),
			errors.Is(err, ErrChannelNotFound):

			return nil

		case err != nil:
			return err
		}
		bucket := chanBucket.NestedReadBucket(spliceRecordsBucket)
		if bucket == nil {
			return nil
		}

		return bucket.ForEach(func(_, v []byte) error {
			r, err := decodeSpliceRecord(bytes.NewReader(v))
			if err != nil {
				return err
			}
			records = append(records, r)

			return nil
		})
	}, func() {
		records = nil
	})

	return records, err
}

// DeleteSpliceRecord removes a pending splice that will never lock. Its
// revocation log stays: a revoked commitment on its funding can't confirm
// once the splice is dead, but the entries cost little and the death may be
// reorged out.
func (c *OpenChannel) DeleteSpliceRecord(txid chainhash.Hash) error {
	c.Lock()
	defer c.Unlock()

	return kvdb.Update(c.Db.backend, func(tx kvdb.RwTx) error {
		chanBucket, err := fetchChanBucketRw(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}
		bucket := chanBucket.NestedReadWriteBucket(spliceRecordsBucket)
		if bucket == nil || bucket.Get(txid[:]) == nil {
			return ErrSpliceRecordNotFound
		}

		return bucket.Delete(txid[:])
	}, func() {})
}

// FetchSpliceRevocationLog returns the revocation log entry of the peer's
// commitment at height on a splice's funding output.
func (c *OpenChannel) FetchSpliceRevocationLog(txid chainhash.Hash,
	height uint64) (*RevocationLog, error) {

	c.RLock()
	defer c.RUnlock()

	var rl RevocationLog
	err := kvdb.View(c.Db.backend, func(tx kvdb.RTx) error {
		chanBucket, err := fetchChanBucket(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}
		logs := chanBucket.NestedReadBucket(spliceRevocationLogBucket)
		if logs == nil {
			return ErrLogEntryNotFound
		}
		log := logs.NestedReadBucket(txid[:])
		if log == nil {
			return ErrLogEntryNotFound
		}
		rl, err = fetchRevocationLog(log, height)

		return err
	}, func() {
		rl = RevocationLog{}
	})
	if err != nil {
		return nil, err
	}

	return &rl, nil
}

// applySpliceWrites applies a commitment chain step's writes for the
// pending splices: our new commitments, the peer's new ones with our
// signatures, or, with revoke, the peer's revoked ones moved into their
// revocation logs.
func applySpliceWrites(chanBucket kvdb.RwBucket, writes []SpliceCommitWrite,
	revoke bool, noAmtData bool) error {

	for _, w := range writes {
		r, err := fetchSpliceRecord(chanBucket, w.Txid)
		if err != nil {
			return fmt.Errorf("splice %v: %w", w.Txid, err)
		}

		if w.LocalCommit != nil {
			r.LocalCommit = w.LocalCommit
		}
		if w.RemoteTip != nil {
			r.RemoteTip = w.RemoteTip
			r.RemoteTipSig = w.RemoteTipSig
		}

		if revoke {
			if r.RemoteTail == nil || r.RemoteTip == nil {
				return fmt.Errorf("splice %v: nothing to "+
					"revoke", w.Txid)
			}
			logs, err := chanBucket.CreateBucketIfNotExists(
				spliceRevocationLogBucket,
			)
			if err != nil {
				return err
			}
			log, err := logs.CreateBucketIfNotExists(w.Txid[:])
			if err != nil {
				return err
			}
			err = putRevocationLog(
				log, r.RemoteTail, w.OurOutputIndex,
				w.TheirOutputIndex, noAmtData,
			)
			if err != nil {
				return err
			}
			r.RemoteTail, r.RemoteTip = r.RemoteTip, nil
			r.RemoteTipSig = nil
		}

		if err := putSpliceRecord(chanBucket, r); err != nil {
			return err
		}
	}

	return nil
}

// UpdateSpliceRecord changes a pending splice's record in one transaction.
func (c *OpenChannel) UpdateSpliceRecord(txid chainhash.Hash,
	update func(r *SpliceRecord) error) error {

	c.Lock()
	defer c.Unlock()

	return kvdb.Update(c.Db.backend, func(tx kvdb.RwTx) error {
		chanBucket, err := fetchChanBucketRw(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}
		r, err := fetchSpliceRecord(chanBucket, txid)
		if err != nil {
			return err
		}
		if err := update(r); err != nil {
			return err
		}
		if r.Txid() != txid {
			return errors.New("splice record's outpoint changed")
		}

		return putSpliceRecord(chanBucket, r)
	}, func() {})
}

// EnsureSpliceRecord stores a pending splice's funding unless a record of it
// exists, which must then name the same funding.
func (c *OpenChannel) EnsureSpliceRecord(r *SpliceRecord) error {
	c.Lock()
	defer c.Unlock()

	return kvdb.Update(c.Db.backend, func(tx kvdb.RwTx) error {
		chanBucket, err := fetchChanBucketRw(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}

		have, err := fetchSpliceRecord(chanBucket, r.Txid())
		switch {
		case errors.Is(err, ErrSpliceRecordNotFound):
			return putSpliceRecord(chanBucket, r)

		case err != nil:
			return err
		}

		same := have.OutPoint == r.OutPoint &&
			have.Capacity == r.Capacity &&
			have.LocalDelta == r.LocalDelta &&
			have.RemoteDelta == r.RemoteDelta &&
			have.LocalKey.KeyLocator == r.LocalKey.KeyLocator &&
			have.LocalKey.PubKey.IsEqual(r.LocalKey.PubKey) &&
			have.RemoteKey.IsEqual(r.RemoteKey)
		if !same {
			return fmt.Errorf("splice %v recorded with another "+
				"funding", r.Txid())
		}

		return nil
	}, func() {})
}
