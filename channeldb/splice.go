package channeldb

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/fn/v2"
	graphdb "github.com/lightningnetwork/lnd/graph/db"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/kvdb"
)

var (
	// spliceFundingIndexBucket maps every funding outpoint a splice moved
	// a channel to onto the outpoint that names the channel, so that a
	// lookup by the output on chain (the graph's, a peer's) finds it.
	// Entries are never removed: a closed channel resolves to its name and
	// is then not found, as for any closed channel.
	//
	// funding outpoint -> original funding outpoint
	spliceFundingIndexBucket = []byte("splice-funding-index")

	// ErrSpliceFundingInUse is returned when a splice would move a
	// channel to a funding outpoint that already names or funded a
	// channel.
	ErrSpliceFundingInUse = errors.New("funding outpoint already in use")
)

// SpliceLock is what changes on a channel when a splice locks: the funding
// output, and with it the capacity and both 2-of-2 keys, since each splice
// uses a new funding key.
type SpliceLock struct {
	// Funding is the new funding output and its short channel id.
	Funding SpliceFunding

	// Capacity is the value of the new funding output.
	Capacity btcutil.Amount

	// LocalMultiSigKey is our key in the new funding output's script.
	LocalMultiSigKey keychain.KeyDescriptor

	// RemoteMultiSigKey is the peer's key in the new funding output's
	// script.
	RemoteMultiSigKey keychain.KeyDescriptor

	// LocalCommit and RemoteCommit, when set, replace the channel's
	// current commitments with the same ones on the new funding. Without
	// them only the funding moves (a channel being built in a test, or
	// restored).
	LocalCommit, RemoteCommit *ChannelCommitment

	// RemoteTip, when the peer has a commitment we signed and it hasn't
	// acknowledged, is that commitment on the new funding. It replaces
	// the pending one, with our signatures for it from the splice's
	// record, so a retransmission names the new funding.
	RemoteTip *ChannelCommitment
}

// LockSplice moves the channel to the funding output of a splice that has
// locked, in one transaction with the index that finds the channel by it.
// FundingOutpoint and ShortChannelID keep naming the channel.
func (c *OpenChannel) LockSplice(l SpliceLock) error {
	c.Lock()
	defer c.Unlock()

	if l.Funding.OutPoint == c.FundingOutpoint {
		return ErrSpliceFundingInUse
	}
	if l.LocalMultiSigKey.PubKey == nil ||
		l.RemoteMultiSigKey.PubKey == nil {

		return errors.New("splice lock without both funding keys")
	}

	var name bytes.Buffer
	if err := graphdb.WriteOutpoint(&name, &c.FundingOutpoint); err != nil {
		return err
	}
	var current bytes.Buffer
	err := graphdb.WriteOutpoint(&current, &l.Funding.OutPoint)
	if err != nil {
		return err
	}

	err = kvdb.Update(c.Db.backend, func(tx kvdb.RwTx) error {
		// The new outpoint must not name a channel, nor already be
		// another channel's funding.
		opBucket := tx.ReadWriteBucket(outpointBucket)
		if opBucket != nil && opBucket.Get(current.Bytes()) != nil {
			return ErrSpliceFundingInUse
		}
		index, err := tx.CreateTopLevelBucket(spliceFundingIndexBucket)
		if err != nil {
			return err
		}
		prev := index.Get(current.Bytes())
		if prev != nil && !bytes.Equal(prev, name.Bytes()) {
			return ErrSpliceFundingInUse
		}

		chanBucket, err := fetchChanBucketRw(
			tx, c.IdentityPub, &c.FundingOutpoint, c.ChainHash,
		)
		if err != nil {
			return err
		}
		channel, err := fetchOpenChannel(
			chanBucket, &c.FundingOutpoint,
		)
		if err != nil {
			return err
		}
		// Already moved: a retry after a crash.
		if channel.CurrentFundingOutpoint() == l.Funding.OutPoint {
			return nil
		}

		previous := PreviousFunding{
			OutPoint:  channel.CurrentFundingOutpoint(),
			Capacity:  channel.Capacity,
			LocalKey:  channel.LocalChanCfg.MultiSigKey,
			RemoteKey: channel.RemoteChanCfg.MultiSigKey.PubKey,
			LockedTo:  l.Funding.OutPoint,
		}
		if l.LocalCommit != nil {
			err := swapSpliceCommitments(chanBucket, channel, l)
			if err != nil {
				return err
			}
		}
		applySpliceLock(channel, l)
		if err := putOpenChannel(chanBucket, channel); err != nil {
			return err
		}
		err = putPreviousFunding(chanBucket, &previous)
		if err != nil {
			return err
		}

		return index.Put(current.Bytes(), name.Bytes())
	}, func() {})
	if err != nil {
		return err
	}

	applySpliceLock(c, l)

	return nil
}

// applySpliceLock sets a splice lock's fields on a channel.
func applySpliceLock(c *OpenChannel, l SpliceLock) {
	c.spliceFunding = fn.Some(l.Funding)
	c.Capacity = l.Capacity
	c.LocalChanCfg.MultiSigKey = l.LocalMultiSigKey
	c.RemoteChanCfg.MultiSigKey = l.RemoteMultiSigKey
	if l.LocalCommit != nil {
		c.LocalCommitment = *l.LocalCommit
	}
	if l.RemoteCommit != nil {
		c.RemoteCommitment = *l.RemoteCommit
	}
}

// swapSpliceCommitments checks a lock's commitments against the channel's and
// moves the pending commitment diff, if any, to the new funding. The splice's
// record and every other pending splice's go: once one locks, the others
// can't confirm.
func swapSpliceCommitments(chanBucket kvdb.RwBucket, channel *OpenChannel,
	l SpliceLock) error {

	switch {
	case l.RemoteCommit == nil:
		return errors.New("splice lock with only our commitment")

	case l.LocalCommit.CommitHeight != channel.LocalCommitment.CommitHeight,
		l.RemoteCommit.CommitHeight !=
			channel.RemoteCommitment.CommitHeight:

		return errors.New("splice lock commitments at other heights")
	}

	txid := l.Funding.OutPoint.Hash
	record, err := fetchSpliceRecord(chanBucket, txid)
	if err != nil {
		return err
	}

	diffBytes := chanBucket.Get(commitDiffKey)
	switch {
	case diffBytes == nil && l.RemoteTip != nil:
		return errors.New("splice lock with a pending commitment the " +
			"channel doesn't have")

	case diffBytes != nil && l.RemoteTip == nil:
		return errors.New("splice lock without the pending commitment")

	case diffBytes != nil:
		diff, err := deserializeCommitDiff(bytes.NewReader(diffBytes))
		if err != nil {
			return err
		}
		if record.RemoteTipSig == nil ||
			diff.Commitment.CommitHeight != l.RemoteTip.CommitHeight {

			return errors.New("splice lock pending commitment " +
				"doesn't match")
		}
		diff.Commitment = *l.RemoteTip
		diff.CommitSig = record.RemoteTipSig

		var b bytes.Buffer
		if err := serializeCommitDiff(&b, diff); err != nil {
			return err
		}
		if err := chanBucket.Put(commitDiffKey, b.Bytes()); err != nil {
			return err
		}
	}

	return chanBucket.DeleteNestedBucket(spliceRecordsBucket)
}

// splicePreviousFundingsBucket keeps the funding outputs a channel moved
// away from, with their keys: until a splice is buried deep enough, a reorg
// can bring the previous funding back, and a revoked commitment on it must
// still be punished.
//
// previous funding outpoint -> PreviousFunding
var splicePreviousFundingsBucket = []byte("splice-previous-fundings")

// PreviousFunding is a funding output a channel moved away from by a splice.
type PreviousFunding struct {
	// OutPoint is the previous funding output.
	OutPoint wire.OutPoint

	// Capacity is its value.
	Capacity btcutil.Amount

	// LocalKey and RemoteKey are the 2-of-2 keys of its script.
	LocalKey  keychain.KeyDescriptor
	RemoteKey *btcec.PublicKey

	// LockedTo is the funding output the splice that spent it moved the
	// channel to.
	LockedTo wire.OutPoint
}

// putPreviousFunding records a funding output the channel moved away from.
func putPreviousFunding(chanBucket kvdb.RwBucket, f *PreviousFunding) error {
	bucket, err := chanBucket.CreateBucketIfNotExists(
		splicePreviousFundingsBucket,
	)
	if err != nil {
		return err
	}

	var key, value bytes.Buffer
	if err := graphdb.WriteOutpoint(&key, &f.OutPoint); err != nil {
		return err
	}
	if err := graphdb.WriteOutpoint(&value, &f.LockedTo); err != nil {
		return err
	}
	err = WriteElements(&value, uint64(f.Capacity),
		uint32(f.LocalKey.Family), f.LocalKey.Index, f.LocalKey.PubKey,
		f.RemoteKey,
	)
	if err != nil {
		return err
	}

	return bucket.Put(key.Bytes(), value.Bytes())
}

// PreviousFundings returns the funding outputs the channel moved away from.
func (c *OpenChannel) PreviousFundings() ([]PreviousFunding, error) {
	c.RLock()
	defer c.RUnlock()

	if c.Db == nil {
		return nil, nil
	}

	var fundings []PreviousFunding
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
		bucket := chanBucket.NestedReadBucket(
			splicePreviousFundingsBucket,
		)
		if bucket == nil {
			return nil
		}

		return bucket.ForEach(func(k, v []byte) error {
			var (
				f        PreviousFunding
				capacity uint64
				family   uint32
			)
			err := graphdb.ReadOutpoint(bytes.NewReader(k), &f.OutPoint)
			if err != nil {
				return err
			}
			r := bytes.NewReader(v)
			if err := graphdb.ReadOutpoint(r, &f.LockedTo); err != nil {
				return err
			}
			err = ReadElements(r, &capacity, &family,
				&f.LocalKey.Index, &f.LocalKey.PubKey, &f.RemoteKey,
			)
			if err != nil {
				return err
			}
			f.Capacity = btcutil.Amount(capacity)
			f.LocalKey.Family = keychain.KeyFamily(family)
			fundings = append(fundings, f)

			return nil
		})
	}, func() {
		fundings = nil
	})

	return fundings, err
}

// channelName returns the outpoint that names the channel funded, now or
// before a splice, by chanPoint: chanPoint itself unless a splice moved a
// channel to it.
func (c *ChannelStateDB) channelName(chanPoint wire.OutPoint) (wire.OutPoint,
	error) {

	var key bytes.Buffer
	if err := graphdb.WriteOutpoint(&key, &chanPoint); err != nil {
		return chanPoint, err
	}

	name := chanPoint
	err := kvdb.View(c.backend, func(tx kvdb.RTx) error {
		index := tx.ReadBucket(spliceFundingIndexBucket)
		if index == nil {
			return nil
		}
		v := index.Get(key.Bytes())
		if v == nil {
			return nil
		}

		return graphdb.ReadOutpoint(bytes.NewReader(v), &name)
	}, func() {
		name = chanPoint
	})
	if err != nil {
		return chanPoint, fmt.Errorf("splice funding index: %w", err)
	}

	return name, nil
}
