package channeldb

import (
	"bytes"
	"errors"
	"fmt"

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
		applySpliceLock(channel, l)
		if err := putOpenChannel(chanBucket, channel); err != nil {
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
