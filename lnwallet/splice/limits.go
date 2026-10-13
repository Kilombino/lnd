package splice

import (
	"sync"
	"time"

	"github.com/lightningnetwork/lnd/lnwire"
)

// Refusals of a peer's splice_init by the node-wide limits.
const (
	codeTooManySplices = "too_many_splices"
	codeTooManyAborts  = "too_many_aborts"
)

// Default limits on splices peers start.
const (
	// DefaultMaxPeerSplices is how many splices peers started may be in
	// progress across the node.
	DefaultMaxPeerSplices = 5

	// DefaultMaxAborts is how many splices a peer started on a channel
	// may end without locking within DefaultAbortWindow before the
	// channel refuses more.
	DefaultMaxAborts = 3

	// DefaultAbortWindow is the window aborts are counted in.
	DefaultAbortWindow = 24 * time.Hour
)

// Limits bounds the splices peers start, across the node's channels: one in
// progress per peer and MaxPending in all, and none on a channel where
// MaxAborts ended without locking within AbortWindow. Splices this node
// starts aren't counted, so a peer can never block them. Kept in memory: a
// restart forgets the aborts.
type Limits struct {
	// MaxPending is how many peer-started splices may be in progress.
	MaxPending int

	// MaxAborts and AbortWindow bound the failed ones per channel.
	MaxAborts   int
	AbortWindow time.Duration

	// Now is the clock.
	Now func() time.Time

	mu      sync.Mutex
	pending map[lnwire.ChannelID]place
	aborts  map[lnwire.ChannelID][]time.Time

	// misbehaved are channels whose peer broke a splice after we had
	// signed it: no more splices on them until a restart.
	misbehaved map[lnwire.ChannelID]struct{}
}

// place is a peer-started splice in progress: the peer, and the machine
// holding it (a channel's machine is replaced on reconnecting, and the old
// one may stop after the new one restored the splice).
type place struct {
	peer  [33]byte
	owner *Machine
}

// NewLimits returns the default limits.
func NewLimits() *Limits {
	return &Limits{
		MaxPending:  DefaultMaxPeerSplices,
		MaxAborts:   DefaultMaxAborts,
		AbortWindow: DefaultAbortWindow,
		Now:         time.Now,
	}
}

// admit takes a place for a splice the peer starts on the channel, or
// returns why not. With force, as for a splice restored at start, the place
// is taken whatever the limits.
func (l *Limits) admit(owner *Machine, chanID lnwire.ChannelID,
	peer [33]byte, force bool) string {

	if l == nil {
		return ""
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.pending == nil {
		l.pending = make(map[lnwire.ChannelID]place)
	}

	if !force {
		if l.recentAborts(chanID) >= l.MaxAborts {
			return codeTooManyAborts
		}
		if len(l.pending) >= l.MaxPending {
			return codeTooManySplices
		}
		for _, p := range l.pending {
			if p.peer == peer {
				return codeTooManySplices
			}
		}
	}
	l.pending[chanID] = place{peer: peer, owner: owner}

	return ""
}

// release frees the channel's place a machine holds; a splice that ended
// without locking counts against the channel.
func (l *Limits) release(owner *Machine, chanID lnwire.ChannelID,
	aborted bool) {

	if l == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if p, ok := l.pending[chanID]; ok && p.owner == owner {
		delete(l.pending, chanID)
	}
	if !aborted {
		return
	}
	if l.aborts == nil {
		l.aborts = make(map[lnwire.ChannelID][]time.Time)
	}
	l.aborts[chanID] = append(l.aborts[chanID], l.Now())
}

// recentAborts counts a channel's aborts within the window, dropping older
// ones.
//
// NOTE: must be called with mu held.
func (l *Limits) recentAborts(chanID lnwire.ChannelID) int {
	cutoff := l.Now().Add(-l.AbortWindow)
	var recent []time.Time
	for _, at := range l.aborts[chanID] {
		if at.After(cutoff) {
			recent = append(recent, at)
		}
	}
	if len(recent) == 0 {
		delete(l.aborts, chanID)
	} else {
		l.aborts[chanID] = recent
	}

	return len(recent)
}

// markMisbehaved refuses splices on the channel from now on: its peer
// broke one after we had signed it. A channel's machine is replaced on every
// reconnection, so the mark lives here.
func (l *Limits) markMisbehaved(chanID lnwire.ChannelID) {
	if l == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.misbehaved == nil {
		l.misbehaved = make(map[lnwire.ChannelID]struct{})
	}
	l.misbehaved[chanID] = struct{}{}
}

// isMisbehaved reports whether the channel's peer broke a splice after we
// had signed it.
func (l *Limits) isMisbehaved(chanID lnwire.ChannelID) bool {
	if l == nil {
		return false
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	_, ok := l.misbehaved[chanID]

	return ok
}
