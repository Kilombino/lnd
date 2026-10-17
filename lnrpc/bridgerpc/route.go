//go:build bridgerpc
// +build bridgerpc

package bridgerpc

import (
	"fmt"
	"math"
	"strings"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/paulscode/lightning-fork-bridge/node"
)

// routeQuery is the route search a quote and the pay gate ask a paying node
// for: to the invoice's destination, for its amount and final CLTV, through
// its route hints (a route to a private destination exists only through
// them), for its features, within the routing budget and the CLTV limit, with
// mission control's view of which channels have been failing. The same for
// both nodes, so the two directions find routes by one rule.
func routeQuery(dest string, amtMsat int64, finalCLTV int32,
	hints []*lnrpc.RouteHint, features []lnrpc.FeatureBit,
	maxFeeMsat uint64, cltvLimit uint32) *lnrpc.QueryRoutesRequest {

	return &lnrpc.QueryRoutesRequest{
		PubKey:         dest,
		AmtMsat:        amtMsat,
		FinalCltvDelta: finalCLTV,
		FeeLimit: &lnrpc.FeeLimit{
			Limit: &lnrpc.FeeLimit_FixedMsat{
				FixedMsat: int64(maxFeeMsat),
			},
		},
		CltvLimit:         cltvLimit,
		RouteHints:        hints,
		DestFeatures:      features,
		UseMissionControl: true,
	}
}

// routeFrom reads a route search's answer: the route's CLTV counted from the
// paying node's height, as the payment's limit is, and its fee; or
// node.ErrNoRoute when there is none.
func routeFrom(res *lnrpc.QueryRoutesResponse, err error,
	height int32) (node.Route, error) {

	if err != nil {
		// lnd answers a search that found nothing with an error, not
		// an empty list: "unable to find a path to destination".
		// The other ways lnd says the payment cannot go (too little
		// to send, a destination needing a feature it lacks) are as
		// final for this quote.
		msg := strings.ToLower(err.Error())
		for _, no := range []string{"unable to find a path", "no route",
			"insufficient", "not enough", "feature"} {

			if strings.Contains(msg, no) {
				return node.Route{}, fmt.Errorf("%w: %v",
					node.ErrNoRoute, err)
			}
		}

		return node.Route{}, fmt.Errorf("finding a route: %w", err)
	}
	if len(res.GetRoutes()) == 0 {
		return node.Route{}, node.ErrNoRoute
	}

	route := res.GetRoutes()[0]
	lock := int64(route.GetTotalTimeLock()) - int64(height)
	if lock < 0 || lock > math.MaxInt32 {
		return node.Route{}, fmt.Errorf("a route locking until %d at "+
			"height %d", route.GetTotalTimeLock(), height)
	}

	return node.Route{
		CLTV: uint32(lock), FeeMsat: uint64(route.GetTotalFeesMsat()),
	}, nil
}
