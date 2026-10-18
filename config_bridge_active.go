//go:build bridgerpc
// +build bridgerpc

package lnd

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/lightningnetwork/lnd/lncfg"
	"github.com/lightningnetwork/lnd/lnrpc/bridgerpc"
)

// validateBridgeConfig fills in the bridge's derived defaults and refuses a
// configuration that would not serve swaps.
//
// It runs at startup rather than at the first quote, because the failure it
// catches is silent: a bridge whose numbers are collectively incoherent accepts
// its configuration, starts, and then refuses every swap for a reason that
// appears nowhere except in the arithmetic. An operator should learn that from
// the daemon refusing to start, with the knob to turn named.
func validateBridgeConfig(cfg *Config, networkDir string) error {
	sub := cfg.SubRPCServers.BridgeRPC
	if sub == nil {
		return nil
	}

	if sub.Journal == "" {
		// Under the network directory so that whatever backs this node
		// up takes the journal with it. It records swaps in flight,
		// including their preimages, and losing it while one is running
		// loses the record of an HTLC that is still out there.
		sub.Journal = filepath.Join(
			networkDir, bridgerpc.DefaultJournalName,
		)
	}
	sub.Journal = CleanAndExpandPath(sub.Journal)

	if sub.Supervised {
		if err := superviseSha256Node(cfg, sub, networkDir); err != nil {
			return err
		}
	}

	// A fixed rate is for networks without a market, such as a regtest
	// lab. On mainnet it is the operator's number standing in for a market
	// that moves several percent in an hour: once it has drifted past the
	// fee, anyone who notices drains the bridge at it, and nothing in the
	// bridge can tell, since the rate it checks against is that same
	// number. So it is refused here, where it can only have been set by
	// hand, rather than allowed to trade.
	if sub.Enabled && sub.RateSource == bridgerpc.RateSourceFixed &&
		cfg.ActiveNetParams.Params.Name == "mainnet" {

		return fmt.Errorf("%w: bridgerpc.ratesource=fixed is for test "+
			"networks without a market; on mainnet the bridge "+
			"trades at the market's rate (leave ratesource out, or "+
			"set it to neoxa)", bridgerpc.ErrConfig)
	}

	if err := bridgeChainBackendCheck(cfg); err != nil {
		return err
	}

	if msg := bridgeInvoiceGCWarning(cfg); msg != "" {
		ltndLog.Warnf("%s", msg)
	}

	if sub.SHA256TLSCertPath != "" {
		sub.SHA256TLSCertPath = CleanAndExpandPath(
			sub.SHA256TLSCertPath,
		)
	}
	if sub.SHA256MacaroonPath != "" {
		sub.SHA256MacaroonPath = CleanAndExpandPath(
			sub.SHA256MacaroonPath,
		)
	}

	// So that Validate checks the margin with the delta it will run with.
	// A config built by hand (the tests') may have no invoices section;
	// the bridge then holds to lnd's default, which it never goes under.
	if cfg.Invoices != nil {
		sub.LocalHoldExpiryDelta = cfg.Invoices.HoldExpiryDelta
	}

	return sub.Validate()
}

// bridgeChainBackendCheck refuses an enabled bridge on a light client that
// picks its own peers.
//
// The bridge holds payers' HTLCs against deadlines counted in this chain's
// blocks, and decides whether to pay out by reading them. A neutrino light
// client checks headers and filters, not transactions or scripts, so it
// believes what its peers serve it about anything else; and peers named with
// neutrino.addpeer, or found on the network, are taken with only a warning.
// With neutrino.connect it talks only to the nodes named there, which an
// operator names because they are their own full node, and the bridge may
// run. A full node backend (bitcoind) is what the bridge is meant for.
func bridgeChainBackendCheck(cfg *Config) error {
	sub := cfg.SubRPCServers.BridgeRPC
	if !sub.Enabled || cfg.Bitcoin == nil ||
		cfg.Bitcoin.Node != neutrinoBackendName {

		return nil
	}
	if cfg.NeutrinoMode != nil && len(cfg.NeutrinoMode.ConnectPeers) > 0 {
		return nil
	}

	return fmt.Errorf("%w: the bridge on a neutrino backend needs "+
		"neutrino.connect set to your own full node: a light client "+
		"checks no transactions or scripts and believes the peers it "+
		"finds or is given with neutrino.addpeer. Use bitcoind, or set "+
		"neutrino.connect to your own node", bridgerpc.ErrConfig)
}

// superviseSha256Node fills in where the supervised SHA256 node and this node's
// files for it are.
//
// The address, certificate and macaroon default to that node's own. The address
// and certificate may be named, for a platform that runs the node elsewhere
// (Umbrel gives it an address of its own); the macaroon may not: it is one this
// node bakes, and a path pointing elsewhere would have the bridge use a
// credential it did not make for a node it did not create.
func superviseSha256Node(cfg *Config, sub *bridgerpc.Config,
	networkDir string) error {

	if sub.SHA256MacaroonPath != "" {
		return fmt.Errorf("%w: bridgerpc.sha256.macaroonpath is for an "+
			"SHA256 node you already run; with "+
			"bridgerpc.sha256.supervised this node makes the "+
			"macaroon itself, so leave it out", bridgerpc.ErrConfig)
	}

	if sub.SHA256Dir == "" {
		sub.SHA256Dir = filepath.Join(
			cfg.LndDir, bridgerpc.DefaultSha256DirName,
		)
	}
	sub.SHA256Dir = CleanAndExpandPath(sub.SHA256Dir)

	if sub.SHA256RPCHost == "" {
		sub.SHA256RPCHost = bridgerpc.DefaultSupervisedRPCHost
	}
	if sub.SHA256TLSCertPath == "" {
		sub.SHA256TLSCertPath = filepath.Join(sub.SHA256Dir, "tls.cert")
	}

	secrets := filepath.Join(networkDir, bridgerpc.Sha256SecretsDir)
	sub.SHA256PasswordFile = filepath.Join(
		secrets, bridgerpc.Sha256PasswordName,
	)
	sub.SHA256MacaroonPath = filepath.Join(
		secrets, bridgerpc.Sha256MacaroonName,
	)
	sub.SHA256OperatorMacaroonPath = filepath.Join(
		secrets, bridgerpc.Sha256OperatorMacaroonName,
	)

	// The stock node keeps its macaroons under its own network directory,
	// named as lnd names networks, which is the same naming this node's
	// directories use.
	sub.SHA256AdminMacaroonPath = filepath.Join(
		sub.SHA256Dir, "data", "chain", "bitcoin",
		lncfg.NormalizeNetwork(cfg.ActiveNetParams.Name),
		"admin.macaroon",
	)

	return nil
}

// bridgeInvoiceGCWarning is what to tell an operator running the bridge with
// lnd's garbage collection of cancelled invoices on, or "" when it is off.
//
// A warning rather than a refusal: the bridge's journal is the record of its
// swaps, and the bridge runs safely without the node's. But a quote also reads
// the node's cancelled hold invoice for a payment hash it has seen before, so
// that one a payer paid into is not quoted again should the journal have lost
// its record, and these options delete that invoice.
func bridgeInvoiceGCWarning(cfg *Config) string {
	sub := cfg.SubRPCServers.BridgeRPC
	if sub == nil || !sub.Enabled {
		return ""
	}

	var on []string
	if cfg.GcCanceledInvoicesOnTheFly {
		on = append(on, "gc-canceled-invoices-on-the-fly")
	}
	if cfg.GcCanceledInvoicesOnStartup {
		on = append(on, "gc-canceled-invoices-on-startup")
	}
	if len(on) == 0 {
		return ""
	}

	return fmt.Sprintf("Bridge is on with %s: this node deletes "+
		"cancelled invoices, the bridge's cancelled hold invoices "+
		"among them, which a quote checks besides the bridge's journal "+
		"so that a payment hash a payer already paid into is not "+
		"quoted again. Leave both gc-canceled-invoices options off on "+
		"this node and on the SHA256 node", strings.Join(on, " and "))
}
