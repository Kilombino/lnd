package lnd

import (
	"fmt"
	"math"
	"path/filepath"

	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcwallet/wallet/txauthor"
	"github.com/lightningnetwork/lnd/chainreg"
	"github.com/lightningnetwork/lnd/input"
	"github.com/lightningnetwork/lnd/zpay32"
)

// blake2bDefaultTargetShift is Knots' Consensus::Params Blake2bTargetShift
// default, which every network but mainnet (22) uses.
const blake2bDefaultTargetShift = 20

// applyBlake2bChainConfig applies the Bitcoin BLAKE2b chain-identity options
// to the active network parameters once the network has been selected, and
// registers the invoice prefix for it. It returns an error for combinations
// that would let the daemon run against a chain it cannot identify.
func applyBlake2bChainConfig(cfg *Config) error {
	params := &cfg.ActiveNetParams

	// allow-peers-without-networks became the default when option_blake2b
	// moved to its even bit, and is kept only so that a configuration
	// written before that still starts. Asking for both at once is asking
	// for opposite things, and only a configuration written after the
	// change can do it, so saying so is better than picking one.
	if cfg.AllowPeersWithoutNetworks && cfg.RequirePeerNetworks {
		return fmt.Errorf("allow-peers-without-networks and " +
			"require-peer-networks contradict each other; " +
			"allow-peers-without-networks is deprecated and is " +
			"now the default, so drop it")
	}

	if cfg.Bitcoin.Blake2bActivationHeight != 0 {
		if cfg.Bitcoin.MainNet {
			return fmt.Errorf("bitcoin.blake2b-activation-height cannot " +
				"be set on mainnet: the activation height is fixed " +
				"at 961640")
		}
		// It ends up in btcd's int32 heights: a value past that would
		// turn negative there and fail with a misleading message.
		if cfg.Bitcoin.Blake2bActivationHeight > math.MaxInt32 {
			return fmt.Errorf("bitcoin.blake2b-activation-height %d is "+
				"out of range", cfg.Bitcoin.Blake2bActivationHeight)
		}
		params.Blake2bActivationHeight = cfg.Bitcoin.Blake2bActivationHeight

		// The light client validates BLAKE2b proof of work itself from the
		// btcd parameters, so they must carry this run's height too. A copy,
		// so the package-level parameters stay as shipped.
		p := *params.Params
		p.Blake2bHeight = int32(cfg.Bitcoin.Blake2bActivationHeight)
		if p.Blake2bTargetShift == 0 {
			p.Blake2bTargetShift = blake2bDefaultTargetShift
		}
		params.Params = &p
	}

	// The light client takes the activation height from btcd's parameters
	// (testnet4's built-in 150308, unless overridden above); the
	// chain-identity check, which a bitcoind backend can ask the node
	// about, gets the same one.
	if cfg.Bitcoin.Node == neutrinoBackendName &&
		params.Blake2bActivationHeight == 0 &&
		params.Params.Blake2bPowDeployed() {

		params.Blake2bActivationHeight = uint32(params.Params.Blake2bHeight)
	}

	if cfg.Bitcoin.ChainHashOverride != "" {
		if !cfg.Bitcoin.RegTest {
			return fmt.Errorf("bitcoin.chain-hash-override is only " +
				"accepted on regtest")
		}
		h, err := chainreg.ParseChainHashOverride(
			cfg.Bitcoin.ChainHashOverride,
		)
		if err != nil {
			return fmt.Errorf("bitcoin.chain-hash-override: %w", err)
		}
		params.ChainHash = h
	}

	// A local network chooses its activation height per run, so the daemon
	// has no way to know it unless told. Refuse to guess; the only
	// exception is the integration build, which may follow a SHA256d
	// regtest for upstream's own tests.
	if cfg.Bitcoin.Node == bitcoindBackendName &&
		cfg.Bitcoin.IsLocalNetwork() &&
		params.Blake2bActivationHeight == 0 &&
		!chainreg.AllowSHA256Regtest() {

		return fmt.Errorf("bitcoin.blake2b-activation-height is required "+
			"on %s: set it to the height the node activates BLAKE2b "+
			"at (its -testactivationheight=blake2b@N)",
			cfg.ActiveNetParams.Name)
	}

	// The status file is what a wrapper reads to show the outcome of the
	// chain-identity check, so its location must not depend on the
	// working directory the daemon happened to start in.
	if cfg.Bitcoin.ChainIdentityFile != "" {
		path := CleanAndExpandPath(cfg.Bitcoin.ChainIdentityFile)
		if !filepath.IsAbs(path) {
			return fmt.Errorf("bitcoin.chain-identity-file must be an "+
				"absolute path, got %q", cfg.Bitcoin.ChainIdentityFile)
		}
		cfg.Bitcoin.ChainIdentityFile = path
	}

	// Replay protection for everything this node signs alone. Mainnet
	// has no off switch: a sweep or on-chain send made without the opt-in
	// is a valid SHA256d transaction spending the same pre-fork coins.
	if cfg.Bitcoin.NoUnifiedSigHash && cfg.Bitcoin.MainNet {
		return fmt.Errorf("bitcoin.no-unified-sighash cannot be set on " +
			"mainnet")
	}
	if cfg.Bitcoin.MainNet && !input.DefaultUnifiedSigHash() {
		return fmt.Errorf("this is a development build, which does not " +
			"opt its signatures into the unified signature hash, and " +
			"cannot run on mainnet; use a release build")
	}
	// A development or integration build never opts in: it runs upstream's
	// tests against a stock SHA256d regtest.
	applyUnifiedSigHash(
		!cfg.Bitcoin.NoUnifiedSigHash && input.DefaultUnifiedSigHash(),
	)
	input.SetAllowLegacySigHash(cfg.Bitcoin.AllowLegacySigHash)

	// Invoices are minted with the ordinary BOLT 11 prefix. The withdrawn
	// one is registered for decoding only, so that invoices this node
	// issued under it keep listing: lnd re-decodes a stored payment
	// request on every list, and a failure there fails the whole call.
	zpay32.RegisterLegacyInvoiceHRP(params.Name, params.LegacyInvoiceHRP)

	return nil
}

// applyUnifiedSigHash switches the opt-in for every signing path the node
// uses alone: the sign descriptors it builds (through input.SoleSignerSigHash)
// and the wallet library's own on-chain sends, which choose their hash types
// from package-level settings.
func applyUnifiedSigHash(enabled bool) {
	input.SetUnifiedSigHash(enabled)
	if enabled {
		txauthor.WitnessSigHashType = txscript.SigHashAll |
			txscript.SigHashUnified
		txauthor.TaprootSigHashType = txscript.SigHashAll |
			txscript.SigHashUnified
	} else {
		txauthor.WitnessSigHashType = txscript.SigHashAll
		txauthor.TaprootSigHashType = txscript.SigHashDefault
	}

	// Bare and P2SH inputs keep SIGHASH_ALL either way: the library's
	// legacy signer hashes the legacy message, and a legacy digest with
	// the bit appended is what the fork chain rejects. The wallet hands
	// out no such addresses, so this only concerns coins imported from
	// elsewhere, which stay spendable, and replayable.
	txauthor.LegacySigHashType = txscript.SigHashAll
}

// checkNeutrinoBlake2b holds the neutrino options that matter on the BLAKE2b
// chain. The DNS seeds of mainnet and testnet4 are Bitcoin's and return almost
// only nodes on the SHA256d chain, which the light client bans at the
// activation height, so it needs peers that serve BLAKE2b headers and compact
// filters, named with neutrino.connect. neutrino.addpeer alone still mixes in
// seed peers: allowed, with a warning. The header import reads 80-byte
// headers only and cannot carry a chain past the activation height.
func checkNeutrinoBlake2b(cfg *Config) error {
	if cfg.NeutrinoMode.BlockHeadersSource != "" ||
		cfg.NeutrinoMode.FilterHeadersSource != "" {

		return fmt.Errorf("neutrino.blockheaderssource and " +
			"neutrino.filterheaderssource are not supported on the " +
			"Bitcoin BLAKE2b chain: the import reads 80-byte headers " +
			"only")
	}

	if cfg.Bitcoin.IsLocalNetwork() || len(cfg.NeutrinoMode.ConnectPeers) > 0 {
		return nil
	}
	if len(cfg.NeutrinoMode.AddPeers) == 0 {
		return fmt.Errorf("bitcoin.node=neutrino on %s needs " +
			"neutrino.connect: the DNS seeds return nodes on the " +
			"SHA256d chain; name Bitcoin Knots nodes on the BLAKE2b " +
			"chain with blockfilterindex=1 and peerblockfilters=1, " +
			"ideally your own", cfg.ActiveNetParams.Name)
	}
	ltndLog.Warnf("neutrino.addpeer without neutrino.connect: peers from "+
		"the DNS seeds, almost all on the SHA256d chain, are dialled "+
		"too (and banned at the BLAKE2b activation height); prefer "+
		"neutrino.connect to Knots nodes on the BLAKE2b chain on %s",
		cfg.ActiveNetParams.Name)

	return nil
}
