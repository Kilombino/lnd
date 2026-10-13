package lnwire

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCustomOverrideOfKnownType checks that an override can't name a type
// lnd decodes itself, where it would silently never apply, while a type lnd
// doesn't know stays overridable.
func TestCustomOverrideOfKnownType(t *testing.T) {
	// Not parallel: the overrides are a package global.
	t.Cleanup(func() {
		require.NoError(t, SetCustomOverrides(nil))
	})

	for _, known := range []uint16{
		uint16(MsgTxAddInput), uint16(MsgSpliceInit),
		uint16(MsgStartBatch), uint16(MsgCommitSig),
	} {
		require.ErrorContains(t, SetCustomOverrides([]uint16{known}),
			"handles", "type %d", known)
	}

	// 100 is unassigned.
	require.NoError(t, SetCustomOverrides([]uint16{100}))
	require.True(t, IsCustomOverride(100))
	msg, err := MakeEmptyMessage(100)
	require.NoError(t, err)
	require.IsType(t, &Custom{}, msg)
}
