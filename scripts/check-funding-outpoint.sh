#!/bin/bash
# A spliced channel keeps its original funding outpoint as its name
# (FundingOutpoint), while the output on chain is CurrentFundingOutpoint().
# Watching or looking up the name instead of the current output would miss
# a spend of the channel. This fails if FundingOutpoint is passed straight
# to a chain registration or a graph lookup by outpoint.
set -e

cd "$(dirname "$0")/.."

found=$(git ls-files '*.go' | grep -v '_test\.go$' | xargs perl -0777 -ne '
	while (/(RegisterSpendNtfn|RegisterConfirmationsNtfn|FetchChannelEdgesByOutpoint)\(((?:[^()]|\((?:[^()]|\([^()]*\))*\))*)\)/g) {
		my ($fn, $args, $at) = ($1, $2, $-[0]);
		next unless $args =~ /\bFundingOutpoint\b/;
		my $line = 1 + (substr($_, 0, $at) =~ tr/\n//);
		print "$ARGV:$line: $fn uses FundingOutpoint\n";
	}
')

if [ -n "$found" ]; then
	echo "$found"
	echo "Use CurrentFundingOutpoint() for the output on chain."
	exit 1
fi
