package chainfee

// FallbackReporter is implemented by estimators that answer with a fixed
// fallback rate when they have no estimate. On a chain with little fee
// market data that happens often, and a caller that uses the rate as a floor
// needs to tell it from a real estimate.
type FallbackReporter interface {
	// FallbackFeePerKW returns the rate the estimator answers with when
	// it has no estimate.
	FallbackFeePerKW() SatPerKWeight
}

// FallbackFeePerKW returns the fallback rate.
func (b *BtcdEstimator) FallbackFeePerKW() SatPerKWeight {
	return b.fallbackFeePerKW
}

// FallbackFeePerKW returns the fallback rate.
func (b *BitcoindEstimator) FallbackFeePerKW() SatPerKWeight {
	return b.fallbackFeePerKW
}

// FallbackFeePerKW returns the static rate: every answer of a static
// estimator is a configured value, not an estimate.
func (e StaticEstimator) FallbackFeePerKW() SatPerKWeight {
	return e.feePerKW
}

// Compile time checks.
var (
	_ FallbackReporter = (*BtcdEstimator)(nil)
	_ FallbackReporter = (*BitcoindEstimator)(nil)
	_ FallbackReporter = (*StaticEstimator)(nil)
)
