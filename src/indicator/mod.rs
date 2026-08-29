use venue_domain::{MarketLevel, Symbol};
pub use venue_indicators::{
    BARS_SOURCE, BOOK_SOURCE, BREAKOUT_OPPORTUNITY_VERSION_KEY, BreakoutDirection,
    BreakoutOpportunity, FEATURE_PROFILE_DIGEST_KEY, FEATURE_PROFILE_KEY, FeatureBuildError,
    FeatureFrame, FeatureFrameError, FeatureState, FeatureValues, PublicBook,
    PublicMarketSourceError, PublicMarketSourceOutput, RecordedPublicEvent, ScalpingFeatureBuilder,
    ScalpingPublicMarketSource, SourceCursor, TRADES_SOURCE,
};

/// Root-market adapter for the transport-free indicator input contract.
impl PublicBook for crate::market::OrderBook {
    fn synchronized(&self) -> bool {
        self.synchronized()
    }

    fn bridged(&self) -> bool {
        self.bridged()
    }

    fn symbol(&self) -> Option<&Symbol> {
        self.symbol()
    }

    fn generation(&self) -> Option<u64> {
        self.generation()
    }

    fn sequence(&self) -> Option<u64> {
        self.sequence()
    }

    fn bids(&self) -> Vec<MarketLevel> {
        self.bids()
    }

    fn asks(&self) -> Vec<MarketLevel> {
        self.asks()
    }
}
