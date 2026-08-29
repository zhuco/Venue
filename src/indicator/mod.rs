use venue_domain::{MarketLevel, Symbol};

#[path = "../../crates/venue-indicators/src/public_book.rs"]
mod public_book;
pub use public_book::PublicBook;

#[path = "../../crates/venue-indicators/src/feature_frame.rs"]
mod feature_frame;
#[path = "../../crates/venue-indicators/src/public_market_source.rs"]
mod public_market_source;
#[path = "../../crates/venue-indicators/src/scalping_features.rs"]
mod scalping_features;

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

pub use feature_frame::{
    BARS_SOURCE, BOOK_SOURCE, BREAKOUT_OPPORTUNITY_VERSION_KEY, BreakoutDirection,
    BreakoutOpportunity, FEATURE_PROFILE_DIGEST_KEY, FEATURE_PROFILE_KEY, FeatureFrame,
    FeatureFrameError, FeatureState, FeatureValues, SourceCursor, TRADES_SOURCE,
};
pub use public_market_source::{
    PublicMarketSourceError, PublicMarketSourceOutput, RecordedPublicEvent,
    ScalpingPublicMarketSource,
};
pub use scalping_features::{FeatureBuildError, ScalpingFeatureBuilder};
