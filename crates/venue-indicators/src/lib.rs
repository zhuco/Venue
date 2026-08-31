//! Deterministic, transport-free feature calculation over normalized public market facts.

pub mod catalog;
pub mod chart;
mod feature_frame;
mod orderbook;
mod public_book;
mod public_market_source;
mod scalping_features;

pub use feature_frame::{
    BARS_SOURCE, BOOK_SOURCE, BREAKOUT_OPPORTUNITY_VERSION_KEY, BreakoutDirection,
    BreakoutOpportunity, FEATURE_PROFILE_DIGEST_KEY, FEATURE_PROFILE_KEY, FeatureFrame,
    FeatureFrameError, FeatureState, FeatureValues, SourceCursor, TRADES_SOURCE,
};
pub use orderbook::{BookError, OrderBook};
pub use public_book::PublicBook;
pub use public_market_source::{
    PublicMarketSourceError, PublicMarketSourceOutput, RecordedPublicEvent,
    ScalpingPublicMarketSource,
};
pub use scalping_features::{FeatureBuildError, ScalpingFeatureBuilder};
