use venue_domain::{MarketLevel, Symbol};
use venue_indicators::{OrderBook, PublicBook};

/// A complete adapter-validated WS image needs no REST/delta bridge. This view changes only
/// readiness for that feed contract; it neither invents predecessor IDs nor assembles a book.
pub(super) struct FullSnapshotBook<'a>(pub(super) &'a OrderBook);

impl PublicBook for FullSnapshotBook<'_> {
    fn synchronized(&self) -> bool {
        self.0.synchronized()
    }
    fn bridged(&self) -> bool {
        self.0.synchronized()
    }
    fn symbol(&self) -> Option<&Symbol> {
        self.0.symbol()
    }
    fn generation(&self) -> Option<u64> {
        self.0.generation()
    }
    fn sequence(&self) -> Option<u64> {
        self.0.sequence()
    }
    fn bids(&self) -> Vec<MarketLevel> {
        self.0.bids()
    }
    fn asks(&self) -> Vec<MarketLevel> {
        self.0.asks()
    }
}
