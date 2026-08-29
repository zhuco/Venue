use venue_domain::{MarketLevel, Symbol};

/// Read-only normalized book evidence consumed by feature calculations.
/// Implementations retain synchronization and transport ownership outside this crate.
pub trait PublicBook {
    fn synchronized(&self) -> bool;
    fn bridged(&self) -> bool;
    fn symbol(&self) -> Option<&Symbol>;
    fn generation(&self) -> Option<u64>;
    fn sequence(&self) -> Option<u64>;
    fn bids(&self) -> Vec<MarketLevel>;
    fn asks(&self) -> Vec<MarketLevel>;
}
