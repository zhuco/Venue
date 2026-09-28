use super::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
};
use venue_domain::{MarketTimeSource, OpenInterestSample};
use venue_gateway_api::{PublicMarketBinding, VenueId};

const VERSION: u8 = 1;
const MAX_ENTRY_BYTES: u64 = 2 * 1024 * 1024;
const MAX_DISK_BYTES: u64 = 512 * 1024 * 1024;
const MAX_FILES: usize = 4_096;
const PRUNE_TO_BYTES: u64 = MAX_DISK_BYTES - 16 * MAX_ENTRY_BYTES;
const PRUNE_TO_FILES: usize = MAX_FILES - 16;
static WRITES: AtomicUsize = AtomicUsize::new(0);
static TEMP_SEQ: AtomicUsize = AtomicUsize::new(0);
static LATEST_OPEN: OnceLock<parking_lot::Mutex<BTreeMap<PathBuf, u64>>> = OnceLock::new();

fn latest_open() -> &'static parking_lot::Mutex<BTreeMap<PathBuf, u64>> {
    LATEST_OPEN.get_or_init(|| parking_lot::Mutex::new(BTreeMap::new()))
}

fn ordinary_cache_dir(path: &std::path::Path) -> bool {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return false;
    };
    if !metadata.file_type().is_dir() {
        return false;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x400 != 0 {
            return false;
        }
    }
    true
}

#[derive(Serialize, Deserialize)]
struct Entry {
    version: u8,
    scope: String,
    interval_ms: u64,
    before: Option<u64>,
    bars: Vec<PublicBar>,
}

#[derive(Serialize, Deserialize)]
struct InterestEntry {
    version: u8,
    scope: String,
    samples: Vec<OpenInterestSample>,
}

/// Rebuildable, credential-free public candle cache. Every read verifies its complete scope.
pub(super) struct PublicHistoryCache {
    root: PathBuf,
}

impl PublicHistoryCache {
    pub(super) fn local() -> Option<Self> {
        let base = std::env::var_os("LOCALAPPDATA")
            .or_else(|| std::env::var_os("XDG_CACHE_HOME"))
            .or_else(|| {
                std::env::var_os("HOME")
                    .map(|home| PathBuf::from(home).join(".cache").into_os_string())
            })?;
        Some(Self {
            root: PathBuf::from(base)
                .join("VenueFlow")
                .join("public-market-cache")
                .join("v1"),
        })
    }

    #[cfg(test)]
    fn at(root: PathBuf) -> Self {
        Self { root }
    }

    fn scope(selection: &MarketSelection) -> Option<String> {
        serde_json::to_string(&selection.binding).ok()
    }

    fn binding_scope(binding: &PublicMarketBinding) -> Option<String> {
        serde_json::to_string(binding).ok()
    }

    fn scope_hash(scope: &str) -> u64 {
        scope.bytes().fold(0xcbf29ce484222325u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
        })
    }

    fn file(&self, selection: &MarketSelection, before: Option<u64>) -> Option<PathBuf> {
        // The hash is only a filename; the full serialized binding is checked inside the file.
        let scope = Self::scope(selection)?;
        let hash = Self::scope_hash(&scope);
        let suffix = before.map_or("recent".to_owned(), |time| format!("before-{time}"));
        Some(self.root.join(format!(
            "p-{hash:016x}-{}-{suffix}.json",
            selection.interval.duration_ms()
        )))
    }

    fn interest_file(&self, binding: &PublicMarketBinding) -> Option<PathBuf> {
        let scope = Self::binding_scope(binding)?;
        Some(
            self.root
                .join(format!("oi-{:016x}.json", Self::scope_hash(&scope))),
        )
    }

    fn interest_complete(binding: &PublicMarketBinding, sample: &OpenInterestSample) -> bool {
        // Local samples are point observations. Binance's history timestamp
        // denotes the end of the period; the other native endpoints use its start.
        let end = if sample.time_source == MarketTimeSource::LocalObservation
            || binding.venue == VenueId::Binance
        {
            sample.exchange_time_ms
        } else {
            sample.exchange_time_ms.saturating_add(300_000)
        };
        end <= sample.received_at_ms
    }

    fn interest_source(binding: &PublicMarketBinding) -> MarketTimeSource {
        if matches!(binding.venue, VenueId::Bitget | VenueId::Hyperliquid) {
            MarketTimeSource::LocalObservation
        } else {
            MarketTimeSource::Exchange
        }
    }

    pub(super) fn interest_fresh(samples: &[OpenInterestSample], now: u64) -> bool {
        samples.last().is_some_and(|last| {
            last.received_at_ms <= now
                && now.saturating_sub(last.received_at_ms) < 300_000
                && last.exchange_time_ms <= now
                && now.saturating_sub(last.exchange_time_ms) < 600_000
        })
    }

    pub(super) fn interest(
        &self,
        binding: &PublicMarketBinding,
        generation: u64,
    ) -> Option<Vec<OpenInterestSample>> {
        let path = self.interest_file(binding)?;
        let metadata = fs::symlink_metadata(&path).ok()?;
        if !metadata.file_type().is_file() || metadata.len() > MAX_ENTRY_BYTES {
            return None;
        }
        let entry: InterestEntry = serde_json::from_slice(&fs::read(path).ok()?).ok()?;
        if entry.version != VERSION
            || entry.scope != Self::binding_scope(binding)?
            || entry.samples.is_empty()
            || entry.samples.len() > 300
            || entry
                .samples
                .windows(2)
                .any(|pair| pair[0].exchange_time_ms >= pair[1].exchange_time_ms)
            || entry.samples.iter().any(|sample| {
                !sample.is_valid()
                    || sample.symbol != binding.symbol
                    || sample.sampling_interval_ms != Some(300_000)
                    || sample.time_source != Self::interest_source(binding)
                    || !Self::interest_complete(binding, sample)
            })
        {
            return None;
        }
        let mut samples = entry.samples;
        for sample in &mut samples {
            sample.generation = generation;
        }
        Some(samples)
    }

    pub(super) fn remember_interest(
        &self,
        binding: &PublicMarketBinding,
        generation: u64,
        fresh: Vec<OpenInterestSample>,
    ) -> Option<Vec<OpenInterestSample>> {
        if fresh.is_empty() {
            return None;
        }
        let mut merged = BTreeMap::new();
        for mut sample in self
            .interest(binding, generation)
            .into_iter()
            .flatten()
            .chain(fresh)
        {
            sample.generation = generation;
            if !sample.is_valid()
                || sample.symbol != binding.symbol
                || sample.sampling_interval_ms != Some(300_000)
                || sample.time_source != Self::interest_source(binding)
                || !Self::interest_complete(binding, &sample)
            {
                return None;
            }
            merged.insert(sample.exchange_time_ms, sample);
        }
        if merged.is_empty() {
            return None;
        }
        let samples = merged
            .into_values()
            .rev()
            .take(300)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>();
        let path = self.interest_file(binding)?;
        let entry = InterestEntry {
            version: VERSION,
            scope: Self::binding_scope(binding)?,
            samples: samples.clone(),
        };
        if let Ok(bytes) = serde_json::to_vec(&entry) {
            self.write_bytes(path, bytes);
        }
        Some(samples)
    }

    pub(super) fn observe_interest(
        &self,
        binding: &PublicMarketBinding,
        generation: u64,
        current: &OpenInterestSample,
    ) -> Option<Vec<OpenInterestSample>> {
        if Self::interest_source(binding) != MarketTimeSource::LocalObservation
            || !current.is_valid()
            || current.symbol != binding.symbol
            || current.sampling_interval_ms.is_some()
            || current
                .received_at_ms
                .saturating_sub(current.exchange_time_ms)
                > 30_000
        {
            return None;
        }
        let cached = self.interest(binding, generation);
        let bucket = current.received_at_ms / 300_000;
        if cached
            .as_ref()
            .and_then(|samples| samples.last())
            .is_some_and(|last| last.exchange_time_ms / 300_000 == bucket)
        {
            return cached;
        }
        let mut sample = current.clone();
        sample.generation = generation;
        sample.exchange_time_ms = current.received_at_ms;
        sample.time_source = MarketTimeSource::LocalObservation;
        sample.sampling_interval_ms = Some(300_000);
        self.remember_interest(binding, generation, vec![sample])
    }

    fn read(&self, selection: &MarketSelection, before: Option<u64>) -> Option<Vec<PublicBar>> {
        let path = self.file(selection, before)?;
        let metadata = fs::symlink_metadata(&path).ok()?;
        if !metadata.file_type().is_file() || metadata.len() > MAX_ENTRY_BYTES {
            return None;
        }
        let entry: Entry = serde_json::from_slice(&fs::read(path).ok()?).ok()?;
        if entry.version != VERSION
            || entry.scope != Self::scope(selection)?
            || entry.interval_ms != selection.interval.duration_ms()
            || entry.before != before
            || (before.is_none() && entry.bars.is_empty())
            || entry.bars.len() > DEFAULT_HISTORY_LIMIT
            || entry
                .bars
                .windows(2)
                .any(|pair| pair[0].open_time_ms >= pair[1].open_time_ms)
            || entry.bars.iter().any(|bar| {
                !bar.is_valid()
                    || bar.symbol != selection.binding.symbol
                    || bar.interval_ms != entry.interval_ms
                    || bar.close_time_ms >= bar.received_at_ms
                    || before.is_some_and(|cursor| bar.open_time_ms >= cursor)
            })
        {
            return None;
        }
        Some(entry.bars)
    }

    pub(super) fn recent(
        &self,
        selection: &MarketSelection,
        generation: u64,
    ) -> Option<Vec<PublicBar>> {
        let mut bars = self.read(selection, None)?;
        for bar in &mut bars {
            bar.generation = generation;
        }
        Some(bars)
    }

    pub(super) fn recent_last_open(
        &self,
        selection: &MarketSelection,
        generation: u64,
    ) -> Option<u64> {
        let path = self.file(selection, None)?;
        if let Some(open) = latest_open().lock().get(&path).copied() {
            return Some(open);
        }
        let open = self.recent(selection, generation)?.last()?.open_time_ms;
        let mut latest = latest_open().lock();
        if latest.len() >= 128 {
            latest.clear();
        }
        latest.insert(path, open);
        Some(open)
    }

    pub(super) fn page(
        &self,
        selection: &MarketSelection,
        before: u64,
        generation: u64,
    ) -> Option<Vec<PublicBar>> {
        let mut bars = self
            .read(selection, Some(before))
            .or_else(|| {
                // A recently cached tail can satisfy a scroll back without another API page.
                let bars = self.read(selection, None)?;
                let end = bars.partition_point(|bar| bar.open_time_ms < before);
                if end == 0
                    || bars[end - 1]
                        .open_time_ms
                        .saturating_add(selection.interval.duration_ms())
                        != before
                {
                    return None;
                }
                Some(bars[end.saturating_sub(500)..end].to_vec())
            })
            .or_else(|| self.covering_page(selection, before))?;
        for bar in &mut bars {
            bar.generation = generation;
        }
        Some(bars)
    }

    fn covering_page(&self, selection: &MarketSelection, before: u64) -> Option<Vec<PublicBar>> {
        if !self.root.ancestors().take(3).all(ordinary_cache_dir) {
            return None;
        }
        let scope = Self::scope(selection)?;
        let prefix = format!(
            "p-{:016x}-{}-before-",
            Self::scope_hash(&scope),
            selection.interval.duration_ms()
        );
        let mut cursors = fs::read_dir(&self.root)
            .ok()?
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name();
                let name = name.to_str()?;
                let cursor = name
                    .strip_prefix(&prefix)?
                    .strip_suffix(".json")?
                    .parse::<u64>()
                    .ok()?;
                (cursor > before
                    && cursor.saturating_sub(before)
                        <= (DEFAULT_HISTORY_LIMIT as u64)
                            .saturating_mul(selection.interval.duration_ms()))
                .then_some(cursor)
            })
            .collect::<Vec<_>>();
        cursors.sort_unstable();
        cursors.dedup();
        // A moved request cursor can still be inside an older cached response.
        // Read only nearby candidates; corrupt or mismatched files remain misses.
        for cursor in cursors.into_iter().take(16) {
            let Some(bars) = self.read(selection, Some(cursor)) else {
                continue;
            };
            let end = bars.partition_point(|bar| bar.open_time_ms < before);
            if end == 0
                || bars[end - 1]
                    .open_time_ms
                    .saturating_add(selection.interval.duration_ms())
                    != before
            {
                continue;
            }
            return Some(bars[end.saturating_sub(500)..end].to_vec());
        }
        None
    }

    pub(super) fn remember_recent(&self, selection: &MarketSelection, bars: &[PublicBar]) {
        if bars.is_empty() {
            return;
        }
        let previous = self.read(selection, None);
        if let Some(previous) = previous.as_ref()
            && bars.iter().all(|fresh| {
                previous
                    .binary_search_by_key(&fresh.open_time_ms, |older| older.open_time_ms)
                    .is_ok_and(|index| {
                        let older = &previous[index];
                        older.close_time_ms == fresh.close_time_ms
                            && older.open == fresh.open
                            && older.high == fresh.high
                            && older.low == fresh.low
                            && older.close == fresh.close
                            && older.base_volume == fresh.base_volume
                            && older.quote_volume == fresh.quote_volume
                            && older.trade_count == fresh.trade_count
                            && older.taker_buy_base_volume == fresh.taker_buy_base_volume
                            && older.taker_buy_quote_volume == fresh.taker_buy_quote_volume
                    })
            })
        {
            return;
        }
        let mut merged = BTreeMap::new();
        let previous = previous.filter(|older| {
            older.last().is_some_and(|last| {
                bars.first().is_some_and(|first| {
                    last.open_time_ms.saturating_add(last.interval_ms) >= first.open_time_ms
                })
            })
        });
        for bar in previous.into_iter().flatten().chain(bars.iter().cloned()) {
            merged.insert(bar.open_time_ms, bar);
        }
        let recent = merged
            .into_values()
            .rev()
            .take(DEFAULT_HISTORY_LIMIT)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>();
        self.write(selection, None, &recent);
        if let (Some(path), Some(last)) = (self.file(selection, None), recent.last()) {
            let mut latest = latest_open().lock();
            if latest.len() >= 128 {
                latest.clear();
            }
            latest.insert(path, last.open_time_ms);
        }
    }

    pub(super) fn remember_page(
        &self,
        selection: &MarketSelection,
        before: u64,
        bars: &[PublicBar],
    ) {
        self.write(selection, Some(before), bars);
    }

    fn write(&self, selection: &MarketSelection, before: Option<u64>, bars: &[PublicBar]) {
        if (bars.is_empty() && before.is_none())
            || bars.len() > DEFAULT_HISTORY_LIMIT
            || bars
                .windows(2)
                .any(|pair| pair[0].open_time_ms >= pair[1].open_time_ms)
            || bars.iter().any(|bar| {
                !bar.is_valid()
                    || bar.symbol != selection.binding.symbol
                    || bar.interval_ms != selection.interval.duration_ms()
                    || bar.close_time_ms >= bar.received_at_ms
                    || before.is_some_and(|cursor| bar.open_time_ms >= cursor)
            })
        {
            return;
        }
        let Some(path) = self.file(selection, before) else {
            return;
        };
        let Some(scope) = Self::scope(selection) else {
            return;
        };
        let entry = Entry {
            version: VERSION,
            scope,
            interval_ms: selection.interval.duration_ms(),
            before,
            bars: bars.to_vec(),
        };
        let Ok(bytes) = serde_json::to_vec(&entry) else {
            return;
        };
        self.write_bytes(path, bytes);
    }

    fn write_bytes(&self, path: PathBuf, bytes: Vec<u8>) {
        if bytes.len() as u64 > MAX_ENTRY_BYTES
            || fs::create_dir_all(&self.root).is_err()
            || !self.root.ancestors().take(3).all(ordinary_cache_dir)
        {
            return;
        }
        let temporary = path.with_extension(format!(
            "{}.{}.tmp",
            std::process::id(),
            TEMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        if fs::write(&temporary, bytes).is_err() {
            let _ = fs::remove_file(&temporary);
            return;
        }
        if fs::rename(&temporary, &path).is_err() {
            // Windows cannot atomically replace an existing destination. A missing cache is safe.
            let _ = fs::remove_file(&path);
            let _ = fs::rename(&temporary, &path);
            let _ = fs::remove_file(&temporary);
        }
        if WRITES.fetch_add(1, Ordering::Relaxed) % 16 == 0 {
            self.prune();
        }
    }

    fn prune(&self) {
        self.prune_to(PRUNE_TO_BYTES, PRUNE_TO_FILES);
    }

    fn prune_to(&self, target_bytes: u64, target_files: usize) {
        if !self.root.ancestors().take(3).all(ordinary_cache_dir) {
            return;
        }
        let Ok(entries) = fs::read_dir(&self.root) else {
            return;
        };
        let mut files = entries
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                if !(name.starts_with("p-") || name.starts_with("oi-"))
                    || !name.ends_with(".json")
                    || !entry.file_type().ok()?.is_file()
                {
                    return None;
                }
                let meta = entry.metadata().ok()?;
                Some((entry.path(), meta.len(), meta.modified().ok()?))
            })
            .collect::<Vec<_>>();
        let mut bytes = files.iter().map(|(_, len, _)| len).sum::<u64>();
        // Allow for the fifteen writes before the next scan without crossing the budget.
        if bytes <= target_bytes && files.len() <= target_files {
            return;
        }
        files.sort_by_key(|(_, _, modified)| *modified);
        let mut count = files.len();
        for (path, len, _) in files {
            if bytes <= target_bytes && count <= target_files {
                break;
            }
            if fs::remove_file(&path).is_ok() {
                bytes = bytes.saturating_sub(len);
                count -= 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use venue_domain::{FieldState, OpenInterestUnit, Price};

    fn bar(symbol: &str, open: u64) -> PublicBar {
        let price = Price::new(Decimal::ONE).unwrap();
        PublicBar {
            symbol: symbol.parse().unwrap(),
            generation: 1,
            received_at_ms: open + 60_000,
            sequence: open / 60_000,
            open_time_ms: open,
            close_time_ms: open + 59_999,
            interval_ms: 60_000,
            open: price,
            high: price,
            low: price,
            close: price,
            base_volume: FieldState::Known(Decimal::ONE),
            quote_volume: FieldState::Known(Decimal::ONE),
            trade_count: FieldState::Known(1),
            taker_buy_base_volume: FieldState::Known(Decimal::ZERO),
            taker_buy_quote_volume: FieldState::Known(Decimal::ZERO),
        }
    }

    #[test]
    fn restart_reuses_exact_market_and_rejects_other_scope_and_corruption() {
        let root =
            std::env::temp_dir().join(format!("venue-public-cache-test-{}", std::process::id()));
        let cache = PublicHistoryCache::at(root.clone());
        let doge = MarketSelection::binance_usd_m("DOGE/USDC", ChartInterval::OneMinute).unwrap();
        let btc = MarketSelection::binance_usd_m("BTC/USDC", ChartInterval::OneMinute).unwrap();
        let mut gate = doge.clone();
        gate.binding.venue = venue_gateway_api::VenueId::Gate;
        let daily = MarketSelection::binance_usd_m("DOGE/USDC", ChartInterval::OneDay).unwrap();
        let bars = [bar("DOGE/USDC", 60_000), bar("DOGE/USDC", 120_000)];
        cache.remember_recent(&doge, &bars[1..]);
        cache.remember_recent(&doge, &bars);
        assert_eq!(cache.recent(&doge, 2).unwrap().len(), 2);
        assert_eq!(
            PublicHistoryCache::at(root.clone())
                .recent(&doge, 8)
                .unwrap()[0]
                .generation,
            8
        );
        assert!(cache.recent(&btc, 8).is_none());
        assert!(cache.recent(&gate, 8).is_none());
        assert!(cache.recent(&daily, 8).is_none());
        assert_eq!(cache.page(&doge, 180_000, 9).unwrap().len(), 2);
        cache.remember_page(&doge, 120_000, &bars[..1]);
        assert_eq!(cache.page(&doge, 120_000, 9).unwrap().len(), 1);
        cache.remember_page(&doge, 60_000, &[]);
        assert!(cache.page(&doge, 60_000, 9).unwrap().is_empty());
        let path = cache.file(&doge, None).unwrap();
        fs::write(&path, b"invalid").unwrap();
        assert!(cache.recent(&doge, 8).is_none());
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(cache.file(&doge, Some(120_000)).unwrap());
        let _ = fs::remove_file(cache.file(&doge, Some(60_000)).unwrap());
        let _ = fs::remove_dir(root);
    }

    #[test]
    fn recent_cache_merges_older_gap_and_corrected_high_without_new_tail() {
        let root =
            std::env::temp_dir().join(format!("venue-recent-repair-test-{}", std::process::id()));
        let cache = PublicHistoryCache::at(root.clone());
        let doge = MarketSelection::binance_usd_m("DOGE/USDC", ChartInterval::OneMinute).unwrap();
        let original = bar("DOGE/USDC", 120_000);
        cache.remember_recent(&doge, &[original.clone()]);
        let mut corrected = original;
        corrected.high = Price::new(Decimal::from(2)).unwrap();
        cache.remember_recent(&doge, &[bar("DOGE/USDC", 60_000), corrected]);
        let persisted = PublicHistoryCache::at(root.clone())
            .recent(&doge, 2)
            .unwrap();
        assert_eq!(persisted.len(), 2);
        assert_eq!(persisted[0].open_time_ms, 60_000);
        assert_eq!(persisted[1].high.value(), Decimal::from(2));
        let _ = fs::remove_file(cache.file(&doge, None).unwrap());
        let _ = fs::remove_dir(root);
    }

    #[test]
    fn interest_cache_is_exact_binding_and_generation_scoped() {
        let root =
            std::env::temp_dir().join(format!("venue-interest-cache-test-{}", std::process::id()));
        let cache = PublicHistoryCache::at(root.clone());
        let doge = MarketSelection::binance_usd_m("DOGE/USDC", ChartInterval::OneMinute).unwrap();
        let mut gate = doge.binding.clone();
        gate.venue = venue_gateway_api::VenueId::Gate;
        let sample = OpenInterestSample {
            symbol: doge.binding.symbol.clone(),
            generation: 1,
            received_at_ms: 900_000,
            exchange_time_ms: 600_000,
            time_source: MarketTimeSource::Exchange,
            sampling_interval_ms: Some(300_000),
            native_quantity: Decimal::from(10),
            native_unit: OpenInterestUnit::BaseAsset,
            base_quantity: FieldState::Known(Decimal::from(10)),
            quote_notional: FieldState::Unavailable {
                reason: venue_domain::UnknownReason::SourceOmitted,
            },
            quote_asset: None,
        };
        assert!(
            cache
                .remember_interest(&doge.binding, 1, vec![sample])
                .is_some()
        );
        assert_eq!(
            PublicHistoryCache::at(root.clone())
                .interest(&doge.binding, 8)
                .unwrap()[0]
                .generation,
            8
        );
        assert!(PublicHistoryCache::interest_fresh(
            &cache.interest(&doge.binding, 8).unwrap(),
            900_001
        ));
        assert!(!PublicHistoryCache::interest_fresh(
            &cache.interest(&doge.binding, 8).unwrap(),
            1_300_000
        ));
        let mut end_stamped = cache.interest(&doge.binding, 8).unwrap()[0].clone();
        end_stamped.exchange_time_ms = end_stamped.received_at_ms;
        assert!(
            cache
                .remember_interest(&doge.binding, 8, vec![end_stamped])
                .is_some()
        );
        assert!(cache.interest(&gate, 8).is_none());
        let file = cache.interest_file(&doge.binding).unwrap();
        fs::write(&file, b"invalid").unwrap();
        assert!(cache.interest(&doge.binding, 8).is_none());
        let _ = fs::remove_file(file);
        let _ = fs::remove_dir(root);
    }

    #[test]
    fn shifted_history_cursor_reuses_a_covering_disk_page() {
        let root =
            std::env::temp_dir().join(format!("venue-shifted-cache-test-{}", std::process::id()));
        let cache = PublicHistoryCache::at(root.clone());
        let doge = MarketSelection::binance_usd_m("DOGE/USDC", ChartInterval::OneMinute).unwrap();
        let bars = (1..=5)
            .map(|minute| bar("DOGE/USDC", minute * 60_000))
            .collect::<Vec<_>>();
        cache.remember_page(&doge, 360_000, &bars);
        let shifted = PublicHistoryCache::at(root.clone())
            .page(&doge, 300_000, 7)
            .unwrap();
        assert_eq!(shifted.len(), 4);
        assert_eq!(shifted.last().unwrap().open_time_ms, 240_000);
        assert!(shifted.iter().all(|bar| bar.generation == 7));
        assert!(cache.page(&doge, 390_000, 7).is_none());
        let _ = fs::remove_file(cache.file(&doge, Some(360_000)).unwrap());
        let _ = fs::remove_dir(root);
    }

    #[test]
    fn local_oi_samples_begin_now_and_never_backfill_other_market_history() {
        let root = std::env::temp_dir().join(format!("venue-local-oi-test-{}", std::process::id()));
        let cache = PublicHistoryCache::at(root.clone());
        let bitget = MarketSelection::for_server(
            crate::model::MarketServer::Bitget,
            "DOGE/USDC",
            ChartInterval::OneMinute,
        )
        .unwrap();
        let doge = MarketSelection::binance_usd_m("DOGE/USDC", ChartInterval::OneMinute).unwrap();
        let mut current = OpenInterestSample {
            symbol: bitget.binding.symbol.clone(),
            generation: 1,
            received_at_ms: 900_001,
            exchange_time_ms: 895_000,
            time_source: MarketTimeSource::Exchange,
            sampling_interval_ms: None,
            native_quantity: Decimal::from(10),
            native_unit: OpenInterestUnit::BaseAsset,
            base_quantity: FieldState::Known(Decimal::from(10)),
            quote_notional: FieldState::Unavailable {
                reason: venue_domain::UnknownReason::SourceOmitted,
            },
            quote_asset: None,
        };
        assert_eq!(
            cache
                .observe_interest(&bitget.binding, 1, &current)
                .unwrap()
                .len(),
            1
        );
        assert!(
            venue_indicators::chart::open_interest::changes(
                &cache.interest(&bitget.binding, 1).unwrap()
            )[0]
            .is_none()
        );
        current.received_at_ms = 930_001;
        current.exchange_time_ms = 925_000;
        assert_eq!(
            cache
                .observe_interest(&bitget.binding, 1, &current)
                .unwrap()
                .len(),
            1
        );
        current.received_at_ms = 1_200_001;
        current.exchange_time_ms = 1_198_000;
        assert_eq!(
            cache
                .observe_interest(&bitget.binding, 1, &current)
                .unwrap()
                .len(),
            2
        );
        assert!(
            venue_indicators::chart::open_interest::changes(
                &cache.interest(&bitget.binding, 1).unwrap()
            )[0]
            .is_some()
        );
        let loaded = PublicHistoryCache::at(root.clone())
            .interest(&bitget.binding, 7)
            .unwrap();
        assert_eq!(loaded[0].exchange_time_ms, 900_001);
        assert_eq!(loaded[1].time_source, MarketTimeSource::LocalObservation);
        assert_eq!(loaded[1].generation, 7);
        assert!(cache.interest(&doge.binding, 7).is_none());
        let _ = fs::remove_file(cache.interest_file(&bitget.binding).unwrap());
        let _ = fs::remove_dir(root);
    }

    #[test]
    fn eviction_keeps_recent_scope_and_rebuilds_an_evicted_page() {
        let root = std::env::temp_dir().join(format!(
            "venue-cache-eviction-test-{}-{}",
            std::process::id(),
            TEMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        let cache = PublicHistoryCache::at(root.clone());
        let doge = MarketSelection::binance_usd_m("DOGE/USDC", ChartInterval::OneMinute).unwrap();
        cache.remember_page(&doge, 120_000, &[bar("DOGE/USDC", 60_000)]);
        std::thread::sleep(std::time::Duration::from_millis(20));
        for index in 0..5 {
            fs::write(root.join(format!("p-old-{index}.json")), [index as u8; 10]).unwrap();
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
        cache.remember_recent(&doge, &[bar("DOGE/USDC", 120_000)]);
        fs::write(root.join("leave-alone.txt"), b"owned fixture sentinel").unwrap();
        let recent_len = fs::metadata(cache.file(&doge, None).unwrap())
            .unwrap()
            .len();
        cache.prune_to(recent_len + 20, 3);
        let old_page_evicted = cache.page(&doge, 120_000, 9).is_none();
        let recent_survived = cache.recent(&doge, 9).is_some();
        let sentinel_survived = root.join("leave-alone.txt").is_file();
        let retained = fs::read_dir(&root)
            .unwrap()
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                (name.starts_with("p-") || name.starts_with("oi-"))
                    .then(|| entry.metadata().ok().map(|meta| meta.len()))
                    .flatten()
            })
            .collect::<Vec<_>>();
        cache.remember_page(&doge, 120_000, &[bar("DOGE/USDC", 60_000)]);
        let rebuilt = cache.page(&doge, 120_000, 10).is_some();
        for entry in fs::read_dir(&root).unwrap().flatten() {
            if entry.file_type().is_ok_and(|kind| kind.is_file()) {
                fs::remove_file(entry.path()).unwrap();
            }
        }
        fs::remove_dir(root).unwrap();
        assert!(old_page_evicted && recent_survived && sentinel_survived && rebuilt);
        assert!(retained.len() <= 3);
        assert!(retained.iter().sum::<u64>() <= recent_len + 20);
        assert_eq!(MAX_DISK_BYTES - PRUNE_TO_BYTES, 16 * MAX_ENTRY_BYTES);
        assert_eq!(MAX_FILES - PRUNE_TO_FILES, 16);
    }

    #[test]
    #[ignore = "creates a near-512 MiB disposable public-cache fixture"]
    fn near_budget_prune_preserves_recent_page_and_room_for_fifteen_writes() {
        let root = std::env::temp_dir().join(format!(
            "venue-cache-near-budget-{}-{}",
            std::process::id(),
            TEMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        let cache = PublicHistoryCache::at(root.clone());
        for index in 0..257 {
            let file = fs::File::create(root.join(format!("p-old-{index:03}.json"))).unwrap();
            file.set_len(MAX_ENTRY_BYTES).unwrap();
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
        let doge = MarketSelection::binance_usd_m("DOGE/USDC", ChartInterval::OneMinute).unwrap();
        cache.remember_recent(&doge, &[bar("DOGE/USDC", 120_000)]);
        fs::write(root.join("leave-alone.txt"), b"not a public cache page").unwrap();
        cache.prune();
        let after_prune = fs::read_dir(&root)
            .unwrap()
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                (name.starts_with("p-") || name.starts_with("oi-"))
                    .then(|| entry.metadata().ok().map(|meta| meta.len()))
                    .flatten()
            })
            .collect::<Vec<_>>();
        let recent_survived = cache.recent(&doge, 9).is_some();
        let foreign_survived = root.join("leave-alone.txt").is_file();
        for index in 0..15 {
            let file = fs::File::create(root.join(format!("p-next-{index:03}.json"))).unwrap();
            file.set_len(MAX_ENTRY_BYTES).unwrap();
        }
        let after_writes = fs::read_dir(&root)
            .unwrap()
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                (name.starts_with("p-") || name.starts_with("oi-"))
                    .then(|| entry.metadata().ok().map(|meta| meta.len()))
                    .flatten()
            })
            .collect::<Vec<_>>();
        for entry in fs::read_dir(&root).unwrap().flatten() {
            if entry.file_type().is_ok_and(|kind| kind.is_file()) {
                fs::remove_file(entry.path()).unwrap();
            }
        }
        fs::remove_dir(root).unwrap();
        assert!(recent_survived && foreign_survived);
        assert!(after_prune.len() <= PRUNE_TO_FILES);
        assert!(after_prune.iter().sum::<u64>() <= PRUNE_TO_BYTES);
        assert!(after_writes.iter().sum::<u64>() <= MAX_DISK_BYTES);
    }
}
