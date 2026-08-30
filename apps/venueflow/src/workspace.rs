use egui_tiles::{Linear, LinearDir, Tile, TileId, Tiles, Tree};
use serde::{Deserialize, Serialize};

use crate::{
    chart::{ChartInterval, ChartViewport},
    model::WorkspaceKind,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PaneKind {
    MarketWatch,
    Chart,
    OrderBook,
    TradeTape,
    Accounts,
    Strategies,
    CopyRelations,
    Ledger,
    Control,
    Diagnostics,
}

impl PaneKind {
    pub const fn title(self) -> &'static str {
        match self {
            Self::MarketWatch => "Market watch",
            Self::Chart => "Chart & indicators",
            Self::OrderBook => "Order book",
            Self::TradeTape => "Trade tape",
            Self::Accounts => "Accounts",
            Self::Strategies => "Strategies",
            Self::CopyRelations => "Copy relations",
            Self::Ledger => "Receipt ledger",
            Self::Control => "Lifecycle control",
            Self::Diagnostics => "Diagnostics",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Pane {
    pub kind: PaneKind,
    pub instance: u32,
    pub symbol: Option<String>,
    pub interval: ChartInterval,
    pub viewport: ChartViewport,
}

impl Pane {
    fn new(kind: PaneKind, instance: u32) -> Self {
        Self {
            kind,
            instance,
            symbol: None,
            interval: ChartInterval::default(),
            viewport: ChartViewport::default(),
        }
    }

    fn chart(instance: u32, symbol: &str) -> Self {
        Self {
            kind: PaneKind::Chart,
            instance,
            symbol: Some(symbol.to_owned()),
            interval: ChartInterval::default(),
            viewport: ChartViewport::default(),
        }
    }

    pub fn title(&self) -> String {
        if self.kind == PaneKind::Chart && self.instance > 1 {
            format!("{} {}", self.kind.title(), self.instance)
        } else {
            self.kind.title().to_owned()
        }
    }
}

impl Default for Pane {
    fn default() -> Self {
        Self::new(PaneKind::Chart, 1)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Workspaces {
    pub active: WorkspaceKind,
    pub trading: Tree<Pane>,
    pub operations: Tree<Pane>,
    pub multi_chart: Tree<Pane>,
}

impl Default for Workspaces {
    fn default() -> Self {
        Self {
            active: WorkspaceKind::Trading,
            trading: build_trading(),
            operations: build_operations(),
            multi_chart: build_multi_chart(),
        }
    }
}

impl Workspaces {
    fn active_tree(&self) -> &Tree<Pane> {
        match self.active {
            WorkspaceKind::Trading => &self.trading,
            WorkspaceKind::Operations => &self.operations,
            WorkspaceKind::MultiChart => &self.multi_chart,
        }
    }

    pub fn active_tree_mut(&mut self) -> &mut Tree<Pane> {
        match self.active {
            WorkspaceKind::Trading => &mut self.trading,
            WorkspaceKind::Operations => &mut self.operations,
            WorkspaceKind::MultiChart => &mut self.multi_chart,
        }
    }

    pub fn restore_active(&mut self) {
        match self.active {
            WorkspaceKind::Trading => self.trading = build_trading(),
            WorkspaceKind::Operations => self.operations = build_operations(),
            WorkspaceKind::MultiChart => self.multi_chart = build_multi_chart(),
        }
    }

    pub fn pane_visibility(&self) -> Vec<(TileId, String, bool)> {
        let tree = self.active_tree();
        tree.tiles
            .iter()
            .filter_map(|(tile_id, tile)| match tile {
                Tile::Pane(pane) => Some((*tile_id, pane.title(), tree.tiles.is_visible(*tile_id))),
                Tile::Container(_) => None,
            })
            .collect()
    }

    pub fn set_visible(&mut self, tile_id: TileId, visible: bool) {
        self.active_tree_mut().tiles.set_visible(tile_id, visible);
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn active_chart_requests(&self, fallback_symbol: &str) -> Vec<(String, ChartInterval)> {
        let tree = self.active_tree();
        tree.tiles
            .iter()
            .filter_map(|(tile_id, tile)| match tile {
                Tile::Pane(pane)
                    if pane.kind == PaneKind::Chart && tree.tiles.is_visible(*tile_id) =>
                {
                    Some((
                        pane.symbol
                            .clone()
                            .unwrap_or_else(|| fallback_symbol.to_owned()),
                        pane.interval,
                    ))
                }
                Tile::Pane(_) | Tile::Container(_) => None,
            })
            .collect()
    }
}

fn pane(tiles: &mut Tiles<Pane>, kind: PaneKind, instance: u32) -> TileId {
    tiles.insert_pane(Pane::new(kind, instance))
}

fn chart(tiles: &mut Tiles<Pane>, instance: u32, symbol: &str) -> TileId {
    tiles.insert_pane(Pane::chart(instance, symbol))
}

fn split(
    tiles: &mut Tiles<Pane>,
    direction: LinearDir,
    first: TileId,
    second: TileId,
    fraction: f32,
) -> TileId {
    tiles.insert_container(Linear::new_binary(direction, [first, second], fraction))
}

fn build_trading() -> Tree<Pane> {
    let mut tiles = Tiles::default();
    let watch = pane(&mut tiles, PaneKind::MarketWatch, 1);
    let chart = pane(&mut tiles, PaneKind::Chart, 1);
    let book = pane(&mut tiles, PaneKind::OrderBook, 1);
    let tape = pane(&mut tiles, PaneKind::TradeTape, 1);
    let strategies = pane(&mut tiles, PaneKind::Strategies, 1);
    let control = pane(&mut tiles, PaneKind::Control, 1);
    let market_side = split(&mut tiles, LinearDir::Vertical, book, tape, 0.52);
    let upper = split(&mut tiles, LinearDir::Horizontal, chart, market_side, 0.76);
    let lower = split(&mut tiles, LinearDir::Horizontal, strategies, control, 0.72);
    let center = split(&mut tiles, LinearDir::Vertical, upper, lower, 0.67);
    let root = split(&mut tiles, LinearDir::Horizontal, watch, center, 0.16);
    Tree::new("venueflow-trading", root, tiles)
}

fn build_operations() -> Tree<Pane> {
    let mut tiles = Tiles::default();
    let accounts = pane(&mut tiles, PaneKind::Accounts, 1);
    let strategies = pane(&mut tiles, PaneKind::Strategies, 2);
    let copy = pane(&mut tiles, PaneKind::CopyRelations, 1);
    let ledger = pane(&mut tiles, PaneKind::Ledger, 1);
    let control = pane(&mut tiles, PaneKind::Control, 2);
    let diagnostics = pane(&mut tiles, PaneKind::Diagnostics, 1);
    let left_top = split(
        &mut tiles,
        LinearDir::Horizontal,
        accounts,
        strategies,
        0.45,
    );
    let left = split(&mut tiles, LinearDir::Vertical, left_top, copy, 0.62);
    let right_top = split(&mut tiles, LinearDir::Vertical, control, diagnostics, 0.62);
    let right = split(&mut tiles, LinearDir::Vertical, right_top, ledger, 0.42);
    let root = split(&mut tiles, LinearDir::Horizontal, left, right, 0.66);
    Tree::new("venueflow-operations", root, tiles)
}

fn build_multi_chart() -> Tree<Pane> {
    let mut tiles = Tiles::default();
    let watch = pane(&mut tiles, PaneKind::MarketWatch, 2);
    let btc = chart(&mut tiles, 1, "BTC/USDT");
    let eth = chart(&mut tiles, 2, "ETH/USDT");
    let sol = chart(&mut tiles, 3, "SOL/USDT");
    let doge = chart(&mut tiles, 4, "DOGE/USDT");
    let charts = tiles.insert_grid_tile(vec![btc, eth, sol, doge]);
    let root = split(&mut tiles, LinearDir::Horizontal, watch, charts, 0.14);
    Tree::new("venueflow-multi-chart", root, tiles)
}

#[cfg(test)]
mod tests {
    use egui_tiles::Tile;

    use super::{PaneKind, Workspaces};

    #[test]
    fn operational_workspace_contains_control_and_audit_surfaces() {
        let workspaces = Workspaces::default();
        for required in [
            PaneKind::Accounts,
            PaneKind::Strategies,
            PaneKind::CopyRelations,
            PaneKind::Ledger,
            PaneKind::Control,
        ] {
            assert!(
                workspaces
                    .operations
                    .tiles
                    .iter()
                    .any(|(_, tile)| { matches!(tile, Tile::Pane(pane) if pane.kind == required) })
            );
        }
    }

    #[test]
    fn multi_chart_preserves_four_independent_symbols() {
        let workspaces = Workspaces::default();
        let count = workspaces
            .multi_chart
            .tiles
            .iter()
            .filter(|(_, tile)| matches!(tile, Tile::Pane(pane) if pane.kind == PaneKind::Chart))
            .count();
        assert_eq!(count, 4);
    }
}
