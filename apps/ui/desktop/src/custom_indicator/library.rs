use super::CustomSettings;
use serde::{Deserialize, Serialize};
use venue_indicators::chart::script::{MAX_SCRIPTS, ScriptEngine, ScriptSpec};

pub const INTRADAY_SOURCE: &str = include_str!("intraday_source.txt");
pub const STARTER_SOURCE: &str = "// @version=2\nperiod = input(21, title='EMA周期');\nline = ema(close, period);\nplot(line, title='EMA', color='#00E5FF', lineWidth=2);\n";
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EntryKind {
    Legacy(CustomSettings),
    Script { source: String },
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub id: u64,
    pub name: String,
    pub enabled: bool,
    pub kind: EntryKind,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CustomLibrary {
    pub next_id: u64,
    pub entries: Vec<Entry>,
}
impl CustomLibrary {
    pub fn migrate(legacy: &CustomSettings) -> Self {
        Self {
            next_id: 3,
            entries: vec![
                Entry {
                    id: 1,
                    name: "EMA / ADX（原版）".into(),
                    enabled: legacy.enabled,
                    kind: EntryKind::Legacy(legacy.clone()),
                },
                Entry {
                    id: 2,
                    name: "日内敏捷共振 · TD / 高量K线 / 趋势隧道".into(),
                    enabled: false,
                    kind: EntryKind::Script {
                        source: INTRADAY_SOURCE.into(),
                    },
                },
            ],
        }
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.entries.len() > 16 {
            return Err("最多保存16个自定义指标".into());
        }
        if self.entries.iter().filter(|e| e.enabled).count() > MAX_SCRIPTS {
            return Err("最多同时启用4个自定义指标".into());
        }
        let mut ids = std::collections::HashSet::new();
        let mut legacy = 0;
        for e in &self.entries {
            if e.id == 0 || e.id >= self.next_id || !ids.insert(e.id) {
                return Err("指标ID无效或重复".into());
            }
            if e.name.trim().is_empty() || e.name.chars().count() > 80 {
                return Err("名称须为1–80个字符".into());
            }
            match &e.kind {
                EntryKind::Legacy(s) => {
                    legacy += 1;
                    s.parameters.validate().map_err(|e| e.to_string())?;
                }
                EntryKind::Script { source } => {
                    ScriptEngine::compile(&ScriptSpec {
                        id: e.id,
                        source: source.clone(),
                    })?;
                }
            }
        }
        if legacy > 1 {
            return Err("原版 EMA/ADX 模板仅支持一个实例；新建请使用脚本".into());
        }
        Ok(())
    }
    pub fn save(&mut self, mut entry: Entry) -> Result<u64, String> {
        let mut next = self.clone();
        entry.name = entry.name.trim().into();
        if entry.id == 0 {
            entry.id = next.next_id;
            next.next_id = next.next_id.checked_add(1).ok_or("指标ID已耗尽")?;
            next.entries.push(entry.clone());
        } else {
            let slot = next
                .entries
                .iter_mut()
                .find(|e| e.id == entry.id)
                .ok_or("指标已删除")?;
            *slot = entry.clone();
        }
        next.validate()?;
        *self = next;
        Ok(entry.id)
    }
    pub fn remove(&mut self, id: u64) {
        self.entries.retain(|e| e.id != id);
    }
    pub fn legacy(&self) -> CustomSettings {
        self.entries
            .iter()
            .find_map(|e| match &e.kind {
                EntryKind::Legacy(s) => {
                    let mut s = s.clone();
                    s.enabled = e.enabled;
                    Some(s)
                }
                _ => None,
            })
            .unwrap_or_default()
    }
    pub fn scripts(&self) -> Vec<ScriptSpec> {
        self.entries
            .iter()
            .filter(|e| e.enabled)
            .filter_map(|e| match &e.kind {
                EntryKind::Script { source } => Some(ScriptSpec {
                    id: e.id,
                    source: source.clone(),
                }),
                _ => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn crud_is_atomic_ids_survive_delete_and_empty_roundtrip()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut library = CustomLibrary::migrate(&CustomSettings::default());
        library.validate()?;
        let id = library.save(Entry {
            id: 0,
            name: "新指标".into(),
            enabled: true,
            kind: EntryKind::Script {
                source: STARTER_SOURCE.into(),
            },
        })?;
        let before = library.clone();
        assert!(
            library
                .save(Entry {
                    id,
                    name: "错误".into(),
                    enabled: true,
                    kind: EntryKind::Script {
                        source: "plot(unknown);".into()
                    }
                })
                .is_err()
        );
        assert_eq!(library, before);
        for id in [1, 2, id] {
            library.remove(id);
        }
        let settings = crate::chart_settings::ChartDisplaySettings {
            custom_library: Some(library),
            ..Default::default()
        };
        let loaded: crate::chart_settings::ChartDisplaySettings =
            serde_json::from_str(&serde_json::to_string(&settings)?)?;
        assert!(
            loaded
                .custom_library
                .as_ref()
                .is_some_and(|l| l.entries.is_empty())
        );
        assert!(!loaded.effective_custom_legacy().enabled);
        Ok(())
    }
    #[test]
    fn migration_preserves_existing_parameters() {
        let mut old = CustomSettings::default();
        old.enabled = true;
        old.parameters.ema_periods = [5, 13, 34];
        let library = CustomLibrary::migrate(&old);
        assert_eq!(library.legacy(), old);
        assert!(library.scripts().is_empty());
    }
    #[test]
    fn supplied_source_runs_both_tunnel_colors_and_td_labels()
    -> Result<(), Box<dyn std::error::Error>> {
        use rust_decimal::Decimal;
        use venue_domain::{FieldState, Price, PublicBar};
        let mut engine = ScriptEngine::compile(&ScriptSpec {
            id: 2,
            source: INTRADAY_SOURCE.into(),
        })?;
        let (mut green, mut red, mut labels) = (false, false, 0);
        for i in 0..600u64 {
            let value = if i < 300 {
                1000 + i as i64
            } else {
                1600 - i as i64
            };
            let price = Price::new(Decimal::from(value))?;
            let bar = PublicBar {
                symbol: "BTC/USDT".parse()?,
                generation: 1,
                received_at_ms: (i + 1) * 60_000,
                sequence: i + 1,
                open_time_ms: i * 60_000,
                close_time_ms: (i + 1) * 60_000 - 1,
                interval_ms: 60_000,
                open: price,
                high: price,
                low: price,
                close: price,
                base_volume: FieldState::Known(10.into()),
                quote_volume: FieldState::Known((value * 10).into()),
                trade_count: FieldState::Known(1),
                taker_buy_base_volume: FieldState::Known(5.into()),
                taker_buy_quote_volume: FieldState::Known((value * 5).into()),
            };
            let frame = engine.update(&bar);
            assert_eq!(frame.lines.len(), 6);
            assert_eq!(frame.fills.len(), 2);
            let g = frame.lines[2].value.is_some();
            let r = frame.lines[4].value.is_some();
            assert!(!(g && r));
            green |= g;
            red |= r;
            labels += frame.labels.len();
        }
        assert!(green && red);
        assert!(labels > 0);
        Ok(())
    }
}
