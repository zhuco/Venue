use eframe::egui::Color32;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct IndicatorStyle {
    pub enabled: bool,
    pub color: [u8; 3],
    pub secondary_color: [u8; 3],
    pub line_width_tenths: u8,
}

impl IndicatorStyle {
    pub const fn new(enabled: bool, color: [u8; 3], secondary_color: [u8; 3]) -> Self {
        Self {
            enabled,
            color,
            secondary_color,
            line_width_tenths: 13,
        }
    }

    pub const fn color(self) -> Color32 {
        Color32::from_rgb(self.color[0], self.color[1], self.color[2])
    }

    pub const fn secondary_color(self) -> Color32 {
        Color32::from_rgb(
            self.secondary_color[0],
            self.secondary_color[1],
            self.secondary_color[2],
        )
    }

    pub fn line_width(self) -> f32 {
        f32::from(self.line_width_tenths.clamp(5, 40)) / 10.0
    }
}

impl Default for IndicatorStyle {
    fn default() -> Self {
        Self::new(true, [240, 185, 11], [132, 142, 156])
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ChartDisplaySettings {
    pub sma_period: u32,
    pub ema_period: u32,
    pub bollinger_period: u32,
    pub bollinger_multiplier_hundredths: u32,
    pub rsi_period: u32,
    pub macd_fast_period: u32,
    pub macd_slow_period: u32,
    pub macd_signal_period: u32,
    pub atr_period: u32,
    pub sma: IndicatorStyle,
    pub ema: IndicatorStyle,
    pub bollinger: IndicatorStyle,
    pub vwap: IndicatorStyle,
    pub rsi: IndicatorStyle,
    pub macd: IndicatorStyle,
    pub atr: IndicatorStyle,
    pub show_volume: bool,
    pub chart_text_size: u8,
}

impl Default for ChartDisplaySettings {
    fn default() -> Self {
        Self {
            sma_period: 20,
            ema_period: 20,
            bollinger_period: 20,
            bollinger_multiplier_hundredths: 200,
            rsi_period: 14,
            macd_fast_period: 12,
            macd_slow_period: 26,
            macd_signal_period: 9,
            atr_period: 14,
            sma: IndicatorStyle::new(true, [240, 185, 11], [240, 185, 11]),
            ema: IndicatorStyle::new(true, [214, 54, 160], [214, 54, 160]),
            bollinger: IndicatorStyle::new(true, [155, 116, 255], [102, 84, 170]),
            vwap: IndicatorStyle::new(true, [14, 203, 129], [14, 203, 129]),
            rsi: IndicatorStyle::new(true, [171, 103, 255], [91, 69, 122]),
            macd: IndicatorStyle::new(true, [240, 185, 11], [14, 203, 129]),
            atr: IndicatorStyle::new(false, [91, 159, 255], [91, 159, 255]),
            show_volume: true,
            chart_text_size: 11,
        }
    }
}

impl ChartDisplaySettings {
    pub fn validate(&self) -> Result<(), &'static str> {
        let periods = [
            self.sma_period,
            self.ema_period,
            self.bollinger_period,
            self.rsi_period,
            self.macd_fast_period,
            self.macd_slow_period,
            self.macd_signal_period,
            self.atr_period,
        ];
        if periods.iter().any(|period| !(1..=100_000).contains(period)) {
            return Err("指标周期必须在 1 到 100000 之间");
        }
        if self.macd_fast_period >= self.macd_slow_period {
            return Err("MACD 快线周期必须小于慢线周期");
        }
        if !(1..=100_000).contains(&self.bollinger_multiplier_hundredths) {
            return Err("BOLL 标准差倍数必须大于 0");
        }
        if !(9..=16).contains(&self.chart_text_size) {
            return Err("图表文字大小必须在 9 到 16 之间");
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn enabled_study_count(&self) -> usize {
        [
            self.sma.enabled,
            self.ema.enabled,
            self.bollinger.enabled,
            self.vwap.enabled,
            self.rsi.enabled,
            self.macd.enabled,
            self.atr.enabled,
        ]
        .into_iter()
        .filter(|enabled| *enabled)
        .count()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn engine_config(&self) -> venue_indicators::chart::ChartStudyConfig {
        venue_indicators::chart::ChartStudyConfig {
            sma_period: self.sma_period as usize,
            ema_period: self.ema_period as usize,
            bollinger_period: self.bollinger_period as usize,
            bollinger_multiplier: rust_decimal::Decimal::new(
                i64::from(self.bollinger_multiplier_hundredths),
                2,
            ),
            rsi_period: self.rsi_period as usize,
            macd_fast_period: self.macd_fast_period as usize,
            macd_slow_period: self.macd_slow_period as usize,
            macd_signal_period: self.macd_signal_period as usize,
            atr_period: self.atr_period as usize,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ChartDisplaySettings;

    #[test]
    fn defaults_match_the_commercial_chart_profile() {
        let settings = ChartDisplaySettings::default();
        assert!(settings.validate().is_ok());
        assert_eq!(settings.enabled_study_count(), 6);
        assert!(!settings.atr.enabled);
    }

    #[test]
    fn invalid_macd_periods_are_rejected_before_recalculation() {
        let mut settings = ChartDisplaySettings::default();
        settings.macd_fast_period = settings.macd_slow_period;
        assert!(settings.validate().is_err());
    }
}
