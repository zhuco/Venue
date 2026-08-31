use eframe::egui::Color32;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct IndicatorStyle {
    pub enabled: bool,
    pub color: [u8; 3],
    pub secondary_color: [u8; 3],
    pub tertiary_color: [u8; 3],
    pub line_width_tenths: u8,
    pub line_enabled: [bool; 3],
    pub background_enabled: bool,
    pub secondary_background_enabled: bool,
    pub fill_opacity_percent: u8,
    pub histogram_colors: [[u8; 3]; 2],
}

impl IndicatorStyle {
    pub const fn new(enabled: bool, color: [u8; 3], secondary_color: [u8; 3]) -> Self {
        Self {
            enabled,
            color,
            secondary_color,
            tertiary_color: [159, 122, 234],
            line_width_tenths: 12,
            line_enabled: [true, true, true],
            background_enabled: false,
            secondary_background_enabled: false,
            fill_opacity_percent: 12,
            histogram_colors: [[14, 203, 129], [246, 70, 93]],
        }
    }

    pub const fn with_tertiary(mut self, color: [u8; 3]) -> Self {
        self.tertiary_color = color;
        self
    }

    pub const fn with_fill(mut self, opacity: u8, both_directions: bool) -> Self {
        self.background_enabled = true;
        self.secondary_background_enabled = both_directions;
        self.fill_opacity_percent = opacity;
        self
    }

    pub fn fill_color(self, color: Color32) -> Color32 {
        let alpha = u16::from(self.fill_opacity_percent.min(40)) * 255 / 100;
        Color32::from_rgba_unmultiplied(color.r(), color.g(), color.b(), alpha as u8)
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

    pub const fn tertiary_color(self) -> Color32 {
        Color32::from_rgb(
            self.tertiary_color[0],
            self.tertiary_color[1],
            self.tertiary_color[2],
        )
    }

    pub fn line_width(self) -> f32 {
        f32::from(self.line_width_tenths.clamp(5, 40)) / 10.0
    }
}

impl Default for IndicatorStyle {
    fn default() -> Self {
        Self::new(false, [240, 185, 11], [214, 54, 160])
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ChartDisplaySettings {
    pub custom_ema_adx: crate::custom_indicator::CustomSettings,
    pub ma_periods: [u32; 3],
    pub ema_periods: [u32; 3],
    pub wma_periods: [u32; 3],
    pub bollinger_period: u32,
    pub bollinger_multiplier_hundredths: u32,
    pub trix_period: u32,
    pub sar_step_ten_thousandths: u32,
    pub sar_maximum_ten_thousandths: u32,
    pub supertrend_period: u32,
    pub supertrend_multiplier_hundredths: u32,
    pub rsi_period: u32,
    pub mfi_period: u32,
    pub kdj_period: u32,
    pub kdj_signal_period: u32,
    pub cci_period: u32,
    pub stoch_rsi_period: u32,
    pub stoch_rsi_stochastic_period: u32,
    pub stoch_rsi_signal_period: u32,
    pub williams_r_period: u32,
    pub dmi_period: u32,
    pub momentum_period: u32,
    pub emv_period: u32,
    pub macd_fast_period: u32,
    pub macd_slow_period: u32,
    pub macd_signal_period: u32,
    pub atr_period: u32,
    pub ma: IndicatorStyle,
    pub ema: IndicatorStyle,
    pub wma: IndicatorStyle,
    pub bollinger: IndicatorStyle,
    pub vwap: IndicatorStyle,
    pub avl: IndicatorStyle,
    pub trix: IndicatorStyle,
    pub sar: IndicatorStyle,
    pub supertrend: IndicatorStyle,
    pub volume: IndicatorStyle,
    pub macd: IndicatorStyle,
    pub rsi: IndicatorStyle,
    pub mfi: IndicatorStyle,
    pub kdj: IndicatorStyle,
    pub obv: IndicatorStyle,
    pub cci: IndicatorStyle,
    pub stoch_rsi: IndicatorStyle,
    pub williams_r: IndicatorStyle,
    pub dmi: IndicatorStyle,
    pub momentum: IndicatorStyle,
    pub emv: IndicatorStyle,
    pub atr: IndicatorStyle,
    pub chart_text_size: u8,
}

impl Default for ChartDisplaySettings {
    fn default() -> Self {
        Self {
            custom_ema_adx: crate::custom_indicator::CustomSettings::default(),
            ma_periods: [7, 25, 99],
            ema_periods: [7, 25, 99],
            wma_periods: [7, 25, 99],
            bollinger_period: 20,
            bollinger_multiplier_hundredths: 200,
            trix_period: 12,
            sar_step_ten_thousandths: 200,
            sar_maximum_ten_thousandths: 2_000,
            supertrend_period: 10,
            supertrend_multiplier_hundredths: 300,
            rsi_period: 14,
            mfi_period: 14,
            kdj_period: 9,
            kdj_signal_period: 3,
            cci_period: 20,
            stoch_rsi_period: 14,
            stoch_rsi_stochastic_period: 14,
            stoch_rsi_signal_period: 3,
            williams_r_period: 14,
            dmi_period: 14,
            momentum_period: 10,
            emv_period: 14,
            macd_fast_period: 12,
            macd_slow_period: 26,
            macd_signal_period: 9,
            atr_period: 14,
            ma: IndicatorStyle::new(true, [240, 185, 11], [214, 54, 160])
                .with_tertiary([159, 122, 234]),
            ema: IndicatorStyle::new(false, [246, 201, 74], [90, 200, 250])
                .with_tertiary([214, 54, 160]),
            wma: IndicatorStyle::new(false, [253, 138, 0], [89, 189, 255])
                .with_tertiary([154, 106, 255]),
            bollinger: IndicatorStyle::new(false, [183, 138, 247], [235, 47, 166])
                .with_fill(6, false),
            vwap: IndicatorStyle::new(false, [14, 203, 129], [14, 203, 129]),
            avl: IndicatorStyle::new(false, [240, 185, 11], [240, 185, 11]),
            trix: IndicatorStyle::new(false, [91, 159, 255], [91, 159, 255]),
            sar: IndicatorStyle::new(false, [14, 203, 129], [246, 70, 93]),
            supertrend: IndicatorStyle::new(false, [14, 203, 129], [246, 70, 93])
                .with_fill(12, true),
            volume: IndicatorStyle::new(true, [14, 203, 129], [246, 70, 93]),
            macd: IndicatorStyle::new(true, [240, 185, 11], [235, 47, 166]),
            rsi: IndicatorStyle::new(false, [171, 103, 255], [91, 69, 122]),
            mfi: IndicatorStyle::new(false, [91, 159, 255], [91, 159, 255]),
            kdj: IndicatorStyle::new(false, [240, 185, 11], [214, 54, 160])
                .with_tertiary([159, 122, 234]),
            obv: IndicatorStyle::new(false, [14, 203, 129], [14, 203, 129]),
            cci: IndicatorStyle::new(false, [240, 185, 11], [240, 185, 11]),
            stoch_rsi: IndicatorStyle::new(false, [14, 203, 129], [214, 54, 160]),
            williams_r: IndicatorStyle::new(false, [159, 122, 234], [159, 122, 234]),
            dmi: IndicatorStyle::new(false, [14, 203, 129], [246, 70, 93])
                .with_tertiary([236, 239, 244]),
            momentum: IndicatorStyle::new(false, [91, 159, 255], [91, 159, 255]),
            emv: IndicatorStyle::new(false, [14, 203, 129], [14, 203, 129]),
            atr: IndicatorStyle::new(false, [91, 159, 255], [91, 159, 255]),
            chart_text_size: 11,
        }
    }
}

impl ChartDisplaySettings {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.custom_ema_adx
            .parameters
            .validate()
            .map_err(|_| "自定义指标参数无效 / Invalid custom indicator parameters")?;
        let periods = self
            .ma_periods
            .into_iter()
            .chain(self.ema_periods)
            .chain(self.wma_periods)
            .chain([
                self.bollinger_period,
                self.trix_period,
                self.supertrend_period,
                self.rsi_period,
                self.mfi_period,
                self.kdj_period,
                self.kdj_signal_period,
                self.cci_period,
                self.stoch_rsi_period,
                self.stoch_rsi_stochastic_period,
                self.stoch_rsi_signal_period,
                self.williams_r_period,
                self.dmi_period,
                self.momentum_period,
                self.emv_period,
                self.macd_fast_period,
                self.macd_slow_period,
                self.macd_signal_period,
                self.atr_period,
            ]);
        if periods
            .into_iter()
            .any(|period| !(1..=100_000).contains(&period))
        {
            return Err("指标周期必须在 1 到 100000 之间");
        }
        if self.macd_fast_period >= self.macd_slow_period {
            return Err("MACD 快线周期必须小于慢线周期");
        }
        if self.bollinger_multiplier_hundredths == 0
            || self.supertrend_multiplier_hundredths == 0
            || self.sar_step_ten_thousandths == 0
            || self.sar_maximum_ten_thousandths < self.sar_step_ten_thousandths
        {
            return Err("指标倍数与加速参数必须为正且上下限有效");
        }
        if !(9..=16).contains(&self.chart_text_size) {
            return Err("图表文字大小必须在 9 到 16 之间");
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn enabled_study_count(&self) -> usize {
        self.styles()
            .into_iter()
            .filter(|style| style.enabled)
            .count()
    }

    #[cfg(test)]
    pub fn styles(&self) -> [&IndicatorStyle; 22] {
        [
            &self.ma,
            &self.ema,
            &self.wma,
            &self.bollinger,
            &self.vwap,
            &self.avl,
            &self.trix,
            &self.sar,
            &self.supertrend,
            &self.volume,
            &self.macd,
            &self.rsi,
            &self.mfi,
            &self.kdj,
            &self.obv,
            &self.cci,
            &self.stoch_rsi,
            &self.williams_r,
            &self.dmi,
            &self.momentum,
            &self.emv,
            &self.atr,
        ]
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn engine_config(&self) -> venue_indicators::chart::ChartStudyConfig {
        venue_indicators::chart::ChartStudyConfig {
            sma_period: self.ma_periods[0] as usize,
            sma_second_period: self.ma_periods[1] as usize,
            sma_third_period: self.ma_periods[2] as usize,
            ema_period: self.ema_periods[0] as usize,
            ema_second_period: self.ema_periods[1] as usize,
            ema_third_period: self.ema_periods[2] as usize,
            wma_period: self.wma_periods[0] as usize,
            wma_second_period: self.wma_periods[1] as usize,
            wma_third_period: self.wma_periods[2] as usize,
            bollinger_period: self.bollinger_period as usize,
            bollinger_multiplier: rust_decimal::Decimal::new(
                i64::from(self.bollinger_multiplier_hundredths),
                2,
            ),
            trix_period: self.trix_period as usize,
            sar_step: rust_decimal::Decimal::new(i64::from(self.sar_step_ten_thousandths), 4),
            sar_maximum: rust_decimal::Decimal::new(i64::from(self.sar_maximum_ten_thousandths), 4),
            supertrend_period: self.supertrend_period as usize,
            supertrend_multiplier: rust_decimal::Decimal::new(
                i64::from(self.supertrend_multiplier_hundredths),
                2,
            ),
            rsi_period: self.rsi_period as usize,
            mfi_period: self.mfi_period as usize,
            kdj_period: self.kdj_period as usize,
            kdj_signal_period: self.kdj_signal_period as usize,
            cci_period: self.cci_period as usize,
            stoch_rsi_period: self.stoch_rsi_period as usize,
            stoch_rsi_stochastic_period: self.stoch_rsi_stochastic_period as usize,
            stoch_rsi_signal_period: self.stoch_rsi_signal_period as usize,
            williams_r_period: self.williams_r_period as usize,
            dmi_period: self.dmi_period as usize,
            momentum_period: self.momentum_period as usize,
            emv_period: self.emv_period as usize,
            macd_fast_period: self.macd_fast_period as usize,
            macd_slow_period: self.macd_slow_period as usize,
            macd_signal_period: self.macd_signal_period as usize,
            atr_period: self.atr_period as usize,
            custom_ema_adx: self
                .custom_ema_adx
                .enabled
                .then(|| self.custom_ema_adx.parameters.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ChartDisplaySettings;

    #[test]
    fn defaults_expose_the_confirmed_commercial_indicator_set() {
        let settings = ChartDisplaySettings::default();
        assert!(settings.validate().is_ok());
        assert_eq!(settings.enabled_study_count(), 3);
        assert_eq!(settings.ma_periods, [7, 25, 99]);
    }

    #[test]
    fn invalid_macd_and_sar_parameters_are_rejected() {
        let mut settings = ChartDisplaySettings::default();
        settings.macd_fast_period = settings.macd_slow_period;
        assert!(settings.validate().is_err());
        settings = ChartDisplaySettings::default();
        settings.sar_maximum_ten_thousandths = 1;
        assert!(settings.validate().is_err());
    }
}
