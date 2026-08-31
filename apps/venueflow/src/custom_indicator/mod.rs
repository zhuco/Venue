mod render;
mod text;
mod ui;

pub(crate) use render::draw;
pub(crate) use ui::settings_ui;

use serde::{Deserialize, Serialize};
use venue_indicators::chart::EmaAdxConfig;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CustomSettings {
    pub enabled: bool,
    pub parameters: EmaAdxConfig,
    pub colors: [[u8; 3]; 3],
    pub line_widths: [u8; 3],
    pub signals: [bool; 6],
    pub confirmed_labels_only: bool,
}

impl Default for CustomSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            parameters: EmaAdxConfig::default(),
            colors: [[0, 230, 118], [255, 23, 68], [158, 158, 158]],
            line_widths: [2, 3, 3],
            signals: [true; 6],
            confirmed_labels_only: true,
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn settings_roundtrip_and_legacy_preferences() -> Result<(), Box<dyn std::error::Error>> {
        let old: crate::chart_settings::ChartDisplaySettings = serde_json::from_str("{}")?;
        assert!(!old.custom_ema_adx.enabled);
        let mut changed = old;
        changed.custom_ema_adx.enabled = true;
        changed.custom_ema_adx.parameters.ema_periods = [5, 13, 34];
        changed.custom_ema_adx.signals[0] = false;
        let loaded = serde_json::from_str::<crate::chart_settings::ChartDisplaySettings>(
            &serde_json::to_string(&changed)?,
        )?;
        assert_eq!(changed, loaded);
        assert!(changed.validate().is_ok());
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn enable_disable_routes_only_selected_configuration() {
        let mut settings = crate::chart_settings::ChartDisplaySettings::default();
        assert!(settings.engine_config().custom_ema_adx.is_none());
        settings.custom_ema_adx.enabled = true;
        assert_eq!(
            settings.engine_config().custom_ema_adx,
            Some(settings.custom_ema_adx.parameters.clone())
        );
        settings.custom_ema_adx.enabled = false;
        assert!(settings.engine_config().custom_ema_adx.is_none());
    }
}
