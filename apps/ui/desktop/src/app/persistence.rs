use super::{PersistedState, STORAGE_KEY, migrate_persisted_state};

const BACKUP_KEY: &str = "venueflow-state-last-good";

pub(super) fn load(storage: Option<&dyn eframe::Storage>) -> (PersistedState, bool) {
    let Some(storage) = storage else {
        return (PersistedState::default(), false);
    };
    let current = storage.get_string(STORAGE_KEY);
    if let Some(state) = current.as_deref().and_then(decode) {
        return (state, false);
    }
    if let Some(state) = storage.get_string(BACKUP_KEY).as_deref().and_then(decode) {
        return (state, true);
    }
    (PersistedState::default(), current.is_some())
}

fn decode(encoded: &str) -> Option<PersistedState> {
    serde_json::from_str::<PersistedState>(encoded)
        .ok()
        .map(migrate_persisted_state)
}

pub(super) fn save(
    storage: &mut dyn eframe::Storage,
    state: &PersistedState,
) -> Result<(), serde_json::Error> {
    let encoded = serde_json::to_string(state)?;
    // Keep only a previously decodable UI preference/layout snapshot, never a bad payload.
    if let Some(previous) = storage
        .get_string(STORAGE_KEY)
        .filter(|value| decode(value).is_some())
    {
        storage.set_string(BACKUP_KEY, previous);
    }
    storage.set_string(STORAGE_KEY, encoded);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use eframe::Storage;
    #[derive(Default)]
    struct Memory(std::collections::HashMap<String, String>);
    impl Storage for Memory {
        fn get_string(&self, key: &str) -> Option<String> {
            self.0.get(key).cloned()
        }
        fn set_string(&mut self, key: &str, value: String) {
            self.0.insert(key.into(), value);
        }
        fn remove_string(&mut self, key: &str) {
            self.0.remove(key);
        }
        fn flush(&mut self) {}
    }
    #[test]
    fn corrupt_current_recovers_last_good_without_overwriting_it() -> Result<(), serde_json::Error>
    {
        let mut storage = Memory::default();
        let mut state = PersistedState::default();
        state.preferences.selected_symbol = "ETH/USDT".into();
        save(&mut storage, &state)?;
        save(&mut storage, &PersistedState::default())?;
        storage.set_string(STORAGE_KEY, "broken".into());
        let (restored, recovered) = load(Some(&storage));
        assert!(recovered);
        assert_eq!(restored.preferences.selected_symbol, "ETH/USDT");
        let backup = storage.get_string(BACKUP_KEY);
        save(&mut storage, &restored)?;
        assert_eq!(storage.get_string(BACKUP_KEY), backup);
        assert!(!load(Some(&storage)).1);
        Ok(())
    }
}
