use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use venue_control_protocol::UiAccountScope;

#[derive(Clone, Default)]
pub(super) struct StreamGates(Arc<Mutex<StreamGateState>>);

#[derive(Default)]
struct StreamGateState {
    selected_only: bool,
    desired: BTreeSet<UiAccountScope>,
    open: BTreeSet<UiAccountScope>,
    running: BTreeSet<UiAccountScope>,
}

impl StreamGates {
    pub(super) fn reconcile(&self, scopes: BTreeSet<UiAccountScope>) {
        if let Ok(mut state) = self.0.lock() {
            if state.selected_only {
                return;
            }
            state.desired = scopes;
            let desired = state.desired.clone();
            state.open.retain(|scope| desired.contains(scope));
        }
    }

    // Snapshot responses may arrive after an account switch. They must never restore
    // subscriptions or write gates for accounts that are no longer selected.
    pub(super) fn select(&self, scope: Option<UiAccountScope>) {
        if let Ok(mut state) = self.0.lock() {
            state.selected_only = true;
            state.desired = scope.into_iter().collect();
            let desired = state.desired.clone();
            state.open.retain(|scope| desired.contains(scope));
        }
    }

    pub(super) fn desired(&self) -> BTreeSet<UiAccountScope> {
        self.0
            .lock()
            .map(|state| state.desired.clone())
            .unwrap_or_default()
    }

    pub(super) fn is_desired(&self, scope: &UiAccountScope) -> bool {
        self.0
            .lock()
            .is_ok_and(|state| state.desired.contains(scope))
    }

    pub(super) fn try_start(&self, scope: &UiAccountScope) -> bool {
        self.0.lock().is_ok_and(|mut state| {
            state.desired.contains(scope) && state.running.insert(scope.clone())
        })
    }

    pub(super) fn opened(&self, scope: &UiAccountScope) {
        if let Ok(mut state) = self.0.lock()
            && state.desired.contains(scope)
        {
            state.open.insert(scope.clone());
        }
    }

    pub(super) fn closed(&self, scope: &UiAccountScope) {
        if let Ok(mut state) = self.0.lock() {
            state.open.remove(scope);
        }
    }

    pub(super) fn finished(&self, scope: &UiAccountScope) {
        if let Ok(mut state) = self.0.lock() {
            state.open.remove(scope);
            state.running.remove(scope);
        }
    }

    pub(super) fn is_open(&self, scope: &UiAccountScope) -> bool {
        self.0.lock().is_ok_and(|state| state.open.contains(scope))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_control_protocol::{GatewayMode, VenueId};

    fn scope(id: &str) -> UiAccountScope {
        UiAccountScope {
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: id.into(),
        }
    }

    #[test]
    fn selected_account_excludes_other_streams_and_stale_snapshots() {
        let gates = StreamGates::default();
        let first = scope("00000000-0000-4000-8000-000000000001");
        let second = scope("00000000-0000-4000-8000-000000000002");
        let all = [first.clone(), second.clone()].into_iter().collect();
        gates.select(None);
        gates.reconcile(all);
        assert!(gates.desired().is_empty());
        gates.select(Some(first.clone()));
        assert!(gates.try_start(&first));
        gates.opened(&first);
        assert!(!gates.try_start(&second));
        gates.select(Some(second.clone()));
        assert!(!gates.is_desired(&first));
        assert!(!gates.is_open(&first));
        gates.opened(&first);
        assert!(!gates.is_open(&first));
        gates.reconcile([first.clone(), second.clone()].into_iter().collect());
        assert_eq!(gates.desired(), [second.clone()].into_iter().collect());
        assert!(gates.try_start(&second));
        gates.opened(&second);
        assert!(gates.is_open(&second));
        gates.finished(&first);
        assert!(gates.is_open(&second));
        gates.select(None);
        assert!(!gates.is_desired(&second));
        assert!(!gates.is_open(&second));
    }
}
