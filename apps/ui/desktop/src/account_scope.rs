use crate::{client::ClientEvent, model::AppModel};
use venue_control_protocol::VenueId;

/// Desktop selection identity, deliberately separate from the durable command protocol.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AccountScope {
    pub generation: u64,
    pub credential_id: String,
    pub trading_account_id: String,
    pub venue: VenueId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Scoped<T> {
    pub scope: AccountScope,
    pub value: T,
}

impl AccountScope {
    pub fn event(&self, event: ClientEvent) -> ClientEvent {
        ClientEvent::AccountScoped {
            scope: self.clone(),
            event: Box::new(event),
        }
    }

    pub fn accepts(&self, event: &ClientEvent) -> bool {
        match event {
            ClientEvent::TerminalAccountProjection {
                credential_id,
                projection,
            } => {
                credential_id == &self.credential_id
                    && projection.as_ref().is_none_or(|p| {
                        p.credential_id == self.credential_id
                            && p.trading_account_id == self.trading_account_id
                    })
            }
            ClientEvent::TerminalAccountUnavailable { credential_id, .. } => {
                credential_id == &self.credential_id
            }
            ClientEvent::TerminalExecutionUpdated(row) => {
                row.trading_account_id == self.trading_account_id
            }
            _ => true,
        }
    }
}

impl AppModel {
    pub(crate) fn confirmed_account_scope(&self) -> Option<AccountScope> {
        let credential = self.selected_execution_credential()?;
        let account = credential.trading_account_id.as_ref()?;
        if self.preferences.execution_account_id.as_ref() != Some(account) {
            return None;
        }
        Some(AccountScope {
            generation: self.account_generation,
            credential_id: credential.credential_id.clone(),
            trading_account_id: account.clone(),
            venue: credential.venue,
        })
    }

    pub(crate) fn begin_account_selection(&mut self, credential_id: String) {
        self.account_generation = self.account_generation.checked_add(1).unwrap_or(u64::MAX);
        self.account_switch_pending = Some(credential_id.clone());
        self.account_selection_requested = Some(credential_id);
        self.preferences.execution_account_id = None;
        self.preferences.selected_instance = None;
        self.clear_trading_intent();
        self.execution.clear_account_view();
        self.notices.clear();
    }

    pub(crate) fn clear_trading_intent(&mut self) {
        self.trade_dock = Default::default();
        self.pending_confirmation = None;
        self.execution.clear_position_confirmation();
    }

    pub(crate) fn accept_account_event(&self, scope: &AccountScope, event: &ClientEvent) -> bool {
        self.confirmed_account_scope().as_ref() == Some(scope) && scope.accepts(event)
    }
}

impl AppModel {
    /// Returns session expiry only when it belongs to the current selection.
    pub(crate) fn apply_account_event(&mut self, scope: &AccountScope, event: ClientEvent) -> bool {
        if !self.accept_account_event(scope, &event) {
            return false;
        }
        match event {
            ClientEvent::TerminalAccountProjection {
                credential_id,
                projection,
            } => {
                if self
                    .account_overview
                    .as_ref()
                    .and_then(|overview| overview.selected_credential_id.as_deref())
                    == Some(credential_id.as_str())
                {
                    self.execution
                        .apply_private(projection, &mut self.trade_dock);
                }
            }
            ClientEvent::TerminalExecutions(mut executions) => {
                executions.retain(|r| {
                    Some(&r.trading_account_id) == self.preferences.execution_account_id.as_ref()
                });
                self.execution.apply_terminal_executions(executions)
            }
            ClientEvent::TerminalExecutionUpdated(summary) => {
                self.execution.apply_terminal_execution(summary)
            }
            ClientEvent::TerminalExecutionsUnavailable(message) => {
                self.execution.terminal_executions_error = Some(message)
            }
            ClientEvent::TerminalSubmissionUnavailable {
                request_id,
                message,
                definitely_not_submitted,
            } => {
                self.execution
                    .position_submission_failed(&request_id, definitely_not_submitted);
                if self.execution.terminal_request_id.as_deref() == Some(request_id.as_str()) {
                    self.execution.terminal_submission_error = Some(message.clone());
                    self.notice(message);
                }
            }
            ClientEvent::TerminalAccountUnavailable {
                credential_id,
                message,
            } => {
                if self.account_overview.as_ref().is_some_and(|overview| {
                    overview.selected_credential_id.as_ref() == Some(&credential_id)
                }) {
                    self.execution.private_error = Some(message);
                }
            }
            ClientEvent::SessionExpired => return true,
            _ => (),
        }
        false
    }
}

#[cfg(test)]
pub(crate) mod tests;
