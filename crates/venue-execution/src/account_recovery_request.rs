use venue_gateway_api::GatewayBinding;

use super::{AccountHostValidationError, AccountRecoveryRequest, AccountSymbolSet};

impl AccountRecoveryRequest {
    /// Exact signed queries for durable PostgreSQL commands. This grants no mutation authority
    /// and does not import a WAL cursor or create recovery artifacts.
    pub fn for_durable_commands(
        binding: GatewayBinding,
        commands: Vec<venue_domain::domain::ExecutionCommand>,
    ) -> Result<Self, AccountHostValidationError> {
        if commands.is_empty()
            || commands
                .iter()
                .any(|command| !crate::validate_durable_command(&binding, command))
        {
            return Err(AccountHostValidationError::Scope);
        }
        let symbols = AccountSymbolSet::single(&binding);
        let mut request = Self::read_only(binding, symbols, None)?;
        request.unresolved = commands;
        Ok(request)
    }
    pub fn read_only(
        binding: GatewayBinding,
        configured_symbols: AccountSymbolSet,
        previous_fills_cursor: Option<String>,
    ) -> Result<Self, AccountHostValidationError> {
        binding
            .validate()
            .map_err(|_| AccountHostValidationError::Scope)?;
        if !configured_symbols.contains(&binding.symbol)
            || previous_fills_cursor
                .as_deref()
                .is_some_and(|cursor| cursor.trim().is_empty())
        {
            return Err(AccountHostValidationError::Scope);
        }
        Ok(Self {
            binding,
            configured_symbols,
            unresolved: Vec::new(),
            previous_fills_cursor,
        })
    }
}
