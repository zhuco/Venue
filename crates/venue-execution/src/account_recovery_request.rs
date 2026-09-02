use venue_gateway_api::GatewayBinding;

use super::{AccountHostValidationError, AccountRecoveryRequest, AccountSymbolSet};

impl AccountRecoveryRequest {
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
