use std::collections::BTreeSet;

use venue_domain::domain::Symbol;
use venue_gateway_api::GatewayBinding;

use crate::AccountHostValidationError;

/// The exact symbols a single account writer is allowed to observe and mutate.  It is a Host
/// boundary, not a second account identity: every member still shares one lock and command WAL.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountSymbolSet {
    symbols: BTreeSet<Symbol>,
}

impl AccountSymbolSet {
    pub fn new(
        binding: &GatewayBinding,
        symbols: impl IntoIterator<Item = Symbol>,
    ) -> Result<Self, AccountHostValidationError> {
        let symbols = symbols.into_iter().collect::<BTreeSet<_>>();
        if symbols.is_empty() || !symbols.contains(&binding.symbol) {
            return Err(AccountHostValidationError::Scope);
        }
        Ok(Self { symbols })
    }

    pub fn single(binding: &GatewayBinding) -> Self {
        Self {
            symbols: BTreeSet::from([binding.symbol.clone()]),
        }
    }

    #[must_use]
    pub fn contains(&self, symbol: &Symbol) -> bool {
        self.symbols.contains(symbol)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Symbol> {
        self.symbols.iter()
    }
}
