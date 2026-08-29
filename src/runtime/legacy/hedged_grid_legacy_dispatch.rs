use std::thread;

use crate::{
    exchange::binance::{PrivateError, PrivateRest},
    execution::{CommandJournal, CommandState},
};

use super::{GridMutation, HedgedGridLiveError, settle_mutation};

/// Injection seam for the legacy Binance runtime's physical mutation boundary. Production still
/// delegates to the same `PrivateRest`; offline equivalence tests replace only the network call
/// while retaining the legacy planner, WAL transitions, response parser, and reducer settlement.
pub(in crate::runtime) trait LegacyGridMutationEndpoint: Sync {
    fn submit(&self, mutation: &GridMutation) -> Result<String, PrivateError>;
}

pub(super) struct BinanceLegacyGridMutationEndpoint<'a> {
    pub(super) private: &'a PrivateRest,
}

impl LegacyGridMutationEndpoint for BinanceLegacyGridMutationEndpoint<'_> {
    fn submit(&self, mutation: &GridMutation) -> Result<String, PrivateError> {
        mutation.submit(self.private)
    }
}

pub(super) fn dispatch_mutations_with_endpoint<E: LegacyGridMutationEndpoint>(
    commands: &mut CommandJournal,
    endpoint: &E,
    mutations: Vec<GridMutation>,
) -> Result<(), HedgedGridLiveError> {
    if mutations.is_empty() {
        return Ok(());
    }
    for mutation in &mutations {
        mutation.prepare(commands)?;
    }
    for mutation in &mutations {
        commands.transition(mutation.command_id(), CommandState::Submitted)?;
    }
    let outcomes = thread::scope(|scope| {
        let handles = mutations
            .iter()
            .cloned()
            .map(|mutation| scope.spawn(move || (mutation.clone(), endpoint.submit(&mutation))))
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().map_err(|_| HedgedGridLiveError::Dispatch))
            .collect::<Result<Vec<_>, _>>()
    })?;
    let mut dispatch_error = None;
    for (mutation, outcome) in outcomes {
        if let Err(error) = settle_mutation(commands, mutation, outcome)
            && dispatch_error.is_none()
        {
            dispatch_error = Some(error);
        }
    }
    if commands.has_unresolved() {
        return Err(HedgedGridLiveError::Unresolved);
    }
    if let Some(error) = dispatch_error {
        return Err(error);
    }
    Ok(())
}
