use crate::{
    domain::{
        CancelCommand, CommandId, ExecutionCommand, MarketOrderCommand, MarketReduceCommand,
        OrderCommand,
    },
    exchange::grid::{GridVenueError, HedgedGridMutationClient},
    execution::{CommandJournal, CommandJournalError},
};

use super::{GridMutation, Stage7GridError};

#[derive(Clone)]
pub(super) enum Stage7Mutation {
    Place(OrderCommand),
    Market(MarketOrderCommand),
    Reduce(MarketReduceCommand),
    Cancel(CancelCommand),
}

impl Stage7Mutation {
    pub(super) fn from_grid(value: GridMutation) -> Self {
        match value {
            GridMutation::Place(command) => Self::Place(command),
            GridMutation::Market(command) => Self::Market(command),
            GridMutation::Reduce(command) => Self::Reduce(command),
            GridMutation::Cancel(command) => Self::Cancel(command),
        }
    }

    pub(super) fn from_execution(value: ExecutionCommand) -> Result<Self, Stage7GridError> {
        match value {
            ExecutionCommand::PlaceLimit(command) => Ok(Self::Place(command)),
            ExecutionCommand::PlaceMarket(command) => Ok(Self::Market(command)),
            ExecutionCommand::MarketReduce(command) => Ok(Self::Reduce(command)),
            ExecutionCommand::Cancel(command) => Ok(Self::Cancel(command)),
            ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_) => Err(Stage7GridError::JournalScope),
        }
    }

    pub(super) fn command_id(&self) -> &CommandId {
        match self {
            Self::Place(command) => &command.command_id,
            Self::Market(command) => &command.command_id,
            Self::Reduce(command) => &command.command_id,
            Self::Cancel(command) => &command.command_id,
        }
    }

    pub(super) fn client_order_id(&self) -> &str {
        match self {
            Self::Place(command) => command.client_order_id.as_str(),
            Self::Market(command) => command.client_order_id.as_str(),
            Self::Reduce(command) => command.client_order_id.as_str(),
            Self::Cancel(command) => command.target_client_order_id.as_str(),
        }
    }

    pub(super) fn execution_command(&self) -> ExecutionCommand {
        match self {
            Self::Place(command) => ExecutionCommand::PlaceLimit(command.clone()),
            Self::Market(command) => ExecutionCommand::PlaceMarket(command.clone()),
            Self::Reduce(command) => ExecutionCommand::MarketReduce(command.clone()),
            Self::Cancel(command) => ExecutionCommand::Cancel(command.clone()),
        }
    }

    pub(super) fn prepare(&self, commands: &mut CommandJournal) -> Result<(), CommandJournalError> {
        match self {
            Self::Place(command) => commands.prepare_place(command.clone()).map(|_| ()),
            Self::Market(command) => commands.prepare_market(command.clone()).map(|_| ()),
            Self::Reduce(command) => commands.prepare_market_reduce(command.clone()).map(|_| ()),
            Self::Cancel(command) => commands.prepare_cancel(command.clone()).map(|_| ()),
        }
    }

    pub(super) fn submit(
        &self,
        client: &dyn HedgedGridMutationClient,
    ) -> Result<String, GridVenueError> {
        match self {
            Self::Place(command) => client.place_limit_post_only(command),
            Self::Market(command) => client.place_market(command),
            Self::Reduce(command) => client.place_market_reduce(command),
            Self::Cancel(command) => client.cancel_by_client_id(command),
        }
    }
}
