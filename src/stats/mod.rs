//! Dedicated stats task: registry mirror, interval timer, and JSON-Lines
//! stdout statistics sink.

mod handle;
mod line;
mod queue;
mod registry;
mod task;
mod writer;

#[cfg(test)]
pub(crate) use handle::StatsEvent;
pub use handle::{StatsHandle, StatsInbox, channel};
pub use task::{DEFAULT_STATS_QUEUE_LINES, StatsRunConfig, run};
