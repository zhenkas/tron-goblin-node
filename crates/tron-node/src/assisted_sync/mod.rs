//! Opt-in trusted state and event replication. Never enables implicit trust
//! for ordinary TRON peers; every assisted link requires configured mTLS.
pub mod bootstrap;
mod bootstrap_transport;
mod config;
pub use bootstrap_transport::receive as receive_bootstrap;
pub mod journal;
pub use config::{AssistConfig, AssistPeer};
mod runtime;
pub use runtime::AssistRuntime;

mod transport;
pub use transport::start;

pub(crate) mod processing;

pub(crate) mod cluster;

mod cluster_transport;
