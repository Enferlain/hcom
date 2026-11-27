//! Optional OpenShell transport and durable state.

pub mod admission;
#[cfg(unix)]
pub mod bridge;
pub mod controller;
pub(crate) mod conversation;
pub mod directory;
pub(crate) mod guest_send;
pub mod openshell;
pub(crate) mod outgoing;
pub mod protocol;
pub(crate) mod receipts;
pub mod store;
