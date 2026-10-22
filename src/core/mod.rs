//! Core modules for hcom — shared logic used by hooks, CLI commands, and TUI.
//!
//! - `helpers`: Input validation (scope, intent, mentions, group routing)
//! - `filters`: Composable event filter system (parse flags → SQL WHERE)
//! - `launch_status`: Batch launch tracking and wait_for_launch polling
//! - `detail_levels`: Transcript detail level definitions
//! - `bundles`: Structured context sharing (bundle create/validate/parse)

pub mod bundles;
pub mod compact;
pub mod detail_levels;
pub mod filters;
pub mod helpers;
pub mod launch_status;
pub mod progress;
pub mod result_wait;
pub mod tips;
