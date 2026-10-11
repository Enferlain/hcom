//! Omp Coding Agent hook handlers — argv-based lifecycle plus TypeScript plugin.

mod plugin;

#[cfg(test)]
mod tests;

pub use plugin::{
    PER_RUN, PLUGIN_SOURCE, get_omp_plugin_path, remove_omp_plugin, strip_managed_extension_args,
};

#[cfg(test)]
pub(crate) use crate::hooks::plugin_rpc::{handle_status, handle_stop};

pub fn dispatch_omp_hook(hook_name: &str, argv: &[String]) -> (i32, String) {
    crate::hooks::pi_family::OMP.dispatch(hook_name, argv)
}
