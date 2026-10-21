//! Forks an active or stopped instance by launching with --fork-session
//! (Claude), fork subcommand (Codex), --fork (OpenCode), or respective options for Kilo/Pi/OMP.

use anyhow::Result;

use crate::router::GlobalFlags;

/// Run the fork command. `argv` is the full argv[1..].
pub fn run(argv: &[String], flags: &GlobalFlags) -> Result<i32> {
    let (name, extra_args) = match super::resume::parse_resume_argv(argv, "f") {
        Ok(parsed) => parsed,
        Err(e) => return Ok(crate::commands::help::usage_error("fork", &e.to_string())),
    };
    super::resume::do_resume(&name, true, &extra_args, flags)
}
