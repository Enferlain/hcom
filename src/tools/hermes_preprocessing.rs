//! Hermes launch argv: hcom runs the interactive `hermes chat`. Everything
//! else passes through for Hermes's own parser to judge.

use super::launch_arg_validation::{RejectedArg, RejectedArgKind, validate_rejected_args};

/// Modes that answer once and exit: not a persistent hcom agent.
const REJECTED: &[RejectedArg] = &[
    RejectedArg {
        token: "-z",
        reason: "answers one prompt and exits",
        kind: RejectedArgKind::Flag,
    },
    RejectedArg {
        token: "--oneshot",
        reason: "answers one prompt and exits",
        kind: RejectedArgKind::Flag,
    },
    RejectedArg {
        token: "-Q",
        reason: "answers one prompt and exits",
        kind: RejectedArgKind::Flag,
    },
    RejectedArg {
        token: "--quiet",
        reason: "answers one prompt and exits",
        kind: RejectedArgKind::Flag,
    },
];

pub fn validate_hermes_args(args: &[String]) -> Vec<String> {
    let mut errors = validate_rejected_args("Hermes", "hcom hermes", args, REJECTED);
    // `--format stream-json` implies a quiet single query; `text` is the default.
    let stream_json = args.iter().enumerate().any(|(i, arg)| {
        arg == "--format=stream-json"
            || (arg == "--format" && args.get(i + 1).is_some_and(|v| v == "stream-json"))
    });
    if stream_json {
        errors.push(
            "Hermes argument `--format stream-json` is not supported by `hcom hermes`: it answers one prompt and exits. Launch the hcom-managed interactive PTY instead."
                .to_string(),
        );
    }
    errors
}

/// Options that take a value (top-level or `chat`), so a value spelled `chat`
/// is not the subcommand. `-c/--continue` takes an optional one.
const VALUE_OPTIONS: &[&str] = &[
    "-p",
    "--profile",
    "-m",
    "--model",
    "--provider",
    "-t",
    "--toolsets",
    "-s",
    "--skills",
    "-r",
    "--resume",
    "--in",
    "--reasoning",
    "-q",
    "--query",
    "--query-file",
    "--image",
    "--format",
    "--max-turns",
    "--run-budget",
    "--source",
    "-z",
    "--usage-file",
];

/// Whether the user's args already hold the `chat` subcommand (in subcommand
/// position: the first token that is neither an option nor an option's value).
fn has_chat_subcommand(args: &[String]) -> bool {
    let mut iter = args.iter().take_while(|a| *a != "--").peekable();
    while let Some(arg) = iter.next() {
        if !arg.starts_with('-') {
            return arg == "chat";
        }
        if VALUE_OPTIONS.contains(&arg.as_str()) {
            iter.next();
        } else if arg == "-c" || arg == "--continue" {
            iter.next_if(|next| !next.starts_with('-') && *next != "chat");
        }
    }
    false
}

/// `-q` (hcom's initial prompt) and `--resume` are `chat` options, so the
/// argv needs `chat`; it goes first unless the user already gave it.
pub fn with_chat_subcommand(mut args: Vec<String>) -> Vec<String> {
    if !has_chat_subcommand(&args) {
        args.insert(0, "chat".to_string());
    }
    args
}

#[cfg(test)]
mod tests {
    use super::*;

    fn merge_resume_args(original: &[String], resume: &[String]) -> Vec<String> {
        use crate::tools::argument_overrides::{override_args, saved_args};
        let inherited = saved_args(crate::tool::Tool::Hermes, original);
        override_args(crate::tool::Tool::Hermes, &inherited, resume)
    }

    fn sv(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn chat_is_added_once() {
        assert_eq!(with_chat_subcommand(sv(&[])), sv(&["chat"]));
        assert_eq!(
            with_chat_subcommand(sv(&["-p", "work", "-m", "x"])),
            sv(&["chat", "-p", "work", "-m", "x"])
        );
        assert_eq!(
            with_chat_subcommand(sv(&["-p", "work", "chat", "--tui"])),
            sv(&["-p", "work", "chat", "--tui"])
        );
        // An option value spelled `chat` is not the subcommand.
        for args in [
            &["-m", "chat"][..],
            &["-q", "chat"],
            &["-p", "chat"],
            &["--model", "chat"],
        ] {
            let mut expected = sv(&["chat"]);
            expected.extend(sv(args));
            assert_eq!(with_chat_subcommand(sv(args)), expected, "{args:?}");
        }
        assert_eq!(
            with_chat_subcommand(sv(&["-c", "chat", "--tui"])),
            sv(&["-c", "chat", "--tui"])
        );
    }

    #[test]
    fn rejects_one_shot_modes() {
        assert!(validate_hermes_args(&sv(&["-p", "x", "--tui", "-q", "hi"])).is_empty());
        assert!(validate_hermes_args(&sv(&["chat", "--format", "text"])).is_empty());
        for args in [
            sv(&["-z", "hi"]),
            sv(&["chat", "-q", "hi", "-Q"]),
            sv(&["chat", "--oneshot"]),
            sv(&["chat", "--format", "stream-json"]),
            sv(&["--format=stream-json"]),
        ] {
            assert!(!validate_hermes_args(&args).is_empty(), "{args:?}");
        }
    }

    #[test]
    fn resume_replays_flags_but_not_session_or_prompt() {
        let original = sv(&[
            "-p", "work", "chat", "-m", "x", "-q", "do it", "--resume", "old", "-c", "--tui",
        ]);
        assert_eq!(
            merge_resume_args(&original, &sv(&["--resume", "sid"])),
            sv(&["-p", "work", "chat", "-m", "x", "--tui", "--resume", "sid"])
        );
        assert_eq!(
            merge_resume_args(
                &sv(&["--continue=name", "--resume=old", "--yolo"]),
                &sv(&["--resume", "sid"])
            ),
            sv(&["--yolo", "--resume", "sid"])
        );
        assert_eq!(
            merge_resume_args(
                &sv(&["-q=seed", "-r=old", "-c=x", "-m", "y"]),
                &sv(&["--resume", "sid"])
            ),
            sv(&["-m", "y", "--resume", "sid"])
        );
        assert_eq!(
            with_chat_subcommand(merge_resume_args(
                &sv(&["-c", "name"]),
                &sv(&["--resume", "s"])
            )),
            sv(&["chat", "--resume", "s"])
        );
    }
}
