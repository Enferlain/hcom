//! Typed evidence for launch blockers; upstream owns launch readiness.
use crate::core::launch_status::BlockedKind;
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockedClassification {
    pub kind: BlockedKind,
    pub evidence: Option<String>,
}

const DISTINCTIVE_TRUST_PROMPTS: &[&str] = &[
    "do you trust the files in this folder?",
    "do you trust the authors of the files in this workspace?",
    "do you trust the contents of this project?",
    "do you want to trust this folder?",
];

fn matching_line(tail: &str, patterns: &[&str]) -> Option<String> {
    tail.lines().find_map(|line| {
        let lower = line.to_lowercase();
        patterns
            .iter()
            .any(|pattern| lower.contains(pattern))
            .then(|| line.trim().to_string())
            .filter(|line| !line.is_empty())
    })
}

pub(super) fn has_distinctive_trust_prompt(tail: &str) -> bool {
    matching_line(tail, DISTINCTIVE_TRUST_PROMPTS).is_some()
}

fn nonzero_exit_evidence(tail: &str) -> Option<String> {
    const MARKER: &str = "process exited with code";
    tail.lines().find_map(|line| {
        let lower = line.to_lowercase();
        let suffix = lower.split_once(MARKER)?.1.trim_start();
        let token = suffix
            .split(|ch: char| !(ch.is_ascii_digit() || ch == '-'))
            .find(|part| !part.is_empty())?;
        token
            .parse::<i32>()
            .ok()
            // A routine inner command can exit nonzero while the provider is
            // healthy. Reserve this signal for shell-style signal exits.
            .filter(|code| *code < 0 || *code >= 128)
            .map(|_| line.trim().to_string())
    })
}

pub(crate) fn classify_launch_blocker(tail: &str, screen_approval: bool) -> BlockedClassification {
    // Distinctive trust prompts outrank the generic approval signal because
    // they identify the exact startup blocker and can safely bypass settling.
    if let Some(evidence) = matching_line(tail, DISTINCTIVE_TRUST_PROMPTS) {
        return BlockedClassification {
            kind: BlockedKind::WorkspaceTrust,
            evidence: Some(evidence),
        };
    }

    // A provider's structured approval signal is stronger than arbitrary text
    // in the proposed command (which may itself mention auth, quota, or errors).
    if screen_approval {
        return BlockedClassification {
            kind: BlockedKind::Confirmation,
            evidence: matching_line(
                tail,
                &[
                    "requesting permission for:",
                    "allow sandbox bypass for command execution?",
                    "do you want to proceed?",
                    "allow access to this file?",
                    "allow this tool?",
                    "approve command",
                ],
            )
            .or_else(|| Some("approval prompt detected by provider".to_string())),
        };
    }

    if let Some(evidence) = matching_line(
        tail,
        &[
            "please login",
            "please sign in",
            "authentication required",
            "invalid api key",
            "missing api key",
            "login with",
            "not logged in",
            "401 unauthorized",
            "api key not found",
            "provide an api key",
        ],
    ) {
        return BlockedClassification {
            kind: BlockedKind::Authentication,
            evidence: Some(evidence),
        };
    }

    if let Some(evidence) = matching_line(
        tail,
        &[
            "quota exceeded",
            "rate limit reached",
            "insufficient credits",
            "credit balance is too low",
            "429 too many requests",
            "out of credits",
            "exceeded your current quota",
        ],
    ) {
        return BlockedClassification {
            kind: BlockedKind::Quota,
            evidence: Some(evidence),
        };
    }

    if let Some(evidence) = matching_line(
        tail,
        &[
            "panic:",
            "panicked at",
            "segmentation fault",
            "core dumped",
            "sigsegv",
            "sigabrt",
            "unhandled exception",
            "process terminated unexpectedly",
        ],
    )
    .or_else(|| nonzero_exit_evidence(tail))
    {
        return BlockedClassification {
            kind: BlockedKind::Crashed,
            evidence: Some(evidence),
        };
    }

    if let Some(evidence) = matching_line(
        tail,
        &[
            "do you want to proceed?",
            "press enter to continue",
            "allow this tool?",
            "approve command",
            "waiting for user approval",
            "permission requested",
            "grant permission",
            "[y/n]",
            "(y/n)",
        ],
    ) {
        return BlockedClassification {
            kind: BlockedKind::Confirmation,
            evidence: Some(evidence),
        };
    }

    BlockedClassification {
        kind: BlockedKind::Unknown,
        evidence: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_classify_launch_blocker_all_kinds() {
        // Workspace Trust
        let res = classify_launch_blocker("Do you trust the files in this folder?", false);
        assert_eq!(res.kind, BlockedKind::WorkspaceTrust);
        assert!(res.evidence.is_some());

        let res = classify_launch_blocker(
            "Do you trust the authors of the files in this workspace?",
            false,
        );
        assert_eq!(res.kind, BlockedKind::WorkspaceTrust);

        let res = classify_launch_blocker("Do you trust the contents of this project?", false);
        assert_eq!(res.kind, BlockedKind::WorkspaceTrust);

        // Authentication
        let res = classify_launch_blocker("Please login with `claude login`", false);
        assert_eq!(res.kind, BlockedKind::Authentication);
        assert!(res.evidence.is_some());

        let res = classify_launch_blocker("Missing API key: ANTHROPIC_API_KEY", false);
        assert_eq!(res.kind, BlockedKind::Authentication);

        // Quota
        let res = classify_launch_blocker("429 Too Many Requests: quota exceeded", false);
        assert_eq!(res.kind, BlockedKind::Quota);
        assert!(res.evidence.is_some());

        let res = classify_launch_blocker("Rate limit reached for model", false);
        assert_eq!(res.kind, BlockedKind::Quota);

        // Crashed
        let res = classify_launch_blocker("thread 'main' panicked at 'fatal crash'", false);
        assert_eq!(res.kind, BlockedKind::Crashed);
        assert!(res.evidence.is_some());

        let res = classify_launch_blocker("Segmentation fault (core dumped)", false);
        assert_eq!(res.kind, BlockedKind::Crashed);

        // Confirmation (screen approval)
        let res = classify_launch_blocker("normal output", true);
        assert_eq!(res.kind, BlockedKind::Confirmation);
        assert_eq!(
            res.evidence,
            Some("approval prompt detected by provider".to_string())
        );
        let res = classify_launch_blocker("Allow access to this file?", true);
        assert_eq!(res.kind, BlockedKind::Confirmation);
        assert_eq!(res.evidence, Some("Allow access to this file?".to_string()));
        let res = classify_launch_blocker("Allow sandbox bypass for command execution?", true);
        assert_eq!(res.kind, BlockedKind::Confirmation);
        assert_eq!(
            res.evidence,
            Some("Allow sandbox bypass for command execution?".to_string())
        );

        // Structured approval outranks command text containing other blocker
        // words, while a distinctive trust prompt remains more specific.
        let res = classify_launch_blocker(
            "Requesting permission for: export ANTHROPIC_API_KEY=example",
            true,
        );
        assert_eq!(res.kind, BlockedKind::Confirmation);
        assert_eq!(
            res.evidence,
            Some("Requesting permission for: export ANTHROPIC_API_KEY=example".to_string())
        );
        let res = classify_launch_blocker("Do you trust the files in this folder?", true);
        assert_eq!(res.kind, BlockedKind::WorkspaceTrust);

        // Confirmation (prompt pattern)
        let res = classify_launch_blocker("Do you want to proceed? [y/N]", false);
        assert_eq!(res.kind, BlockedKind::Confirmation);

        // Successful exits and ordinary progress prose are not blockers.
        let res = classify_launch_blocker("process exited with code 0", false);
        assert_eq!(res.kind, BlockedKind::Unknown);
        let res = classify_launch_blocker("process exited with code 17", false);
        assert_eq!(res.kind, BlockedKind::Unknown);
        let res = classify_launch_blocker("process exited with code 137", false);
        assert_eq!(res.kind, BlockedKind::Crashed);
        assert_eq!(
            res.evidence,
            Some("process exited with code 137".to_string())
        );
        let res = classify_launch_blocker("Authenticating with GitHub...", false);
        assert_eq!(res.kind, BlockedKind::Unknown);
        let res = classify_launch_blocker("Read the Workspace Trust documentation", false);
        assert_eq!(res.kind, BlockedKind::Unknown);
        assert!(!has_distinctive_trust_prompt(
            "Read the Workspace Trust documentation"
        ));

        // Unknown fallback
        let res = classify_launch_blocker("some random settled output that is not ready", false);
        assert_eq!(res.kind, BlockedKind::Unknown);
        assert_eq!(res.evidence, None);

        let res = classify_launch_blocker("", false);
        assert_eq!(res.kind, BlockedKind::Unknown);
        assert_eq!(res.evidence, None);
    }
}
