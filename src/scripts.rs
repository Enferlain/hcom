/// Bundled scripts embedded at compile time.
pub const SCRIPTS: &[(&str, &str)] = &[
    ("agy", include_str!("scripts/bundled/agy.sh")),
    ("confess", include_str!("scripts/bundled/confess.sh")),
    ("debate", include_str!("scripts/bundled/debate.sh")),
    ("fatcow", include_str!("scripts/bundled/fatcow.sh")),
    ("glm", include_str!("scripts/bundled/glm.sh")),
];
