//! Exact versions of the real CLIs the real-tool tests drive, read from
//! `scripts/mock-tools.pins` — the same file the install scripts and the CI
//! cache key use, so a bump is a one-line change there.

const PINS: &str = include_str!("../../scripts/mock-tools.pins");

/// The pinned version of `package` (e.g. `@openai/codex`).
pub fn pinned_version(package: &str) -> &'static str {
    PINS.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .find_map(|line| line.strip_prefix(package)?.strip_prefix('@'))
        .unwrap_or_else(|| panic!("no pin for {package} in scripts/mock-tools.pins"))
}

/// Command that installs every pin into the prefix the tests put on PATH.
pub const INSTALL_HINT: &str = "just mock-tools (or scripts/install-mock-tools.sh)";

/// Where `just mock-tools` puts the pinned launchers: `$HCOM_MOCK_TOOLS_PREFIX`
/// (set by `just ci`, which moves it under `$HOME` on Android), else
/// `target/mock-tools`. npm links shims under `bin/` on Unix and into the
/// prefix root on Windows.
pub fn mock_tools_bin() -> std::path::PathBuf {
    let prefix = std::env::var_os("HCOM_MOCK_TOOLS_PREFIX")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/mock-tools")
        });
    if cfg!(windows) {
        prefix
    } else {
        prefix.join("bin")
    }
}

/// PATH for launched tools: `front` (the hcom under test), then the installed
/// pins so they outrank any ambient install, then the inherited PATH.
pub fn tool_path(front: Option<&std::path::Path>) -> std::ffi::OsString {
    let mut entries: Vec<std::path::PathBuf> = front
        .map(std::path::Path::to_path_buf)
        .into_iter()
        .collect();
    let pins = mock_tools_bin();
    if pins.is_dir() {
        entries.push(pins);
    }
    if let Some(inherited) = std::env::var_os("PATH") {
        entries.extend(std::env::split_paths(&inherited));
    }
    std::env::join_paths(entries).expect("construct tool PATH")
}
