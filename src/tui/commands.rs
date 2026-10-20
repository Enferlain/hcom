//! Native command dispatch for TUI — replaces daemon socket RPC.
//!
//! Runs hcom commands as subprocesses of the same binary, capturing
//! stdout/stderr into a Response. Subprocess has its own file
//! descriptors, keeping TUI rendering unaffected.
//!
//! Why subprocess instead of in-process function calls:
//! The TUI owns stdout (fd 1) for ratatui rendering. Commands print
//! to stdout via println!(). Redirecting fd 1 from a background thread
//! would corrupt the TUI display. Subprocess isolation is clean and
//! adds ~5ms overhead — imperceptible for interactive operations.

use std::io::Read;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::tui::rpc::Response;

/// Resolve the hcom binary path.
///
/// In normal operation (TUI running), this is the current executable.
/// The binary is cached per-process since it never changes at runtime.
fn hcom_binary() -> Result<PathBuf, String> {
    std::env::current_exe().map_err(|e| format!("cannot find hcom binary: {e}"))
}

/// Run an hcom command natively, returning captured stdout/stderr.
///
/// Spawns the current binary as a subprocess with the given argv.
/// Inherits environment (HCOM_DEV_ROOT, HCOM_DIR, etc.) so routing
/// and config resolution work identically to CLI invocations.
pub fn run_native(argv: &[String]) -> Result<Response, String> {
    let binary = hcom_binary()?;

    let timeout = if matches!(
        crate::router::resolve_action(argv),
        crate::router::Action::Launch { .. }
    ) {
        Duration::from_secs(300)
    } else {
        Duration::from_secs(60)
    };
    run_with_timeout(Command::new(binary).args(argv), timeout)
}

fn run_with_timeout(command: &mut Command, timeout: Duration) -> Result<Response, String> {
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn hcom: {e}"))?;
    // Drain both pipes concurrently so a full pipe cannot block child exit.
    let read_pipe = |mut pipe: Box<dyn Read + Send>| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut bytes = Vec::new();
            let _ = pipe.read_to_end(&mut bytes);
            let _ = tx.send(bytes);
        });
        rx
    };
    let stdout = read_pipe(Box::new(child.stdout.take().unwrap()));
    let stderr = read_pipe(Box::new(child.stderr.take().unwrap()));
    let start = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if start.elapsed() < timeout => {
                std::thread::sleep(Duration::from_millis(20));
            }
            result => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(match result {
                    Err(e) => format!("wait for hcom: {e}"),
                    _ => format!("hcom command timed out after {} seconds", timeout.as_secs()),
                });
            }
        }
    };
    let output = std::process::Output {
        status,
        stdout: stdout
            .recv_timeout(timeout.saturating_sub(start.elapsed()))
            .map_err(|_| "hcom output collection timed out".to_string())?,
        stderr: stderr
            .recv_timeout(timeout.saturating_sub(start.elapsed()))
            .map_err(|_| "hcom output collection timed out".to_string())?,
    };

    Ok(Response {
        exit_code: output.status.code().unwrap_or(1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_timeout_stops_waiting() {
        let start = Instant::now();
        let error = run_with_timeout(
            Command::new("sh").args(["-c", "sleep 2"]),
            Duration::from_millis(30),
        )
        .err()
        .unwrap();
        assert!(error.contains("timed out"));
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn response_from_run_native_has_correct_fields() {
        // Test the Response construction without needing the binary
        let resp = Response {
            exit_code: 0,
            stdout: "hello\n".into(),
            stderr: String::new(),
        };
        assert!(resp.ok());
        assert_eq!(resp.combined_output_lines(), vec!["hello"]);
    }
}
