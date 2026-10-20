//! Minimal subprocess coverage of the persistent-install gate.
use std::fs;
use std::process::Command;

#[test]
fn ephemeral_uvx_requires_persistent_install() {
    let root = tempfile::tempdir().unwrap();
    let cache = root.path().join("cache");
    let venv = cache.join("archive-v0/env");
    let exe = venv.join(if cfg!(windows) {
        "Scripts/hcom.exe"
    } else {
        "bin/hcom"
    });
    fs::create_dir_all(exe.parent().unwrap()).unwrap();
    fs::write(cache.join("CACHEDIR.TAG"), "").unwrap();
    fs::write(venv.join("pyvenv.cfg"), "uv = 0.7\n").unwrap();
    fs::hard_link(env!("CARGO_BIN_EXE_hcom"), &exe)
        .or_else(|_| fs::copy(env!("CARGO_BIN_EXE_hcom"), &exe).map(|_| ()))
        .unwrap();
    let state = root.path().join("state");
    let home = root.path().join("home");
    for (arg, code, antigravity, stdout) in [
        ("list", 1, false, Some("")),
        ("--help", 0, false, None),
        ("--version", 0, false, None),
        ("pre", 1, false, Some("")),
        ("copilot-pretooluse", 0, false, Some("")),
        (
            "gemini-beforetool",
            0,
            true,
            Some("{\"decision\":\"allow\"}"),
        ),
        (
            "gemini-sessionend",
            0,
            true,
            Some("{\"decision\":\"allow\"}"),
        ),
    ] {
        let mut cmd = Command::new(&exe);
        cmd.env_clear()
            .arg(arg)
            .current_dir(root.path())
            .env("HOME", &home)
            .env("USERPROFILE", &home)
            .env("HCOM_DIR", &state)
            .env("HCOM_DEV_ROOT", root.path().join("missing-dev-root"));
        for key in ["PATH", "SystemRoot", "WINDIR", "LD_LIBRARY_PATH"] {
            if let Some(value) = std::env::var_os(key) {
                cmd.env(key, value);
            }
        }
        if antigravity {
            cmd.env("ANTIGRAVITY_AGENT", "1");
        }
        let output = cmd.output().unwrap();
        assert_eq!(output.status.code(), Some(code), "{arg}: {output:?}");
        if let Some(stdout) = stdout {
            assert_eq!(String::from_utf8_lossy(&output.stdout), stdout, "{arg}");
            assert_eq!(
                String::from_utf8_lossy(&output.stderr),
                "hcom: uvx is no longer supported; run uv tool install hcom, then restart this session.\n",
                "{arg}"
            );
        } else {
            assert!(!output.stdout.is_empty(), "{arg}");
            assert!(output.stderr.is_empty(), "{output:?}");
        }
        assert!(!state.exists(), "{arg} created hcom state");
        assert!(!home.exists(), "{arg} created tool configs");
    }
}
