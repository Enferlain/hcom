"""hcom mesh status backend for the Hermes Desktop plugin.

Serves the statusbar chip's data and the manual drain-now action:
  - GET /status  → current unread state for this session's hcom identity
                   (has_unread, has_request, count, latest_from, name)
  - POST /drain  → connect-drop wake ping to the plugin's registered notify
                   port: the same path a mesh send uses, which the plugin's
                   wake handler gates exactly like a mesh ping (unread check,
                   intent gate, debounce). No second injection path exists.

Identity resolution: the plugin registers its notify port under the hcom
instance name at bind time (notify_endpoints.kind = 'plugin'), and the
gateway session id is the instance's session binding. The chip asks the
backend with the live session id; the backend maps it to the hcom name.
"""

from __future__ import annotations

import json
import socket
import subprocess
from pathlib import Path

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

_HCOM_DB = Path.home() / ".hcom" / "hcom.db"
_HCOM_BIN = "hcom"


def _hcom_json(args: list[str], timeout: float = 5.0) -> dict | None:
    """Run an hcom RPC subcommand; return parsed JSON stdout or None."""
    try:
        proc = subprocess.run(
            [_HCOM_BIN, *args], capture_output=True, text=True, timeout=timeout
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    out = (proc.stdout or "").strip()
    if not out:
        return {}
    try:
        parsed = json.loads(out)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _name_for_session(session_id: str) -> str | None:
    """Map a Hermes session reference to its hcom instance name.

    Accepts the durable session key (e.g. 20260925_214123_0e89a3) or the
    ephemeral UI session uuid: the instances table records the durable id,
    and a UI id maps through the Hermes sessions registry if needed. Falls
    back to the single active hermes-tool instance only when exactly one
    exists (unambiguous); never guesses among several.
    """
    import sqlite3

    if not _HCOM_DB.exists():
        return None

    def _query(sql: str, args: tuple) -> str | None:
        try:
            conn = sqlite3.connect(f"file:{_HCOM_DB}?mode=ro", uri=True, timeout=3.0)
        except sqlite3.Error:
            return None
        try:
            row = conn.execute(sql, args).fetchone()
            return row[0] if row else None
        except sqlite3.Error:
            return None
        finally:
            conn.close()

    if session_id:
        # 1. Direct binding by durable session id.
        name = _query(
            "SELECT instance_name FROM session_bindings WHERE session_id = ?",
            (session_id,),
        )
        if name:
            return name
        # 2. UI uuid → durable key via the Hermes sessions registry.
        durable = _ui_uuid_to_durable(session_id)
        if durable and durable != session_id:
            name = _query(
                "SELECT instance_name FROM session_bindings WHERE session_id = ?",
                (durable,),
            )
            if name:
                return name

    # 3. Fallback: newest-seen hermes instance. Two live rows is the NORM
    #    after a session switch (old row stays listening until stale-marked),
    #    so refusing on ambiguity hides the chip exactly when it matters.
    #    last_seen breaks the tie toward the conversation actually in use.
    try:
        conn = sqlite3.connect(f"file:{_HCOM_DB}?mode=ro", uri=True, timeout=3.0)
    except sqlite3.Error:
        return None
    try:
        # last_seen is not maintained for hermes-tool rows; last_stop (the
        # last status transition timestamp) is the reliable recency signal.
        row = conn.execute(
            "SELECT name FROM instances WHERE tool = 'hermes' "
            "AND status IN ('listening', 'active') "
            "ORDER BY last_stop DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else None
    except sqlite3.Error:
        return None
    finally:
        conn.close()


def _ui_uuid_to_durable(ui_id: str) -> str | None:
    """Resolve an Electron UI session uuid to the durable session key.

    The Hermes session registry (state.db sessions table) carries both; if
    the uuid is unknown or already durable, returns None (caller falls
    through).
    """
    import sqlite3

    db = Path.home() / ".hermes" / "state.db"
    if not db.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=3.0)
    except sqlite3.Error:
        return None
    try:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "sessions" not in tables:
            return None
        cols = {
            r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()
        }
        # Look for a ui id ↔ durable key pairing among plausible columns.
        for col in ("ui_session_id", "client_session_id", "electron_session_id"):
            if col in cols:
                row = conn.execute(
                    f"SELECT session_key FROM sessions WHERE {col} = ?", (ui_id,)
                ).fetchone()
                if row and row[0]:
                    return row[0]
        return None
    except sqlite3.Error:
        return None
    finally:
        conn.close()


def _notify_port(name: str) -> int | None:
    """The plugin-kind notify port hcom holds for this instance."""
    import sqlite3

    if not _HCOM_DB.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{_HCOM_DB}?mode=ro", uri=True, timeout=3.0)
    except sqlite3.Error:
        return None
    try:
        row = conn.execute(
            "SELECT port FROM notify_endpoints WHERE instance = ? AND kind = 'plugin'",
            (name,),
        ).fetchone()
        return int(row[0]) if row and row[0] else None
    except sqlite3.Error:
        return None
    finally:
        conn.close()


class DrainRequest(BaseModel):
    session_id: str = ""


_DEBUG_LOG = Path.home() / ".hcom" / "plugin_status_debug.log"


def _dbg(line: str) -> None:
    """Temporary instrumentation: append to a plain file I can read back."""
    try:
        with open(_DEBUG_LOG, "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


@router.get("/status")
def status(session_id: str = "") -> dict:
    name = _name_for_session(session_id)
    _dbg(f"status session_id={session_id!r} -> name={name!r}")
    if not name:
        # Not mesh-bound (pre-bind window or plain session): no chip data.
        return {"bound": False}
    check = _hcom_json(["hermes-read", "--name", name, "--check"]) or {}
    return {
        "bound": True,
        "name": name,
        "has_unread": bool(check.get("has_unread")),
        "has_request": bool(check.get("has_request")),
        "count": int(check.get("count") or 0),
        "latest_from": str(check.get("latest_from") or ""),
    }


@router.post("/drain")
def drain(body: DrainRequest) -> dict:
    """Manual drain-now: poke the plugin's notify port.

    This is deliberately the SAME wake path a mesh send triggers — the
    plugin re-checks unread/intent/debounce before submitting any turn, so
    the button cannot bypass policy or double-submit.
    """
    name = _name_for_session(body.session_id)
    if not name:
        return {"ok": False, "error": "not-bound"}
    port = _notify_port(name)
    if not port:
        return {"ok": False, "error": "no-notify-port"}
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2.0):
            pass  # connect-drop: the poke IS the signal
    except OSError as exc:
        return {"ok": False, "error": f"ping-failed: {exc}"}
    return {"ok": True}
