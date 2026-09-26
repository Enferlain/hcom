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
    """Map a Hermes session id to its hcom instance name.

    session_bindings is the live mapping (session_id → name). A missing row
    means this session is not mesh-bound — the chip shows nothing.
    """
    import sqlite3

    if not session_id or not _HCOM_DB.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{_HCOM_DB}?mode=ro", uri=True, timeout=3.0)
    except sqlite3.Error:
        return None
    try:
        row = conn.execute(
            "SELECT name FROM session_bindings WHERE session_id = ?", (session_id,)
        ).fetchone()
        return row[0] if row else None
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


@router.get("/status")
def status(session_id: str = "") -> dict:
    name = _name_for_session(session_id)
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
