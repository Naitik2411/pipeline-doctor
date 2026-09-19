from __future__ import annotations

from pathlib import Path
from typing import Any
import json

STATE_PATH = Path(__file__).resolve().parents[2]/".fault_state.json"


def read_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {"active":None, "params":{}}
    return json.loads(STATE_PATH.read_text())

def write_state(active:str, params:dict[str,Any]|None = None) -> None:
    STATE_PATH.write_text(
        json.dumps({"active": active, "params": params or {}}, indent=2)
    )

def clear_state() -> None:
    write_state(None, {})
