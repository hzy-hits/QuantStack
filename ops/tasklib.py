#!/usr/bin/env python3
"""Shared task-registry helpers for ops scripts."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml


OPS_DIR = Path(__file__).resolve().parent
STACK_ROOT = OPS_DIR.parent
TASKS_PATH = OPS_DIR / "tasks.yaml"


def load_registry(path: Path = TASKS_PATH) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    tasks = data.get("tasks") or {}
    if not isinstance(tasks, dict):
        raise ValueError("tasks.yaml must contain a mapping at key 'tasks'")
    return data


def tasks(data: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    data = data or load_registry()
    return data["tasks"]


def get_task(task_id: str, data: dict[str, Any] | None = None) -> dict[str, Any]:
    registry = tasks(data)
    if task_id not in registry:
        known = ", ".join(sorted(registry))
        raise KeyError(f"unknown task id '{task_id}'. Known tasks: {known}")
    task = dict(registry[task_id] or {})
    defaults = (data or load_registry()).get("defaults") or {}
    for key, value in defaults.items():
        task.setdefault(key, value)
    return task


def _date_tokens(date_override: str | None = None) -> dict[str, str]:
    if date_override:
        cst = datetime.fromisoformat(date_override)
        ny = cst
    else:
        cst = datetime.now(ZoneInfo("Asia/Shanghai"))
        ny = datetime.now(ZoneInfo("America/New_York"))
    return {
        "cst_date": cst.strftime("%Y-%m-%d"),
        "ny_date": ny.strftime("%Y-%m-%d"),
        "cst_yyyymmdd": cst.strftime("%Y%m%d"),
        "ny_yyyymmdd": ny.strftime("%Y%m%d"),
    }


def render_value(value: Any, *, task_id: str, date_override: str | None = None) -> Any:
    tokens = {
        "stack_root": str(STACK_ROOT),
        "task_id": task_id,
        **_date_tokens(date_override),
    }
    if isinstance(value, str):
        return value.format(**tokens)
    if isinstance(value, list):
        return [render_value(item, task_id=task_id, date_override=date_override) for item in value]
    if isinstance(value, dict):
        return {
            str(k): render_value(v, task_id=task_id, date_override=date_override)
            for k, v in value.items()
        }
    return value


def resolve_path(value: str | None, *, task_id: str, date_override: str | None = None) -> Path | None:
    if not value:
        return None
    rendered = render_value(value, task_id=task_id, date_override=date_override)
    path = Path(rendered)
    return path if path.is_absolute() else STACK_ROOT / path


def materialize_task(task_id: str, date_override: str | None = None) -> dict[str, Any]:
    data = load_registry()
    task = get_task(task_id, data)
    rendered = render_value(task, task_id=task_id, date_override=date_override)
    command = rendered.get("command") or []
    if not isinstance(command, list) or not all(isinstance(part, str) for part in command):
        raise ValueError(f"{task_id}: command must be a list of strings")
    cwd = resolve_path(rendered.get("cwd") or ".", task_id=task_id, date_override=date_override)
    if cwd is None:
        raise ValueError(f"{task_id}: cwd could not be resolved")
    out = {
        "task_id": task_id,
        "cwd": str(cwd),
        "command": command,
        "log": str(resolve_path(rendered.get("log"), task_id=task_id, date_override=date_override) or ""),
        "legacy_log": str(
            resolve_path(rendered.get("legacy_log"), task_id=task_id, date_override=date_override) or ""
        ),
        "lock": str(resolve_path(rendered.get("lock"), task_id=task_id, date_override=date_override) or ""),
        "timeout_minutes": int(rendered.get("timeout_minutes") or 0),
        "sleep_seconds": int(rendered.get("sleep_seconds") or 0),
        "env": {str(k): str(v) for k, v in (rendered.get("env") or {}).items()},
        "schedule": rendered.get("schedule"),
        "schedules": rendered.get("schedules"),
        "group": rendered.get("group") or "",
        "market": rendered.get("market") or "",
        "session": rendered.get("session") or "",
        "sends_email": bool(rendered.get("sends_email")),
        "owner": rendered.get("owner") or "",
        "outputs": rendered.get("outputs") or [],
    }
    return out


def to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
