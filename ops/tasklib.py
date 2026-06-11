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
        "depends_on": list(rendered.get("depends_on") or []),
    }
    return out


def to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


CST_TZ = ZoneInfo("Asia/Shanghai")


def parse_cron_field(field: str, lo: int, hi: int) -> set[int]:
    """Expand one cron field (supports '*', 'a', 'a-b', comma lists, '*/n')."""
    if field == "*":
        return set(range(lo, hi + 1))
    out: set[int] = set()
    for part in field.split(","):
        if part == "*":
            out.update(range(lo, hi + 1))
        elif part.startswith("*/"):
            step = int(part[2:])
            out.update(range(lo, hi + 1, step))
        elif "-" in part:
            a, b = part.split("-")
            out.update(range(int(a), int(b) + 1))
        else:
            out.add(int(part))
    return out


def cron_matches(expr: str, dt: datetime) -> bool:
    """True if a 5-field cron expression matches datetime dt."""
    parts = expr.split()
    if len(parts) != 5:
        return False
    minute, hour, dom, month, dow = parts
    if dt.minute not in parse_cron_field(minute, 0, 59):
        return False
    if dt.hour not in parse_cron_field(hour, 0, 23):
        return False
    if dt.day not in parse_cron_field(dom, 1, 31):
        return False
    if dt.month not in parse_cron_field(month, 1, 12):
        return False
    cron_dow = dt.isoweekday() % 7          # Mon=1..Sat=6, Sun=0
    dow_set = parse_cron_field(dow, 0, 7)
    if 7 in dow_set:                        # cron allows 7 for Sunday
        dow_set.add(0)
    return cron_dow in dow_set


def _scheduled_today(task: dict[str, Any], now: datetime) -> bool:
    """True if any of the task's cron day-fields match today (time ignored)."""
    exprs = [task.get("schedule")] if task.get("schedule") else list(task.get("schedules") or [])
    for expr in exprs:
        if not expr or str(expr).startswith("@"):
            continue
        parts = str(expr).split()
        if len(parts) != 5:
            continue
        _minute, _hour, dom, month, dow = parts
        if now.day not in parse_cron_field(dom, 1, 31):
            continue
        if now.month not in parse_cron_field(month, 1, 12):
            continue
        dow_set = parse_cron_field(dow, 0, 7)
        if 7 in dow_set:
            dow_set.add(0)
        if now.isoweekday() % 7 in dow_set:
            return True
    return False


def unmet_dependencies(task: dict[str, Any], *, registry: dict[str, dict[str, Any]],
                       state_dir: Path, now: datetime) -> list[str]:
    """depends_on entries that are scheduled today but have no success today (CST).

    A dependency that is not scheduled today (weekend/holiday cadence) counts
    as met so dependents are never dead-blocked.
    """
    unmet: list[str] = []
    for dep_id in task.get("depends_on") or []:
        dep = registry.get(dep_id)
        if dep is None or not _scheduled_today(dep, now):
            continue
        success_path = state_dir / f"{dep_id}.last_success.json"
        try:
            finished = datetime.fromisoformat(
                json.loads(success_path.read_text(encoding="utf-8"))["finished_at"])
        except (OSError, ValueError, KeyError):
            unmet.append(dep_id)
            continue
        if finished.tzinfo is None:
            finished = finished.replace(tzinfo=CST_TZ)
        if finished.astimezone(CST_TZ).date() != now.astimezone(CST_TZ).date():
            unmet.append(dep_id)
    return unmet


def order_by_dependency(missed: list[tuple[str, Any]],
                        registry: dict[str, dict[str, Any]]) -> list[tuple[str, Any]]:
    """Order (task_id, due) items so depends_on targets run first (stable)."""
    pending = {task_id for task_id, _ in missed}
    ordered: list[tuple[str, Any]] = []
    remaining = list(missed)
    for _ in range(len(missed) + 1):
        progressed = False
        for item in list(remaining):
            deps = set((registry.get(item[0]) or {}).get("depends_on") or [])
            if not deps & pending:
                ordered.append(item)
                pending.discard(item[0])
                remaining.remove(item)
                progressed = True
        if not progressed:
            return ordered + remaining  # cycle fallback: keep original order
    return ordered
