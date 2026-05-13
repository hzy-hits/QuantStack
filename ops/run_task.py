#!/usr/bin/env python3
"""Run one registered quant-stack ops task."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO

from tasklib import STACK_ROOT, load_registry, materialize_task, tasks, to_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a task from ops/tasks.yaml")
    parser.add_argument("task_id", nargs="?", help="Task id, e.g. us.postmarket")
    parser.add_argument("--dry-run", action="store_true", help="Print resolved task without running it")
    parser.add_argument("--date", help="Override {cst_date}/{ny_date} tokens with YYYY-MM-DD")
    parser.add_argument("--list", action="store_true", help="List registered task ids")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def git_value(args: list[str]) -> str:
    try:
        return subprocess.check_output(["git", *args], cwd=STACK_ROOT, text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return ""


class Tee:
    def __init__(self, files: list[TextIO]):
        self.files = files

    def write(self, text: str) -> None:
        for fh in self.files:
            fh.write(text)
            fh.flush()

    def close(self) -> None:
        for fh in self.files:
            if fh not in {sys.stdout, sys.stderr}:
                fh.close()


def open_logs(task: dict[str, object]) -> Tee:
    files: list[TextIO] = [sys.stdout]
    seen: set[str] = set()
    for key in ("log", "legacy_log"):
        path_s = str(task.get(key) or "")
        if not path_s or path_s in seen:
            continue
        seen.add(path_s)
        path = Path(path_s)
        path.parent.mkdir(parents=True, exist_ok=True)
        files.append(path.open("a", encoding="utf-8"))
    return Tee(files)


def write_state(task: dict[str, object], name: str, payload: dict[str, object]) -> None:
    state_dir = STACK_ROOT / "ops" / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / f"{task['task_id']}.{name}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_command(task: dict[str, object]) -> list[str]:
    command = list(task["command"])  # type: ignore[arg-type]
    timeout_minutes = int(task.get("timeout_minutes") or 0)
    if timeout_minutes > 0 and shutil.which("timeout"):
        return ["timeout", f"{timeout_minutes}m", *command]
    return command


def run_task(task: dict[str, object]) -> int:
    lock_path = Path(str(task.get("lock") or STACK_ROOT / "ops" / "state" / f"{task['task_id']}.lock"))
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_fh = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(f"task already running: {task['task_id']} lock={lock_path}", file=sys.stderr)
        return 75

    lock_fh.seek(0)
    lock_fh.truncate()
    lock_fh.write(str(os.getpid()))
    lock_fh.flush()

    tee = open_logs(task)
    started = utc_now()
    head = git_value(["rev-parse", "--short", "HEAD"])
    dirty = git_value(["status", "--short"])
    dirty_count = len([line for line in dirty.splitlines() if line.strip()])
    state_payload = {
        "task_id": task["task_id"],
        "started_at": started,
        "cwd": task["cwd"],
        "command": task["command"],
        "git_head": head,
        "git_dirty_count": dirty_count,
    }
    write_state(task, "last_start", state_payload)

    try:
        tee.write("\n")
        tee.write("==========================================\n")
        tee.write(f"task:       {task['task_id']}\n")
        tee.write(f"started_at: {started}\n")
        tee.write(f"cwd:        {task['cwd']}\n")
        tee.write(f"git_head:   {head or '-'} dirty_files={dirty_count}\n")
        tee.write(f"command:    {' '.join(task['command'])}\n")
        tee.write("==========================================\n")

        sleep_seconds = int(task.get("sleep_seconds") or 0)
        if sleep_seconds > 0:
            tee.write(f"sleep_before_run: {sleep_seconds}s\n")
            time.sleep(sleep_seconds)

        env = os.environ.copy()
        env["QUANT_STACK_ROOT"] = str(STACK_ROOT)
        for key, value in (task.get("env") or {}).items():  # type: ignore[union-attr]
            env[str(key)] = str(value)

        command = build_command(task)
        proc = subprocess.Popen(
            command,
            cwd=str(task["cwd"]),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            tee.write(line)
        return_code = proc.wait()
        finished = utc_now()
        final_payload = {
            **state_payload,
            "finished_at": finished,
            "return_code": return_code,
            "log": task.get("log"),
            "legacy_log": task.get("legacy_log"),
        }
        if return_code == 0:
            write_state(task, "last_success", final_payload)
        else:
            write_state(task, "last_failure", final_payload)
        tee.write("==========================================\n")
        tee.write(f"finished_at: {finished}\n")
        tee.write(f"return_code: {return_code}\n")
        tee.write("==========================================\n")
        return return_code
    finally:
        tee.close()
        try:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
        finally:
            lock_fh.close()


def main() -> int:
    args = parse_args()
    if args.list:
        for task_id in tasks(load_registry()):
            print(task_id)
        return 0
    if not args.task_id:
        print("ERROR: task_id is required unless --list is used", file=sys.stderr)
        return 2
    try:
        task = materialize_task(args.task_id, args.date)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    if args.dry_run:
        print(to_json(task))
        return 0
    if not Path(str(task["cwd"])).is_dir():
        print(f"ERROR: cwd does not exist: {task['cwd']}", file=sys.stderr)
        return 2
    return run_task(task)


if __name__ == "__main__":
    raise SystemExit(main())
