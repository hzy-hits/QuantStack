from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import exchange_calendars as xcals


LOCAL_TZ = ZoneInfo("Asia/Shanghai")
NY_TZ = ZoneInfo("America/New_York")
PIPELINE_LOCK_FILE = Path("/tmp/quant-research-pipeline.lock")
DEFAULT_STATE_RETENTION_DAYS = 30
GLOBAL_BUSY_PATTERNS = (
    "bash scripts/run_full.sh",
    "bash scripts/run_weekly.sh",
    "bash scripts/daily_pipeline.sh",
    "bash scripts/weekly_pipeline.sh",
    "bash scripts/daily_factors.sh",
    "bash scripts/autoresearch.sh",
    "scripts/paper_trade.py",
    "scripts/weekly_maintenance.py",
)


@dataclass(frozen=True)
class CompletionCheck:
    kind: str
    path_template: str
    markers: tuple[str, ...] = ()
    min_size: int = 1


@dataclass(frozen=True)
class TaskConfig:
    name: str
    workdir: Path
    command: tuple[str, ...]
    completion: CompletionCheck
    local_weekdays: tuple[int, ...]
    local_hour: int
    local_minute: int
    grace_minutes: int = 20
    logical_date_kind: str = "local"
    trading_calendar: str | None = None
    same_day_only: bool = False
    priority: int = 50


@dataclass(frozen=True)
class DueRun:
    task: TaskConfig
    local_day: date
    logical_date: date
    scheduled_at_local: datetime

    @property
    def key(self) -> str:
        return f"{self.task.name}|{self.local_day.isoformat()}|{self.logical_date.isoformat()}"

    def context(self) -> dict[str, str]:
        return {
            "local_date": self.local_day.isoformat(),
            "local_date_nodash": self.local_day.strftime("%Y%m%d"),
            "logical_date": self.logical_date.isoformat(),
            "logical_date_nodash": self.logical_date.strftime("%Y%m%d"),
        }


def default_state_path(project_dir: Path) -> Path:
    return project_dir / "logs" / "cron_watchdog_state.json"


def load_state(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {"runs": {}}
    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"runs": {}}
    runs = data.get("runs")
    if not isinstance(runs, dict):
        return {"runs": {}}
    return {"runs": runs}


def save_state(state_path: Path, state: dict[str, Any]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(state, indent=2, sort_keys=True, ensure_ascii=True),
        encoding="utf-8",
    )


def resolve_repo_dir(*candidates: str | Path | None) -> Path:
    for candidate in candidates:
        if candidate is None:
            continue
        path = Path(candidate).expanduser()
        if path.is_dir():
            return path.resolve()
    raise FileNotFoundError(f"unable to resolve repo dir from: {candidates}")


def resolve_stack_roots(project_dir: Path) -> tuple[Path, Path, Path]:
    us_root = project_dir.resolve()
    stack_root = os.environ.get("QUANT_STACK_ROOT")
    base_root = us_root.parents[1]

    factor_lab_root = resolve_repo_dir(
        os.environ.get("FACTOR_LAB_ROOT"),
        Path(stack_root) / "factor-lab" if stack_root else None,
        us_root.parent / "factor-lab",
        base_root / "python" / "factor-lab",
    )
    cn_root = resolve_repo_dir(
        os.environ.get("QUANT_CN_ROOT"),
        Path(stack_root) / "quant-research-cn" if stack_root else None,
        base_root / "rust" / "quant-research-cn",
    )
    return us_root, cn_root, factor_lab_root


@lru_cache(maxsize=None)
def get_calendar(name: str):
    return xcals.get_calendar(name)


def is_trading_day(report_date: date, calendar_name: str | None) -> bool:
    if not calendar_name:
        return True
    return bool(get_calendar(calendar_name).is_session(report_date.isoformat()))


def _scheduled_at_local(local_day: date, task: TaskConfig) -> datetime:
    return datetime.combine(
        local_day,
        time(hour=task.local_hour, minute=task.local_minute),
        tzinfo=LOCAL_TZ,
    )


def scheduled_logical_date(local_day: date, task: TaskConfig) -> date:
    scheduled_at = _scheduled_at_local(local_day, task)
    if task.logical_date_kind == "ny":
        return scheduled_at.astimezone(NY_TZ).date()
    return local_day


def build_default_tasks(project_dir: Path) -> tuple[TaskConfig, ...]:
    us_root, cn_root, factor_lab_root = resolve_stack_roots(project_dir)

    return (
        TaskConfig(
            name="us-premarket",
            workdir=us_root,
            command=("bash", "scripts/run_full.sh", "--premarket", "{logical_date}"),
            completion=CompletionCheck(
                kind="file_exists",
                path_template="reports/{logical_date}_report_zh_pre.md",
                min_size=200,
            ),
            local_weekdays=(0, 1, 2, 3, 4),
            local_hour=20,
            local_minute=0,
            logical_date_kind="ny",
            trading_calendar="XNYS",
            priority=10,
        ),
        TaskConfig(
            name="us-postmarket",
            workdir=us_root,
            command=("bash", "scripts/run_full.sh", "{logical_date}"),
            completion=CompletionCheck(
                kind="file_exists",
                path_template="reports/{logical_date}_report_zh_post.md",
                min_size=200,
            ),
            local_weekdays=(1, 2, 3, 4, 5),
            local_hour=5,
            local_minute=0,
            logical_date_kind="ny",
            trading_calendar="XNYS",
            priority=10,
        ),
        TaskConfig(
            name="us-weekly",
            workdir=us_root,
            command=("bash", "scripts/run_weekly.sh", "{logical_date}"),
            completion=CompletionCheck(
                kind="file_exists",
                path_template="reports/{logical_date}_report_weekly_zh.md",
                min_size=200,
            ),
            local_weekdays=(5,),
            local_hour=9,
            local_minute=30,
            logical_date_kind="ny",
            priority=15,
        ),
        TaskConfig(
            name="cn-morning",
            workdir=cn_root,
            command=("bash", "scripts/daily_pipeline.sh", "morning", "{logical_date}"),
            completion=CompletionCheck(
                kind="file_contains_all",
                path_template="reports/logs/{logical_date}_morning.log",
                markers=(
                    "Pipeline complete:",
                    "Report: reports/{logical_date}_report_zh.md",
                ),
            ),
            local_weekdays=(0, 1, 2, 3, 4),
            local_hour=8,
            local_minute=30,
            trading_calendar="XSHG",
            priority=10,
        ),
        TaskConfig(
            name="cn-evening",
            workdir=cn_root,
            command=("bash", "scripts/daily_pipeline.sh", "evening", "{logical_date}"),
            completion=CompletionCheck(
                kind="file_contains_all",
                path_template="reports/logs/{logical_date}_evening.log",
                markers=(
                    "Pipeline complete:",
                    "Report: reports/{logical_date}_report_zh.md",
                ),
            ),
            local_weekdays=(0, 1, 2, 3, 4),
            local_hour=18,
            local_minute=0,
            trading_calendar="XSHG",
            priority=10,
        ),
        TaskConfig(
            name="cn-weekly",
            workdir=cn_root,
            command=("bash", "scripts/weekly_pipeline.sh", "{logical_date}"),
            completion=CompletionCheck(
                kind="file_exists",
                path_template="reports/{logical_date}_report_weekly_zh.md",
                min_size=200,
            ),
            local_weekdays=(5,),
            local_hour=10,
            local_minute=0,
            priority=15,
        ),
        TaskConfig(
            name="factor-cn-daily",
            workdir=factor_lab_root,
            command=("bash", "scripts/daily_factors.sh", "--market", "cn"),
            completion=CompletionCheck(
                kind="file_contains_all",
                path_template="logs/daily_{local_date_nodash}.log",
                markers=("=== A-Share Factor Lab Research Candidates ===", "Done:"),
            ),
            local_weekdays=(0, 1, 2, 3, 4),
            local_hour=4,
            local_minute=0,
            trading_calendar="XSHG",
            same_day_only=True,
            priority=20,
        ),
        TaskConfig(
            name="factor-us-daily",
            workdir=factor_lab_root,
            command=("bash", "scripts/daily_factors.sh", "--market", "us"),
            completion=CompletionCheck(
                kind="file_contains_all",
                path_template="logs/daily_{local_date_nodash}.log",
                markers=("=== US Factor Lab Research Candidates ===", "Done:"),
            ),
            local_weekdays=(1, 2, 3, 4, 5),
            local_hour=9,
            local_minute=0,
            logical_date_kind="ny",
            trading_calendar="XNYS",
            same_day_only=True,
            priority=20,
        ),
        TaskConfig(
            name="paper-record",
            workdir=factor_lab_root,
            command=("python3", "scripts/paper_trade.py", "record"),
            completion=CompletionCheck(
                kind="file_contains_any",
                path_template="logs/paper_{local_date_nodash}.log",
                markers=("  Recorded ", "  Already recorded ", "  No factor selected for "),
            ),
            local_weekdays=(1, 2, 3, 4, 5),
            local_hour=4,
            local_minute=33,
            logical_date_kind="ny",
            trading_calendar="XNYS",
            same_day_only=True,
            priority=20,
        ),
        TaskConfig(
            name="paper-evaluate",
            workdir=factor_lab_root,
            command=("python3", "scripts/paper_trade.py", "evaluate"),
            completion=CompletionCheck(
                kind="file_contains_any",
                path_template="logs/paper_{local_date_nodash}.log",
                markers=("    Cum:   Long", "  No unevaluated picks found"),
            ),
            local_weekdays=(1, 2, 3, 4, 5),
            local_hour=7,
            local_minute=47,
            logical_date_kind="ny",
            trading_calendar="XNYS",
            same_day_only=True,
            priority=20,
        ),
        TaskConfig(
            name="paper-report",
            workdir=factor_lab_root,
            command=("python3", "scripts/paper_trade.py", "report"),
            completion=CompletionCheck(
                kind="file_contains_any",
                path_template="logs/paper_{local_date_nodash}.log",
                markers=("  Paper Trading Report", "No paper trading data yet."),
            ),
            local_weekdays=(1, 2, 3, 4, 5),
            local_hour=7,
            local_minute=53,
            logical_date_kind="ny",
            trading_calendar="XNYS",
            same_day_only=True,
            priority=20,
        ),
        TaskConfig(
            name="autoresearch-cn-06",
            workdir=factor_lab_root,
            command=("bash", "scripts/autoresearch.sh", "--market", "cn"),
            completion=CompletionCheck(
                kind="file_exists",
                path_template="reports/autoresearch_cn_{local_date_nodash}_06.md",
                min_size=100,
            ),
            local_weekdays=(0, 1, 2, 3, 4),
            local_hour=6,
            local_minute=0,
            trading_calendar="XSHG",
            same_day_only=True,
            priority=30,
        ),
        TaskConfig(
            name="autoresearch-am",
            workdir=factor_lab_root,
            command=("bash", "scripts/autoresearch.sh"),
            completion=CompletionCheck(
                kind="file_contains_all",
                path_template="logs/autoresearch_{local_date_nodash}.log",
                markers=("=== CN Strategy Grid Search ===", "Done:"),
            ),
            local_weekdays=(0, 1, 2, 3, 4),
            local_hour=10,
            local_minute=0,
            trading_calendar="XSHG",
            same_day_only=True,
            priority=30,
        ),
        TaskConfig(
            name="autoresearch-pm",
            workdir=factor_lab_root,
            command=("bash", "scripts/autoresearch.sh"),
            completion=CompletionCheck(
                kind="file_exists",
                path_template="reports/autoresearch_cn_{local_date_nodash}_14.md",
                min_size=100,
            ),
            local_weekdays=(0, 1, 2, 3, 4),
            local_hour=14,
            local_minute=0,
            trading_calendar="XSHG",
            same_day_only=True,
            priority=30,
        ),
        TaskConfig(
            name="factor-maintenance",
            workdir=factor_lab_root,
            command=("python3", "scripts/weekly_maintenance.py", "--days", "250"),
            completion=CompletionCheck(
                kind="file_contains_any",
                path_template="logs/maintenance_{local_date_nodash}.log",
                markers=("  Done:", "rows inserted"),
            ),
            local_weekdays=(5,),
            local_hour=8,
            local_minute=17,
            same_day_only=True,
            priority=40,
        ),
    )


def find_due_runs(
    now_local: datetime,
    *,
    tasks: tuple[TaskConfig, ...],
    lookback_days: int = 3,
) -> list[DueRun]:
    if now_local.tzinfo is None:
        now_local = now_local.replace(tzinfo=LOCAL_TZ)
    else:
        now_local = now_local.astimezone(LOCAL_TZ)

    due: list[DueRun] = []
    start_day = now_local.date() - timedelta(days=lookback_days)
    end_day = now_local.date()

    cur = start_day
    while cur <= end_day:
        for task in tasks:
            if cur.weekday() not in task.local_weekdays:
                continue
            scheduled_at = _scheduled_at_local(cur, task)
            if now_local < scheduled_at + timedelta(minutes=task.grace_minutes):
                continue
            logical_date = scheduled_logical_date(cur, task)
            if not is_trading_day(logical_date, task.trading_calendar):
                continue
            due.append(
                DueRun(
                    task=task,
                    local_day=cur,
                    logical_date=logical_date,
                    scheduled_at_local=scheduled_at,
                )
            )
        cur += timedelta(days=1)

    due.sort(key=lambda item: (item.task.priority, -item.scheduled_at_local.timestamp()))
    return due


def _resolve_path(base_dir: Path, template: str, context: dict[str, str]) -> Path:
    raw = template.format(**context)
    path = Path(raw)
    return path if path.is_absolute() else (base_dir / path)


def task_completed(due: DueRun) -> bool:
    check = due.task.completion
    context = due.context()
    path = _resolve_path(due.task.workdir, check.path_template, context)
    if not path.exists():
        return False
    if check.kind == "file_exists":
        return path.stat().st_size >= check.min_size

    text = path.read_text(encoding="utf-8", errors="replace")
    markers = tuple(marker.format(**context) for marker in check.markers)
    if check.kind == "file_contains_all":
        return all(marker in text for marker in markers)
    if check.kind == "file_contains_any":
        return any(marker in text for marker in markers)
    raise ValueError(f"unsupported completion check kind: {check.kind}")


def is_us_pipeline_running(lock_file: Path = PIPELINE_LOCK_FILE) -> bool:
    if not lock_file.exists():
        return False
    try:
        raw = lock_file.read_text(encoding="utf-8").strip()
        pid = int(raw)
    except (OSError, ValueError):
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def any_managed_pipeline_running() -> bool:
    if is_us_pipeline_running():
        return True

    try:
        output = subprocess.check_output(["ps", "-ef"], text=True, encoding="utf-8")
    except Exception:
        return False

    current_pid = str(os.getpid())
    for line in output.splitlines():
        if current_pid in line or "ps -ef" in line:
            continue
        if any(pattern in line for pattern in GLOBAL_BUSY_PATTERNS):
            return True
    return False


def update_completion_state(
    state: dict[str, Any],
    due_runs: list[DueRun],
    *,
    now_local: datetime,
) -> None:
    runs = state.setdefault("runs", {})
    now_str = now_local.isoformat()
    for due in due_runs:
        if task_completed(due):
            record = runs.setdefault(due.key, {})
            record["completed_at"] = now_str
            record["workdir"] = str(due.task.workdir)


def prune_state(
    state: dict[str, Any],
    *,
    now_local: datetime,
    retention_days: int = DEFAULT_STATE_RETENTION_DAYS,
) -> None:
    cutoff = now_local - timedelta(days=retention_days)
    runs = state.setdefault("runs", {})
    stale_keys: list[str] = []
    for key, record in runs.items():
        raw = record.get("completed_at") or record.get("last_triggered_at")
        if not raw:
            continue
        try:
            when = datetime.fromisoformat(raw)
        except ValueError:
            stale_keys.append(key)
            continue
        if when.tzinfo is None:
            when = when.replace(tzinfo=LOCAL_TZ)
        else:
            when = when.astimezone(LOCAL_TZ)
        if when < cutoff:
            stale_keys.append(key)
    for key in stale_keys:
        runs.pop(key, None)


def eligible_due_run(
    due_runs: list[DueRun],
    *,
    now_local: datetime,
    state: dict[str, Any],
    pipeline_running: bool,
    max_auto_triggers: int,
) -> DueRun | None:
    if pipeline_running:
        return None

    runs = state.setdefault("runs", {})
    today = now_local.date()
    for due in due_runs:
        if task_completed(due):
            continue
        if due.task.same_day_only and due.local_day != today:
            continue
        record = runs.get(due.key, {})
        if int(record.get("trigger_count", 0)) >= max_auto_triggers:
            continue
        return due
    return None


def trigger_due_run(
    due: DueRun,
    *,
    state: dict[str, Any],
    now_local: datetime,
    watcher_log_dir: Path,
) -> Path:
    watcher_log_dir.mkdir(parents=True, exist_ok=True)
    log_path = watcher_log_dir / f"{due.task.name}_{due.local_day.isoformat()}.log"
    context = due.context()
    cmd = [part.format(**context) for part in due.task.command]

    with log_path.open("ab") as fh:
        banner = (
            f"\n\n=== watchdog trigger {now_local.isoformat()} "
            f"{due.task.name} local={due.local_day.isoformat()} "
            f"logical={due.logical_date.isoformat()} ===\n"
        )
        fh.write(banner.encode("utf-8"))
        fh.flush()
        subprocess.Popen(
            cmd,
            cwd=due.task.workdir,
            stdin=subprocess.DEVNULL,
            stdout=fh,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    record = state.setdefault("runs", {}).setdefault(due.key, {})
    record["trigger_count"] = int(record.get("trigger_count", 0)) + 1
    record["last_triggered_at"] = now_local.isoformat()
    record["watchdog_log_path"] = str(log_path)
    record["workdir"] = str(due.task.workdir)
    return log_path


def due_status(due: DueRun, *, now_local: datetime) -> str:
    if task_completed(due):
        return "complete"
    if due.task.same_day_only and due.local_day != now_local.date():
        return "expired"
    return "ready"


def run_watchdog(
    project_dir: Path,
    *,
    now_local: datetime | None = None,
    dry_run: bool = False,
    lookback_days: int = 3,
    max_auto_triggers: int = 1,
    state_path: Path | None = None,
    tasks: tuple[TaskConfig, ...] | None = None,
) -> list[str]:
    now_local = now_local or datetime.now(LOCAL_TZ)
    if now_local.tzinfo is None:
        now_local = now_local.replace(tzinfo=LOCAL_TZ)
    else:
        now_local = now_local.astimezone(LOCAL_TZ)

    state_path = state_path or default_state_path(project_dir)
    tasks = tasks or build_default_tasks(project_dir)
    state = load_state(state_path)
    due_runs = find_due_runs(
        now_local,
        tasks=tasks,
        lookback_days=lookback_days,
    )
    update_completion_state(state, due_runs, now_local=now_local)
    prune_state(state, now_local=now_local)

    messages: list[str] = []
    if not due_runs:
        messages.append("watchdog: no due runs")
        save_state(state_path, state)
        return messages

    for due in due_runs:
        status = due_status(due, now_local=now_local)
        messages.append(
            "watchdog: "
            f"{due.task.name} local={due.local_day.isoformat()} "
            f"logical={due.logical_date.isoformat()} "
            f"scheduled={due.scheduled_at_local.isoformat()} status={status}"
        )

    running = any_managed_pipeline_running()
    if running:
        messages.append("watchdog: managed pipeline already running; skip retrigger")
        save_state(state_path, state)
        return messages

    candidate = eligible_due_run(
        due_runs,
        now_local=now_local,
        state=state,
        pipeline_running=running,
        max_auto_triggers=max_auto_triggers,
    )
    if candidate is None:
        messages.append("watchdog: no eligible missing run")
        save_state(state_path, state)
        return messages

    if dry_run:
        messages.append(
            "watchdog: dry-run would trigger "
            f"{candidate.task.name} local={candidate.local_day.isoformat()} "
            f"logical={candidate.logical_date.isoformat()}"
        )
        save_state(state_path, state)
        return messages

    watcher_log_dir = project_dir / "logs" / "watchdog"
    log_path = trigger_due_run(
        candidate,
        state=state,
        now_local=now_local,
        watcher_log_dir=watcher_log_dir,
    )
    messages.append(
        "watchdog: triggered "
        f"{candidate.task.name} local={candidate.local_day.isoformat()} "
        f"logical={candidate.logical_date.isoformat()} "
        f"log={log_path}"
    )
    save_state(state_path, state)
    return messages
