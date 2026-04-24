from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from quant_bot.orchestration.watchdog import (
    CompletionCheck,
    DueRun,
    LOCAL_TZ,
    TaskConfig,
    eligible_due_run,
    find_due_runs,
    scheduled_logical_date,
    task_completed,
    update_completion_state,
)


def _task(
    tmp_path: Path,
    *,
    name: str,
    completion: CompletionCheck,
    local_hour: int = 8,
    local_minute: int = 30,
    logical_date_kind: str = "local",
    same_day_only: bool = False,
) -> TaskConfig:
    return TaskConfig(
        name=name,
        workdir=tmp_path,
        command=("echo", "noop"),
        completion=completion,
        local_weekdays=(0, 1, 2, 3, 4, 5, 6),
        local_hour=local_hour,
        local_minute=local_minute,
        logical_date_kind=logical_date_kind,
        same_day_only=same_day_only,
    )


def test_scheduled_logical_date_maps_postmarket_to_prior_new_york_day(tmp_path: Path):
    task = _task(
        tmp_path,
        name="us-postmarket",
        completion=CompletionCheck("file_exists", "reports/{logical_date}_report_zh_post.md"),
        local_hour=5,
        local_minute=0,
        logical_date_kind="ny",
    )

    assert scheduled_logical_date(date(2026, 4, 16), task) == date(2026, 4, 15)


def test_scheduled_logical_date_maps_us_weekly_to_friday_new_york_day(tmp_path: Path):
    task = _task(
        tmp_path,
        name="us-weekly",
        completion=CompletionCheck("file_exists", "reports/{logical_date}_report_weekly_zh.md"),
        local_hour=9,
        local_minute=30,
        logical_date_kind="ny",
    )

    assert scheduled_logical_date(date(2026, 4, 18), task) == date(2026, 4, 17)


def test_find_due_runs_returns_latest_missing_postmarket(tmp_path: Path):
    task = _task(
        tmp_path,
        name="us-postmarket",
        completion=CompletionCheck("file_exists", "reports/{logical_date}_report_zh_post.md"),
        local_hour=5,
        local_minute=0,
        logical_date_kind="ny",
    )
    now_local = datetime(2026, 4, 16, 8, 0, tzinfo=LOCAL_TZ)

    due_runs = find_due_runs(now_local, tasks=(task,), lookback_days=1)

    assert len(due_runs) == 2
    due = due_runs[0]
    assert due.logical_date == date(2026, 4, 15)
    assert due.local_day == date(2026, 4, 16)


def test_task_completed_uses_log_markers(tmp_path: Path):
    log_dir = tmp_path / "reports" / "logs"
    log_dir.mkdir(parents=True)
    log_path = log_dir / "2026-04-16_morning.log"
    log_path.write_text(
        "Pipeline complete: 2026-04-16 09:01\nReport: reports/2026-04-16_report_zh.md\n",
        encoding="utf-8",
    )
    task = _task(
        tmp_path,
        name="cn-morning",
        completion=CompletionCheck(
            "file_contains_all",
            "reports/logs/{logical_date}_morning.log",
            markers=("Pipeline complete:", "Report: reports/{logical_date}_report_zh.md"),
        ),
    )
    due = DueRun(
        task=task,
        local_day=date(2026, 4, 16),
        logical_date=date(2026, 4, 16),
        scheduled_at_local=datetime(2026, 4, 16, 8, 30, tzinfo=LOCAL_TZ),
    )

    assert task_completed(due) is True


def test_update_completion_state_marks_existing_artifact_complete(tmp_path: Path):
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True)
    report_path = reports_dir / "2026-04-15_report_zh_post.md"
    report_path.write_text("ok", encoding="utf-8")
    task = _task(
        tmp_path,
        name="us-postmarket",
        completion=CompletionCheck("file_exists", "reports/{logical_date}_report_zh_post.md"),
        logical_date_kind="ny",
        local_hour=5,
        local_minute=0,
    )
    due = DueRun(
        task=task,
        local_day=date(2026, 4, 16),
        logical_date=date(2026, 4, 15),
        scheduled_at_local=datetime(2026, 4, 16, 5, 0, tzinfo=LOCAL_TZ),
    )
    state = {"runs": {}}

    update_completion_state(
        state,
        [due],
        now_local=datetime(2026, 4, 16, 8, 0, tzinfo=LOCAL_TZ),
    )

    assert "completed_at" in state["runs"][due.key]


def test_eligible_due_run_skips_expired_same_day_only_task(tmp_path: Path):
    task = _task(
        tmp_path,
        name="factor-us-daily",
        completion=CompletionCheck(
            "file_contains_all",
            "logs/daily_{local_date_nodash}.log",
            markers=("=== US Factor Lab Research Candidates ===", "Done:"),
        ),
        local_hour=9,
        local_minute=0,
        same_day_only=True,
    )
    due = DueRun(
        task=task,
        local_day=date(2026, 4, 15),
        logical_date=date(2026, 4, 15),
        scheduled_at_local=datetime(2026, 4, 15, 9, 0, tzinfo=LOCAL_TZ),
    )
    candidate = eligible_due_run(
        [due],
        now_local=datetime(2026, 4, 16, 9, 30, tzinfo=LOCAL_TZ),
        state={"runs": {}},
        pipeline_running=False,
        max_auto_triggers=1,
    )

    assert candidate is None
