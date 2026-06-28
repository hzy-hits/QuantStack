from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

from quant_bot.orchestration.watchdog import (
    build_default_tasks,
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

REPO_ROOT = Path(__file__).resolve().parents[1]


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


def test_root_runner_us_postmarket_missing_report_is_due(tmp_path: Path, monkeypatch):
    stack_root = tmp_path / "quant-stack"
    us_root = stack_root / "quant-research-v1"
    (us_root / "reports").mkdir(parents=True)
    (stack_root / "quant-research-cn").mkdir(parents=True)
    monkeypatch.setenv("QUANT_STACK_ROOT", str(stack_root))

    tasks = {task.name: task for task in build_default_tasks(us_root)}
    task = tasks["us-postmarket"]
    assert task.workdir == stack_root
    assert task.completion.path_template == "quant-research-v1/reports/{logical_date}_report_zh_post.md"
    assert task.completion.state_task_id == "us.postmarket"
    assert task.command == ("ops/run_task.sh", "us.postmarket", "--date", "{logical_date}")

    now_local = datetime(2026, 5, 7, 8, 0, tzinfo=LOCAL_TZ)
    due = [
        item
        for item in find_due_runs(now_local, tasks=(task,), lookback_days=1)
        if item.logical_date == date(2026, 5, 6)
    ][0]
    assert task_completed(due) is False

    report = stack_root / "quant-research-v1" / "reports" / "2026-05-06_report_zh_post.md"
    report.write_text("x" * 300, encoding="utf-8")
    assert task_completed(due) is True


def test_task_completed_rejects_report_when_delivery_failure_is_unresolved(tmp_path: Path):
    reports_dir = tmp_path / "quant-research-v1" / "reports"
    reports_dir.mkdir(parents=True)
    report = reports_dir / "2026-05-06_report_zh_post.md"
    report.write_text("x" * 300, encoding="utf-8")

    state_dir = tmp_path / "ops" / "state"
    state_dir.mkdir(parents=True)
    (state_dir / "us.postmarket.last_success.json").write_text(
        json.dumps({"finished_at": "2026-05-07T00:10:00+00:00"}),
        encoding="utf-8",
    )
    (state_dir / "us.postmarket.last_failure.json").write_text(
        json.dumps({"finished_at": "2026-05-07T00:50:00+00:00"}),
        encoding="utf-8",
    )

    task = _task(
        tmp_path,
        name="us-postmarket",
        completion=CompletionCheck(
            "file_exists",
            "quant-research-v1/reports/{logical_date}_report_zh_post.md",
            min_size=200,
            state_task_id="us.postmarket",
        ),
        local_hour=5,
        local_minute=0,
        logical_date_kind="ny",
    )
    due = DueRun(
        task=task,
        local_day=date(2026, 5, 7),
        logical_date=date(2026, 5, 6),
        scheduled_at_local=datetime(2026, 5, 7, 5, 0, tzinfo=LOCAL_TZ),
    )

    assert task_completed(due) is False

    (state_dir / "us.postmarket.last_success.json").write_text(
        json.dumps({"finished_at": "2026-05-07T01:10:00+00:00"}),
        encoding="utf-8",
    )
    assert task_completed(due) is True


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


def test_default_cn_tasks_use_slot_specific_report_files():
    tasks = {task.name: task for task in build_default_tasks(REPO_ROOT)}

    morning = tasks["cn-morning"].completion
    assert morning.kind == "file_exists"
    assert morning.path_template == "quant-research-cn/reports/{logical_date}_report_zh_morning.md"

    evening = tasks["cn-evening"].completion
    assert evening.kind == "file_exists"
    assert evening.path_template == "quant-research-cn/reports/{logical_date}_report_zh_evening.md"


def test_default_tasks_do_not_schedule_autoresearch():
    tasks = {task.name for task in build_default_tasks(REPO_ROOT)}

    assert "autoresearch-cn-06" not in tasks
    assert "autoresearch-am" not in tasks
    assert "autoresearch-pm" not in tasks


def test_default_tasks_do_not_schedule_retired_factor_lab_or_paper_tasks():
    tasks = {task.name for task in build_default_tasks(REPO_ROOT)}

    assert "factor-cn-daily" not in tasks
    assert "factor-us-daily" not in tasks
    assert "paper-record" not in tasks
    assert "paper-evaluate" not in tasks
    assert "paper-report" not in tasks
    assert "factor-maintenance" not in tasks


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
