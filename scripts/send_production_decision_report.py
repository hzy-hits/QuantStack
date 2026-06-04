#!/usr/bin/env python3
"""Generate and send the Main Strategy V2 final market report.

This is the cron-facing delivery hook for market-specific final reports under
reports/review_dashboard/main_strategy_v2/{date}/. The combined backtest report
is an internal review artifact and is not deliverable in production.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml


STACK_ROOT = Path(__file__).resolve().parents[1]
QUANT_V1_ROOT = STACK_ROOT / "quant-research-v1"
QUANT_V1_SRC = QUANT_V1_ROOT / "src"
if str(QUANT_V1_SRC) not in sys.path:
    sys.path.insert(0, str(QUANT_V1_SRC))

from quant_bot.delivery.gmail import send_report_email  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send Main Strategy V2 final market report.")
    parser.add_argument("--date", required=True, help="Report date, YYYY-MM-DD.")
    parser.add_argument("--start", default="2026-03-01", help="Backtest start date.")
    parser.add_argument("--session", default="morning", help="Session label for subject, e.g. morning/evening/pre/post.")
    parser.add_argument("--market", choices=["all", "cn", "us"], default="all", help="Audience/legacy path to replace.")
    parser.add_argument("--delivery-mode", choices=["test", "prod"], default=os.environ.get("QUANT_DELIVERY_MODE", "test"))
    parser.add_argument("--test-recipient", default=os.environ.get("QUANT_TEST_RECIPIENT"))
    parser.add_argument("--skip-generate", action="store_true", help="Send an already-generated report.")
    parser.add_argument("--delivery-dry-run", action="store_true", help="Generate and resolve recipients, but skip Gmail.")
    parser.add_argument("--dry-run", action="store_true", help="Print what would happen; skip generation and Gmail.")
    return parser.parse_args()


def _split_recipients(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _list_config_recipients(value: object) -> list[str]:
    if isinstance(value, str):
        return _split_recipients(value)
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _reporting_config() -> dict[str, Any]:
    path = QUANT_V1_ROOT / "config.yaml"
    if not path.exists():
        return {}
    cfg = yaml.safe_load(path.read_text()) or {}
    reporting = cfg.get("reporting", {})
    return reporting if isinstance(reporting, dict) else {}


def _resolve_test_recipients(test_recipient: str | None) -> tuple[list[str], str]:
    override = _split_recipients(test_recipient) or _split_recipients(os.environ.get("QUANT_TEST_RECIPIENT"))
    if override:
        return override, "override"

    reporting = _reporting_config()
    configured = _list_config_recipients(reporting.get("test_recipients"))
    if not configured:
        configured = _list_config_recipients(reporting.get("test_recipient"))
    if configured:
        return configured, "config.reporting.test_recipients"

    raise SystemExit(
        "Test delivery needs --test-recipient, QUANT_TEST_RECIPIENT, or reporting.test_recipients."
    )


def _prod_recipient_count() -> int:
    return len(_list_config_recipients(_reporting_config().get("recipients")))


def _main_strategy_report_dir(as_of: str) -> Path:
    return STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2" / as_of


def agent_report_path(as_of: str, market: str) -> Path:
    base = _main_strategy_report_dir(as_of)
    if market == "cn":
        return base / "cn_daily_report_agent.md"
    if market == "us":
        return base / "us_daily_report_agent.md"
    raise ValueError(f"agent report only supports cn/us, got {market!r}")


def programmatic_report_path(as_of: str, market: str) -> Path:
    base = _main_strategy_report_dir(as_of)
    if market == "cn":
        return base / "cn_daily_report.md"
    if market == "us":
        return base / "us_daily_report.md"
    return base / "main_strategy_v2_backtest.md"


def report_path(as_of: str, market: str = "all") -> Path:
    """Resolve the markdown the email delivers.

    US/CN delivery is Codex-agent-only. The programmatic markdown remains the
    narrator input and debugging artifact, but it is no longer an email fallback.
    """
    base = STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2" / as_of
    if market in {"cn", "us"}:
        return agent_report_path(as_of, market)
    return base / "main_strategy_v2_backtest.md"


def report_json_path(as_of: str) -> Path:
    return STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2" / as_of / "main_strategy_v2_backtest.json"


def _renderer_module() -> Any:
    path = STACK_ROOT / "scripts" / "generate_main_strategy_v2_report.py"
    spec = importlib.util.spec_from_file_location("main_strategy_v2_renderer", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load report renderer: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _validator_module() -> Any:
    path = STACK_ROOT / "scripts" / "validate_main_strategy_v2_reports.py"
    spec = importlib.util.spec_from_file_location("main_strategy_v2_validator", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load report validator: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def render_standalone_from_json(as_of: str, market: str) -> Path:
    if market not in {"cn", "us"}:
        raise ValueError(f"standalone rendering only supports cn/us, got {market!r}")
    payload_path = report_json_path(as_of)
    if not payload_path.exists():
        raise FileNotFoundError(f"main strategy payload not found: {payload_path}")
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    renderer = _renderer_module()
    if market == "cn":
        text = renderer.render_cn_standalone_report(payload)
    else:
        text = renderer.render_us_standalone_report(payload)
    path = programmatic_report_path(as_of, market)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def generate_report(as_of: str, start: str) -> None:
    # Refresh the bubble-hedge + risk-regime artifacts first so the report's
    # Hedge/Wedge/Confirm/Press gate reflects today's tape. These run on a
    # different cron cadence; refreshing here keeps the emailed report fresh
    # regardless of when the radar cron last fired. Non-fatal if they fail —
    # run_main_strategy_v2 degrades the gate to 1.0x when artifacts are stale.
    for label, script in (
        ("bubble hedge radar", "score_bubble_hedge_radar.py"),
        ("capitulation radar", "score_capitulation_radar.py"),
        ("capitulation convex radar", "score_capitulation_convex_radar.py"),
        ("risk regime engine", "score_risk_regime_engine.py"),
        ("cn risk regime", "score_cn_risk_regime.py"),
    ):
        try:
            subprocess.run(
                [sys.executable, str(STACK_ROOT / "scripts" / script), "--as-of", as_of],
                cwd=STACK_ROOT,
                check=True,
            )
        except subprocess.CalledProcessError as exc:  # noqa: PERF203
            print(f"warn: {label} refresh failed ({exc}); report will use stale/default gate")

    # Pin --ai-infra-mode explicitly. generate_main_strategy_v2_report.py infers
    # enforce_expand only when default DBs are used; passing it here removes
    # the implicit dependency so the emailed report always carries the
    # AI-infra production basket (the whole point of the daily report).
    subprocess.run(
        [
            sys.executable,
            str(STACK_ROOT / "scripts" / "generate_main_strategy_v2_report.py"),
            "--date",
            as_of,
            "--start",
            start,
            "--ai-infra-mode",
            "enforce_expand",
        ],
        cwd=STACK_ROOT,
        check=True,
    )


def session_label(session: str) -> str:
    normalized = session.lower()
    if normalized in {"pre", "morning"}:
        return "盘前"
    elif normalized in {"post", "evening"}:
        return "盘后"
    return session


def subject_for(as_of: str, session: str, market: str) -> str:
    label = session_label(session)
    if market == "cn":
        return f"A股量化研究{label}日报 — {as_of}"
    if market == "us":
        return f"美股量化研究{label}日报 — {as_of}"
    return f"量化{label}日报 — {as_of}"


def legacy_report_path(as_of: str, session: str, market: str) -> Path | None:
    normalized = session.lower()
    if market == "cn":
        if normalized in {"pre", "morning"}:
            slot = "morning"
        elif normalized in {"post", "evening"}:
            slot = "evening"
        elif normalized == "daily":
            return STACK_ROOT / "quant-research-cn" / "reports" / f"{as_of}_report_zh.md"
        else:
            slot = normalized
        return STACK_ROOT / "quant-research-cn" / "reports" / f"{as_of}_report_zh_{slot}.md"
    if market == "us":
        if normalized in {"pre", "morning"}:
            slot = "pre"
        elif normalized in {"post", "evening", "daily"}:
            slot = "post"
        else:
            slot = normalized
        return STACK_ROOT / "quant-research-v1" / "reports" / f"{as_of}_report_zh_{slot}.md"
    return None


def replacement_report_text(text: str, as_of: str, session: str, market: str) -> str:
    if market not in {"cn", "us"}:
        return text
    if text.startswith("# A股量化日报") or text.startswith("# 美股量化日报"):
        return text
    title_market = "A股" if market == "cn" else "美股"
    title = f"# {title_market}量化研究{session_label(session)}日报 - {as_of}"
    body = text
    if body.startswith("# Main Strategy V2 Backtest"):
        body = "\n".join(body.splitlines()[2:]).lstrip()
    body = body.replace("## 今日交易决策 / Production Decision", "## 今日交易清单")
    note = "> 新日报已替代旧 agent 报告；可交易名单、观察名单和风险说明分开读。"
    return f"{title}\n\n{note}\n\n{body}"


def validate_market_report_scope(path: Path, market: str) -> None:
    if market not in {"cn", "us"}:
        return
    text = path.read_text(encoding="utf-8")
    if market == "us":
        required_prefix = "# 美股量化日报"
        banned = [
            "# A股",
            "A股执行",
            "A 股",
            "| CN |",
            "CN stock basket",
            "cn_tape_",
            "cn_oversold",
            "cn_observed",
        ]
    else:
        required_prefix = "# A股量化日报"
        banned = [
            "# 美股",
            "| US |",
            "US stock trades",
            "us_theme_",
            "US options",
        ]
    if not text.startswith(required_prefix):
        raise RuntimeError(f"refusing to send {market} report with wrong title: {path}")
    found = [marker for marker in banned if marker in text]
    if found:
        raise RuntimeError(
            f"refusing to send {market} report with cross-market content: {path}; markers={found[:5]}"
        )


def materialize_legacy_report(as_of: str, session: str, market: str, source: Path) -> Path:
    target = legacy_report_path(as_of, session, market)
    if target is None:
        return source
    target.parent.mkdir(parents=True, exist_ok=True)
    text = source.read_text(encoding="utf-8")
    target.write_text(replacement_report_text(text, as_of, session, market), encoding="utf-8")
    return target


def load_headline(as_of: str, market: str) -> str:
    path = report_json_path(as_of)
    if not path.exists():
        return "-"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "-"
    summary = ((payload.get("production_decision_summary") or {}).get("summary") or {})
    if market == "us":
        return f"US-only report: US stock R={summary.get('us_r', '-')}; CN omitted from this email."
    if market == "cn":
        return f"CN-only report: CN stock R={summary.get('cn_r', '-')}; US omitted from this email."
    return summary.get("headline") or "-"


_NARRATOR_SPECS = {
    "us": ("run_us_narrator.py", "us_daily_report_agent.md", "QUANT_DISABLE_US_NARRATOR"),
    "cn": ("run_cn_narrator.py", "cn_daily_report_agent.md", "QUANT_DISABLE_CN_NARRATOR"),
}


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes"}


def _agent_meta_path(agent_md: Path) -> Path:
    return agent_md.with_name(agent_md.name + ".meta.json")


def _markdown_table_count(text: str) -> int:
    lines = text.splitlines()
    count = 0
    for idx, line in enumerate(lines[:-1]):
        cur = line.strip()
        nxt = lines[idx + 1].strip()
        if cur.startswith("|") and cur.endswith("|") and nxt.startswith("|") and set(nxt.replace("|", "").strip()) <= {"-", ":"}:
            count += 1
    return count


def _is_structured_us_agent_report(agent_md: Path) -> bool:
    try:
        text = agent_md.read_text(encoding="utf-8")
    except OSError:
        return False
    required = [
        "# 美股量化日报",
        "## 一句话",
        "## 市场状态",
        "## 今日交易清单",
        "## 观察与风险",
        "## 催化与复核",
        "## 附注",
        "Production",
        "IV/HV",
        "Gamma",
    ]
    if any(marker not in text for marker in required):
        return False
    return _markdown_table_count(text) >= 4


def _agent_report_matches_payload(agent_md: Path) -> bool:
    if agent_md.name != "us_daily_report_agent.md":
        return True
    payload_path = agent_md.parent / "main_strategy_v2_backtest.json"
    if not payload_path.exists():
        return True
    try:
        validator = _validator_module()
        payload = validator.load_json(payload_path)
        text = agent_md.read_text(encoding="utf-8")
    except (OSError, RuntimeError, SystemExit):
        return False
    failures = validator.validate_us_report_text_against_payload(payload, text, agent_md.name)
    return not failures


def _is_fresh_codex_agent_report(agent_md: Path, as_of: str) -> bool:
    if not agent_md.exists() or agent_md.stat().st_size <= 0:
        return False
    meta_path = _agent_meta_path(agent_md)
    if not meta_path.exists() or meta_path.stat().st_size <= 0:
        return False
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if meta.get("backend") != "codex" or meta.get("as_of") != as_of:
        return False
    from datetime import date as _date, datetime as _dt

    mtime = _dt.fromtimestamp(agent_md.stat().st_mtime).date()
    if mtime not in {_date.fromisoformat(as_of), _date.today()}:
        return False
    if agent_md.name == "us_daily_report_agent.md" and not _is_structured_us_agent_report(agent_md):
        return False
    if not _agent_report_matches_payload(agent_md):
        return False
    return True


def _assert_codex_backend_env() -> None:
    selected = (os.environ.get("QUANT_NARRATOR_BACKEND") or "codex").strip().lower()
    if selected != "codex":
        raise RuntimeError(
            "US/CN report delivery is Codex-agent-only; "
            f"QUANT_NARRATOR_BACKEND={selected!r} is not allowed."
        )


def _ensure_narrator(as_of: str, market: str) -> None:
    """Run the Codex/GPT-5.5 agent narrator for `market` if needed.

    Fail closed: US/CN delivery refuses to send unless the final markdown exists
    and its sidecar metadata proves it came from the Codex narrator for `as_of`.
    """
    spec = _NARRATOR_SPECS.get(market)
    if not spec:
        return
    _assert_codex_backend_env()
    script_name, agent_name, disable_env = spec
    if _truthy_env(disable_env):
        raise RuntimeError(
            f"{market} Codex narrator is disabled by {disable_env}; refusing to send a fallback report."
        )
    agent_md = (
        STACK_ROOT / "reports" / "review_dashboard" / "main_strategy_v2"
        / as_of / agent_name
    )
    if _is_fresh_codex_agent_report(agent_md, as_of):
        return
    narrator = STACK_ROOT / "scripts" / "agents" / script_name
    if not narrator.exists():
        raise FileNotFoundError(f"{market} Codex narrator script missing: {narrator}")
    timeout = int(os.environ.get(f"{market.upper()}_NARRATOR_TIMEOUT", "900"))
    try:
        result = subprocess.run(
            [sys.executable, str(narrator), "--date", as_of],
            cwd=STACK_ROOT,
            timeout=timeout,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            tail = ((result.stderr or "") + "\n" + (result.stdout or ""))[-1000:]
            raise RuntimeError(f"{market} Codex narrator failed with exit={result.returncode}: {tail}")
        print(f"[narrator:{market}] {result.stdout.splitlines()[-1] if result.stdout else 'ok'}")
    except subprocess.TimeoutExpired:
        raise TimeoutError(f"{market} Codex narrator timed out after {timeout}s; refusing fallback report")
    except OSError as e:
        raise RuntimeError(f"{market} Codex narrator launch failed: {e}") from e
    if not _is_fresh_codex_agent_report(agent_md, as_of):
        raise RuntimeError(
            f"{market} Codex narrator did not produce a verified agent report: {agent_md}"
        )


def _ensure_us_narrator(as_of: str) -> None:
    _ensure_narrator(as_of, "us")


def validate_main_strategy_contract(as_of: str, market: str, extra_paths: list[Path] | None = None) -> None:
    validator = _validator_module()
    report_dir = _main_strategy_report_dir(as_of)
    failures = validator.validate_report_dir(report_dir)
    extra_paths = extra_paths or []
    if market == "us":
        payload = validator.load_json(report_json_path(as_of))
        seen: set[Path] = set()
        for path in extra_paths:
            if path in seen or not path.exists():
                continue
            seen.add(path)
            text = path.read_text(encoding="utf-8")
            try:
                label = str(path.relative_to(STACK_ROOT))
            except ValueError:
                label = str(path)
            failures.extend(validator.validate_us_report_text_against_payload(payload, text, label))
    if failures:
        details = "\n".join(f"- {failure.code}: {failure.detail}" for failure in failures[:20])
        more = "" if len(failures) <= 20 else f"\n... {len(failures) - 20} more"
        raise RuntimeError(
            f"refusing to send {market} report; Main Strategy V2 contract failed:\n{details}{more}"
        )


def main() -> None:
    args = parse_args()
    if args.delivery_mode == "prod" and args.market == "all" and not args.dry_run:
        raise SystemExit(
            "prod delivery requires --market cn or --market us; "
            "main_strategy_v2_backtest.md is an internal kitchen ticket, not an email report."
        )
    subject = subject_for(args.date, args.session, args.market)

    if args.dry_run:
        print(f"Dry run: would generate {report_path(args.date, args.market)}")
    elif not args.skip_generate:
        generate_report(args.date, args.start)

    # For US/CN emails, ensure the Codex agent narrator output exists. Run AFTER
    # generate so the programmatic <market>_daily_report.md narrator input is
    # present. There is intentionally no programmatic markdown fallback here.
    if args.market in {"us", "cn"} and not args.dry_run:
        _ensure_narrator(args.date, args.market)
    path = report_path(args.date, args.market)

    if not args.dry_run and not path.exists():
        raise FileNotFoundError(f"production decision report not found: {path}")
    if not args.dry_run:
        validate_market_report_scope(path, args.market)
    effective_path = path
    if not args.dry_run:
        effective_path = materialize_legacy_report(args.date, args.session, args.market, path)
        validate_market_report_scope(effective_path, args.market)
        validate_main_strategy_contract(args.date, args.market, [path, effective_path])

    if args.delivery_mode == "test":
        recipients, source = _resolve_test_recipients(args.test_recipient)
        send_to = recipients[0]
        send_bcc = recipients[1:]
        effective_subject = f"[TEST] {subject}"
        delivery_note = f"test recipients from {source}: {len(recipients)}"
    else:
        send_to = None
        send_bcc = None
        effective_subject = subject
        delivery_note = f"prod recipients from config.reporting.recipients: {_prod_recipient_count()}"

    print(f"Report: {effective_path}")
    if effective_path != path:
        print(f"Source: {path}")
    print(f"Subject: {effective_subject}")
    print(f"Delivery: {args.delivery_mode} ({delivery_note})")
    print(f"Headline: {load_headline(args.date, args.market)}")

    if args.dry_run or args.delivery_dry_run:
        print("Gmail send skipped")
        return

    msg_ids = send_report_email(
        report_path=effective_path,
        chart_paths=[],
        to=send_to,
        bcc=send_bcc,
        subject=effective_subject,
        credentials_path=QUANT_V1_ROOT / "credentials.json",
        token_path=QUANT_V1_ROOT / "token.json",
        config_path=str(QUANT_V1_ROOT / "config.yaml"),
    )
    print(f"Sent production decision report: {len(msg_ids)} message(s) {','.join(msg_ids)}")


if __name__ == "__main__":
    main()
