"""Shared Codex backend for the US/CN daily-report narrators.

Every narrator/extractor call shells out to the local ``codex`` CLI running
GPT-5.5 with high reasoning effort. Delivery is fail-closed: non-Codex
backends and automatic LLM fallbacks are intentionally disabled for production
reports.

codex specifics (mirrors the proven invocation in
``quant-research-v1/scripts/run_agents.sh``):
    codex exec -m $CODEX_MODEL -c model_reasoning_effort="$EFFORT" \
      --sandbox read-only --color never --skip-git-repo-check --ephemeral \
      -C <root> -o <outfile> -   < prompt(on stdin)
The final agent message is read back from the ``-o`` file (clean text, no
event chatter).

env knobs:
  QUANT_NARRATOR_BACKEND   codex only                  (default codex)
  CODEX_BIN                codex binary                (default "codex")
  CODEX_MODEL              model id                    (default "gpt-5.5")
  CODEX_REASONING_EFFORT   minimal|low|medium|high|xhigh (default "high")
  NARRATOR_CONCURRENCY     max parallel codex execs    (default 3)
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def backend() -> str:
    selected = (os.environ.get("QUANT_NARRATOR_BACKEND") or "codex").strip().lower()
    if selected != "codex":
        raise RuntimeError(
            "Daily report narrators are Codex-agent-only; "
            f"QUANT_NARRATOR_BACKEND={selected!r} is not allowed."
        )
    return "codex"


def concurrency() -> int:
    try:
        return max(1, int(os.environ.get("NARRATOR_CONCURRENCY", "3")))
    except ValueError:
        return 3


# Prepended to every codex prompt: codex exec spins a full agent that *could*
# try to use tools / poke the repo. We only want narrative text back.
_CODEX_GUARD = (
    "You are a financial report writer. Produce ONLY the requested report text "
    "as your final message. Do not run any commands, do not use any tools, do "
    "not read or write files, do not explain what you are doing. Output the "
    "report content directly.\n\n"
)


def call_codex(
    system: str,
    user: str,
    *,
    label: str = "codex",
    timeout: int = 900,
    model: str | None = None,
    effort: str | None = None,
) -> str | None:
    """Run one codex-exec call. Returns final message text, or None on failure."""
    codex_bin = os.environ.get("CODEX_BIN", "codex")
    model = model or os.environ.get("CODEX_MODEL", "gpt-5.5")
    effort = effort or os.environ.get("CODEX_REASONING_EFFORT", "high")
    prompt = f"{_CODEX_GUARD}{system}\n\n{user}"

    out_fd, out_path = tempfile.mkstemp(prefix="narrator_codex_", suffix=".md")
    os.close(out_fd)
    try:
        cmd = [
            codex_bin, "exec",
            "-m", model,
            "-c", f'model_reasoning_effort="{effort}"',
            "--sandbox", "read-only",
            "--color", "never",
            "--skip-git-repo-check",
            "--ephemeral",
            "-C", str(ROOT),
            "-o", out_path,
            "-",
        ]
        proc = subprocess.run(
            cmd,
            input=prompt,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        content = ""
        try:
            content = Path(out_path).read_text(encoding="utf-8").strip()
        except OSError:
            content = ""
        if proc.returncode != 0 or not content:
            tail = (proc.stderr or "")[-300:]
            print(
                f"  [warn] codex call '{label}' failed: rc={proc.returncode} "
                f"bytes={len(content)} stderr={tail}",
                file=sys.stderr,
            )
            return content or None
        return content
    except subprocess.TimeoutExpired:
        print(f"  [warn] codex call '{label}' timed out after {timeout}s", file=sys.stderr)
        return None
    except OSError as e:
        print(f"  [warn] codex launch '{label}' failed: {e}", file=sys.stderr)
        return None
    finally:
        try:
            os.unlink(out_path)
        except OSError:
            pass


def call_llm(
    system: str,
    user: str,
    *,
    label: str = "llm",
    temperature: float = 0.3,
    max_tokens: int = 1500,
    timeout: int = 900,
) -> str | None:
    """Codex-only LLM call. Returns None on Codex failure; no LLM fallback.

    `temperature` and `max_tokens` remain in the signature so older narrator
    call sites do not need a noisy mechanical rewrite; they are ignored by the
    Codex backend.
    """
    backend()
    return call_codex(system, user, label=label, timeout=timeout)


if __name__ == "__main__":  # smoke test
    txt = call_codex(
        "你是金融写手。只输出一句中文市场点评。",
        "主题:科技股情绪偏热。",
        label="smoke",
        timeout=180,
    )
    print("RESULT:", txt)
    sys.exit(0 if txt else 1)
