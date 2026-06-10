"""Shared LLM backend for the US/CN daily-report narrators.

Primary backend shells out to the local ``codex`` CLI running GPT-5.5 with
high reasoning effort. When codex fails (quota exhausted, timeout, empty
output), ``call_llm`` automatically falls back to the DeepSeek HTTP API so
unattended cron runs still deliver a daily report (policy change 2026-06-10;
the previous fail-closed/no-fallback contract is preserved via
``QUANT_NARRATOR_FALLBACK=none``).

codex specifics (mirrors the proven invocation in
``quant-research-v1/scripts/run_agents.sh``):
    codex exec -m $CODEX_MODEL -c model_reasoning_effort="$EFFORT" \
      --sandbox $CODEX_NARRATOR_SANDBOX --color never --skip-git-repo-check --ephemeral \
      -C <root> -o <outfile> -   < prompt(on stdin)
The final agent message is read back from the ``-o`` file (clean text, no
event chatter).

DeepSeek specifics: OpenAI-compatible chat completions endpoint; the API key
comes from ``DEEPSEEK_API_KEY`` or ``quant-research-cn/config.yaml``
(``api.deepseek_key``, same source as the CN enrichment layer).

env knobs:
  QUANT_NARRATOR_BACKEND   primary: codex|deepseek     (default "codex")
  QUANT_NARRATOR_FALLBACK  fallback: deepseek|none     (default "deepseek")
  CODEX_BIN                codex binary                (default "codex")
  CODEX_MODEL              codex model id              (default "gpt-5.5")
  CODEX_REASONING_EFFORT   minimal|low|medium|high|xhigh (default "high")
  CODEX_NARRATOR_SANDBOX   codex exec sandbox          (default "workspace-write")
  NARRATOR_CONCURRENCY     max parallel LLM calls      (default 3)
  DEEPSEEK_MODEL           deepseek model id           (default "deepseek-v4-pro")
  DEEPSEEK_URL             chat completions endpoint   (default api.deepseek.com)
  DEEPSEEK_API_KEY         overrides config.yaml key
  DEEPSEEK_MAX_TOKENS      floor for max_tokens        (default 8192; codex
                           ignores caller max_tokens, so honoring small caps
                           verbatim would truncate narrator reports)
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

_VALID_BACKENDS = {"codex", "deepseek"}

# Successful calls per backend in this process — lets narrators record which
# backend actually produced the report instead of assuming codex.
_BACKEND_CALLS: dict[str, int] = {}


def backend() -> str:
    selected = (os.environ.get("QUANT_NARRATOR_BACKEND") or "codex").strip().lower()
    if selected not in _VALID_BACKENDS:
        raise RuntimeError(
            "Daily report narrators support codex|deepseek; "
            f"QUANT_NARRATOR_BACKEND={selected!r} is not allowed."
        )
    return selected


def fallback_backend() -> str | None:
    raw = os.environ.get("QUANT_NARRATOR_FALLBACK")
    selected = ("deepseek" if raw is None else raw).strip().lower()
    if selected in {"", "none", "off", "disabled"}:
        return None
    if selected not in _VALID_BACKENDS:
        raise RuntimeError(
            "Narrator fallback supports deepseek|none; "
            f"QUANT_NARRATOR_FALLBACK={selected!r} is not allowed."
        )
    return selected


def deepseek_model() -> str:
    return os.environ.get("DEEPSEEK_MODEL", "deepseek-v4-pro").strip()


def runtime_backend_summary() -> str:
    """Which backend(s) actually produced output in this process."""
    if not _BACKEND_CALLS:
        return backend()
    if len(_BACKEND_CALLS) == 1:
        return next(iter(_BACKEND_CALLS))
    return "+".join(f"{name}({count})" for name, count in sorted(_BACKEND_CALLS.items()))


def runtime_model_summary() -> str:
    used = set(_BACKEND_CALLS) or {backend()}
    models = []
    if "codex" in used:
        models.append(os.environ.get("CODEX_MODEL", "gpt-5.5"))
    if "deepseek" in used:
        models.append(deepseek_model())
    return "+".join(models)


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
    sandbox = os.environ.get("CODEX_NARRATOR_SANDBOX", "workspace-write")
    prompt = f"{_CODEX_GUARD}{system}\n\n{user}"

    out_fd, out_path = tempfile.mkstemp(prefix="narrator_codex_", suffix=".md")
    os.close(out_fd)
    try:
        cmd = [
            codex_bin, "exec",
            "-m", model,
            "-c", f'model_reasoning_effort="{effort}"',
            "--sandbox", sandbox,
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


def _deepseek_api_key() -> str | None:
    key = (os.environ.get("DEEPSEEK_API_KEY") or "").strip()
    if key:
        return key
    cfg_path = ROOT / "quant-research-cn" / "config.yaml"
    try:
        import yaml

        cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        key = ((cfg.get("api") or {}).get("deepseek_key") or "").strip()
        return key or None
    except Exception as exc:  # noqa: BLE001 - missing key is a soft failure.
        print(f"  [warn] deepseek key load failed: {exc}", file=sys.stderr)
        return None


def call_deepseek(
    system: str,
    user: str,
    *,
    label: str = "deepseek",
    temperature: float = 0.3,
    max_tokens: int = 1500,
    timeout: int = 900,
    model: str | None = None,
) -> str | None:
    """One DeepSeek chat-completions call. Returns text, or None on failure."""
    import requests

    api_key = _deepseek_api_key()
    if not api_key:
        print(f"  [warn] deepseek call '{label}' skipped: no API key", file=sys.stderr)
        return None
    url = os.environ.get("DEEPSEEK_URL", "https://api.deepseek.com/v1/chat/completions")
    try:
        max_tokens_floor = int(os.environ.get("DEEPSEEK_MAX_TOKENS", "8192"))
    except ValueError:
        max_tokens_floor = 8192
    payload = {
        "model": model or deepseek_model(),
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": temperature,
        # codex ignores caller max_tokens, so narrator call sites tuned for
        # codex pass caps that would truncate a full report — apply a floor.
        "max_tokens": max(max_tokens, max_tokens_floor),
        "stream": False,
    }
    try:
        resp = requests.post(
            url,
            headers={"Authorization": f"Bearer {api_key}"},
            json=payload,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        print(f"  [warn] deepseek call '{label}' failed: {exc}", file=sys.stderr)
        return None
    if resp.status_code != 200:
        body = (resp.text or "")[:300].replace("\n", " ")
        print(
            f"  [warn] deepseek call '{label}' failed: http={resp.status_code} body={body}",
            file=sys.stderr,
        )
        return None
    try:
        content = (resp.json()["choices"][0]["message"]["content"] or "").strip()
    except (ValueError, KeyError, IndexError, TypeError) as exc:
        print(f"  [warn] deepseek call '{label}' bad response shape: {exc}", file=sys.stderr)
        return None
    return content or None


def call_llm(
    system: str,
    user: str,
    *,
    label: str = "llm",
    temperature: float = 0.3,
    max_tokens: int = 1500,
    timeout: int = 900,
) -> str | None:
    """LLM call with backend chain: primary, then fallback (default deepseek).

    `temperature` and `max_tokens` are ignored by the codex backend (kept in
    the signature so call sites stay uniform) and honored by deepseek.
    Returns None only when every backend in the chain fails.
    """
    chain = [backend()]
    fb = fallback_backend()
    if fb and fb not in chain:
        chain.append(fb)
    for idx, name in enumerate(chain):
        if name == "codex":
            text = call_codex(system, user, label=label, timeout=timeout)
        else:
            text = call_deepseek(
                system,
                user,
                label=label,
                temperature=temperature,
                max_tokens=max_tokens,
                timeout=timeout,
            )
        if text:
            _BACKEND_CALLS[name] = _BACKEND_CALLS.get(name, 0) + 1
            return text
        if idx + 1 < len(chain):
            print(
                f"  [warn] {label}: {name} failed/empty; falling back to {chain[idx + 1]}",
                file=sys.stderr,
            )
    return None


if __name__ == "__main__":  # smoke test
    txt = call_codex(
        "你是金融写手。只输出一句中文市场点评。",
        "主题:科技股情绪偏热。",
        label="smoke",
        timeout=180,
    )
    print("RESULT:", txt)
    sys.exit(0 if txt else 1)
