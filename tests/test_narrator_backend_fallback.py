from __future__ import annotations

import importlib.util
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "scripts" / "agents" / "codex_backend.py"

ENV_KEYS = (
    "QUANT_NARRATOR_BACKEND",
    "QUANT_NARRATOR_FALLBACK",
    "DEEPSEEK_MODEL",
)


def _load_module():
    spec = importlib.util.spec_from_file_location("codex_backend_under_test", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


backend_mod = _load_module()


class NarratorBackendFallbackTest(unittest.TestCase):
    def setUp(self) -> None:
        self._env = mock.patch.dict(os.environ, {}, clear=False)
        self._env.start()
        for key in ENV_KEYS:
            os.environ.pop(key, None)
        backend_mod._BACKEND_CALLS.clear()

    def tearDown(self) -> None:
        self._env.stop()

    def test_codex_success_does_not_touch_deepseek(self) -> None:
        with mock.patch.object(backend_mod, "call_codex", return_value="codex text") as codex, \
                mock.patch.object(backend_mod, "call_deepseek", return_value="ds text") as deepseek:
            out = backend_mod.call_llm("sys", "user", label="t1")
        self.assertEqual(out, "codex text")
        self.assertEqual(codex.call_count, 1)
        deepseek.assert_not_called()
        self.assertEqual(backend_mod.runtime_backend_summary(), "codex")

    def test_codex_empty_falls_back_to_deepseek_by_default(self) -> None:
        with mock.patch.object(backend_mod, "call_codex", return_value=None) as codex, \
                mock.patch.object(backend_mod, "call_deepseek", return_value="ds text") as deepseek:
            out = backend_mod.call_llm(
                "sys", "user", label="t2", temperature=0.2, max_tokens=4500
            )
        self.assertEqual(out, "ds text")
        self.assertEqual(codex.call_count, 1)
        self.assertEqual(deepseek.call_count, 1)
        kwargs = deepseek.call_args.kwargs
        self.assertEqual(kwargs.get("temperature"), 0.2)
        self.assertEqual(kwargs.get("max_tokens"), 4500)
        self.assertIn("deepseek", backend_mod.runtime_backend_summary())

    def test_fallback_none_keeps_fail_closed(self) -> None:
        os.environ["QUANT_NARRATOR_FALLBACK"] = "none"
        with mock.patch.object(backend_mod, "call_codex", return_value=None), \
                mock.patch.object(backend_mod, "call_deepseek", return_value="ds text") as deepseek:
            out = backend_mod.call_llm("sys", "user", label="t3")
        self.assertIsNone(out)
        deepseek.assert_not_called()

    def test_primary_can_be_switched_to_deepseek(self) -> None:
        os.environ["QUANT_NARRATOR_BACKEND"] = "deepseek"
        with mock.patch.object(backend_mod, "call_codex", return_value="codex text") as codex, \
                mock.patch.object(backend_mod, "call_deepseek", return_value="ds text") as deepseek:
            out = backend_mod.call_llm("sys", "user", label="t4")
        self.assertEqual(out, "ds text")
        codex.assert_not_called()
        self.assertEqual(deepseek.call_count, 1)

    def test_invalid_backend_raises(self) -> None:
        os.environ["QUANT_NARRATOR_BACKEND"] = "claude"
        with self.assertRaises(RuntimeError):
            backend_mod.backend()

    def test_deepseek_model_default_and_override(self) -> None:
        self.assertEqual(backend_mod.deepseek_model(), "deepseek-v4-pro")
        os.environ["DEEPSEEK_MODEL"] = "deepseek-chat"
        self.assertEqual(backend_mod.deepseek_model(), "deepseek-chat")


if __name__ == "__main__":
    unittest.main()
