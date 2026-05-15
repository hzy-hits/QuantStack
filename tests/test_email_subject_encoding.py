"""Verify email Subject headers are RFC 2047 encoded for non-ASCII strings.

User report (2026-05-14): US / CN report email titles arrived garbled
(乱码). Root cause was `msg["Subject"] = "美股量化研究日报 — ..."` being
passed through as raw UTF-8 without `Header(...).encode()`, so mail
clients defaulted to ASCII / latin-1 and rendered mojibake.

These tests assert the encoder + that the encoded form starts with
`=?utf-8?` (RFC 2047 prefix). ASCII subjects must pass through
unchanged to keep English headers clean.
"""
from __future__ import annotations

import base64
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

STACK_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(STACK_ROOT / "quant-research-v1" / "src"))


def _subject_header_lines(raw: bytes) -> list[bytes]:
    return [
        line
        for line in raw.splitlines()
        if line.startswith(b"Subject:") or line.startswith(b" =?utf-8?")
    ]


class EmailSubjectEncodingTests(unittest.TestCase):
    def setUp(self) -> None:
        from quant_bot.delivery import gmail
        self.gmail = gmail

    def test_ascii_subject_passes_through(self) -> None:
        result = self.gmail._encode_subject("Daily US Report 2026-05-14")
        self.assertEqual(result, "Daily US Report 2026-05-14")

    def test_chinese_subject_is_rfc2047_encoded(self) -> None:
        result = self.gmail._encode_subject("美股量化研究日报 — 2026-05-14")
        self.assertTrue(result.startswith("=?utf-8?"), msg=f"got {result!r}")
        self.assertIn("?b?", result.lower())  # base64 encoded body

    def test_cn_subject_is_rfc2047_encoded(self) -> None:
        result = self.gmail._encode_subject("A股量化研究日报 — 2026-05-14")
        self.assertTrue(result.startswith("=?utf-8?"))

    def test_em_dash_alone_triggers_encoding(self) -> None:
        # Em-dash (U+2014) is non-ASCII; the existing subjects include it.
        result = self.gmail._encode_subject("Daily Report — 2026-05-14")
        self.assertTrue(result.startswith("=?utf-8?"))

    def test_decoding_round_trip_preserves_text(self) -> None:
        from email.header import decode_header
        original = "美股量化研究日报 — 2026-05-14"
        encoded = self.gmail._encode_subject(original)
        decoded_parts = decode_header(encoded)
        rebuilt = "".join(
            part.decode(charset or "utf-8") if isinstance(part, bytes) else part
            for part, charset in decoded_parts
        )
        self.assertEqual(rebuilt, original)

    def test_gmail_raw_subject_is_single_encoded_header_line(self) -> None:
        original = "美股量化研究盘后日报 — 2026-05-14"
        with tempfile.TemporaryDirectory() as tmpdir:
            report = Path(tmpdir) / "report.md"
            report.write_text("# 测试\n", encoding="utf-8")
            msg, _ = self.gmail.prepare_email(report, [], "me@example.com", original)
            raw = base64.urlsafe_b64decode(self.gmail._encode_message(msg)["raw"])
        subject_lines = _subject_header_lines(raw)
        self.assertEqual(len(subject_lines), 1, msg=subject_lines)
        self.assertTrue(
            subject_lines[0].startswith(b"Subject: =?utf-8?"),
            msg=subject_lines[0],
        )

    def test_cn_legacy_sender_raw_subject_is_encoded(self) -> None:
        path = STACK_ROOT / "quant-research-cn" / "scripts" / "send_email.py"
        spec = importlib.util.spec_from_file_location("cn_send_email", path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        original = "A股量化研究盘前日报 — 2026-05-15"
        with tempfile.TemporaryDirectory() as tmpdir:
            report = Path(tmpdir) / "2026-05-15_report_zh.md"
            report.write_text("# 测试\n", encoding="utf-8")
            message = module.build_email(str(report), ["me@example.com"], subject=original)
            raw = base64.urlsafe_b64decode(message["raw"])
        subject_lines = _subject_header_lines(raw)
        self.assertEqual(len(subject_lines), 1, msg=subject_lines)
        self.assertTrue(
            subject_lines[0].startswith(b"Subject: =?utf-8?"),
            msg=subject_lines[0],
        )


if __name__ == "__main__":
    unittest.main()
