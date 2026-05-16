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

    def test_no_image_report_top_level_is_multipart_alternative(self) -> None:
        # A multipart/related wrapper with zero related parts is rendered as
        # raw base64 (乱码) by QQ Mail / 163 / Foxmail. Daily reports carry no
        # inline charts → the top level must be multipart/alternative.
        original = "A股量化研究盘前日报 — 2026-05-15"
        with tempfile.TemporaryDirectory() as tmpdir:
            report = Path(tmpdir) / "report.md"
            report.write_text("# 测试报告\n\n今天没有可交易标的。\n", encoding="utf-8")
            msg, _ = self.gmail.prepare_email(report, [], "me@example.com", original)
        self.assertEqual(msg.get_content_type(), "multipart/alternative")
        subtypes = [p.get_content_type() for p in msg.walk()]
        self.assertEqual(
            subtypes,
            ["multipart/alternative", "text/plain", "text/html"],
        )

    def test_report_with_charts_uses_multipart_related(self) -> None:
        # When inline images exist, the related wrapper is correct and needed.
        import base64 as _b64
        png = _b64.b64decode(
            b"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            report = Path(tmpdir) / "report.md"
            report.write_text("# 测试\n\n## 板块\n\n内容。\n", encoding="utf-8")
            chart = Path(tmpdir) / "chart.png"
            chart.write_bytes(png)
            msg, _ = self.gmail.prepare_email(
                report, [chart], "me@example.com", "Daily Report"
            )
        self.assertEqual(msg.get_content_type(), "multipart/related")
        self.assertIn("image/png", [p.get_content_type() for p in msg.walk()])

    def test_format_sender_rfc2047_encodes_chinese_name(self) -> None:
        # User report (2026-05-15): sender display name 黄振宇 arrived as
        # é»„æŒ¯å®‡ — UTF-8 bytes read as Latin-1. The From display name must
        # be RFC 2047 encoded.
        result = self.gmail._format_sender("黄振宇", "13502448752hzy@gmail.com")
        self.assertTrue(result.startswith("=?utf-8?"), msg=result)
        self.assertIn("<13502448752hzy@gmail.com>", result)
        # round-trips back to the original name
        import email.utils
        from email.header import decode_header, make_header
        name, addr = email.utils.parseaddr(result)
        self.assertEqual(str(make_header(decode_header(name))), "黄振宇")
        self.assertEqual(addr, "13502448752hzy@gmail.com")

    def test_format_sender_ascii_name_passes_through(self) -> None:
        result = self.gmail._format_sender("Quant Bot", "bot@example.com")
        self.assertEqual(result, "Quant Bot <bot@example.com>")

    def test_format_sender_no_name_returns_bare_address(self) -> None:
        self.assertEqual(
            self.gmail._format_sender(None, "bot@example.com"), "bot@example.com"
        )

    def test_prepared_email_from_header_is_ascii_safe(self) -> None:
        # The raw From header line must be pure ASCII (encoded-word), never
        # raw UTF-8 bytes.
        with tempfile.TemporaryDirectory() as tmpdir:
            report = Path(tmpdir) / "report.md"
            report.write_text("# 测试\n", encoding="utf-8")
            sender = self.gmail._format_sender("黄振宇", "acct@gmail.com")
            msg, _ = self.gmail.prepare_email(
                report, [], "to@example.com", "主题", sender=sender
            )
            raw = base64.urlsafe_b64decode(self.gmail._encode_message(msg)["raw"])
        from_lines = [ln for ln in raw.splitlines() if ln.startswith(b"From:")]
        self.assertEqual(len(from_lines), 1)
        from_lines[0].decode("ascii")  # raises if non-ASCII bytes leaked
        self.assertIn(b"=?utf-8?", from_lines[0])

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
