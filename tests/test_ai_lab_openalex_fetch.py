from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "fetch_ai_lab_publications_openalex.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("fetch_ai_lab_publications_openalex", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fetcher = _load_module()


class AiLabOpenAlexFetchTests(unittest.TestCase):
    def test_source_matching_handles_top_conference_names(self) -> None:
        self.assertTrue(fetcher.source_matches("ICLR", "International Conference on Learning Representations"))
        self.assertTrue(fetcher.source_matches("ICML", "Proceedings of the International Conference on Machine Learning"))
        self.assertTrue(fetcher.source_matches("CVPR", "IEEE/CVF Conference on Computer Vision and Pattern Recognition"))
        self.assertTrue(fetcher.source_matches("NeurIPS", "Advances in Neural Information Processing Systems"))

    def test_fetch_publications_writes_deduped_openalex_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            seed = root / "seed.yaml"
            output = root / "raw.jsonl"
            seed.write_text(
                """
version: 1
conference_scope: [ICLR]
companies:
  - symbol: GOOGL
    company: Alphabet
    labs: [Google DeepMind]
    stack_aliases: []
""",
                encoding="utf-8",
            )

            def fake_api_get_json(url: str, timeout: int = 30) -> dict[str, object]:
                parsed = urlparse(url)
                if parsed.path.endswith("/sources"):
                    return {
                        "results": [
                            {
                                "id": "https://openalex.org/S123",
                                "display_name": "International Conference on Learning Representations",
                            }
                        ]
                    }
                params = parse_qs(parsed.query)
                self.assertIn("raw_affiliation_strings.search", params.get("filter", [""])[0])
                return {
                    "meta": {"next_cursor": ""},
                    "results": [
                        {
                            "id": "https://openalex.org/W1",
                            "display_name": "Gemini representation learning",
                            "publication_year": 2025,
                            "primary_location": {
                                "source": {"display_name": "International Conference on Learning Representations"}
                            },
                            "authorships": [
                                {
                                    "author": {"display_name": "A Researcher"},
                                    "institutions": [{"display_name": "Google DeepMind"}],
                                }
                            ],
                        }
                    ],
                }

            original = fetcher.api_get_json
            fetcher.api_get_json = fake_api_get_json
            try:
                written = fetcher.fetch_publications(
                    seed=seed,
                    years=[2025],
                    output=output,
                    mailto="test@example.com",
                    symbols={"GOOGL"},
                    conferences_filter={"ICLR"},
                    max_pages=1,
                    sleep_seconds=0,
                )
            finally:
                fetcher.api_get_json = original

            self.assertEqual(written, 1)
            rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(rows[0]["id"], "https://openalex.org/W1")
            self.assertEqual(rows[0]["_ai_lab_fetch_context"]["conference"], "ICLR")
            self.assertEqual(rows[0]["_ai_lab_fetch_context"]["symbol_hint"], "GOOGL")


if __name__ == "__main__":
    unittest.main()
