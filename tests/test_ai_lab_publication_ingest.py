from __future__ import annotations

import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "build_ai_lab_publications.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("build_ai_lab_publications", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module from {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ingest = _load_module()


class AiLabPublicationIngestTests(unittest.TestCase):
    def test_aggregates_top_conference_publications_by_lab_affiliation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw = root / "raw.csv"
            out = root / "ai_lab_publications.csv"
            raw.write_text(
                "title,conference,year,status,presentation_type,affiliations\n"
                "Gemini systems,NeurIPS,2025,accepted,spotlight,Google DeepMind; University X\n"
                "Llama training,ICML,2025,accepted,poster,Meta AI; University Y\n"
                "Rejected paper,CVPR,2025,rejected,poster,NVIDIA Research\n"
                "Other venue,KDD,2025,accepted,oral,Google Research\n",
                encoding="utf-8",
            )

            rows = ingest.aggregate_publications(raw, REPO_ROOT / "data" / "ai_lab_quality_seed.yaml")
            ingest.write_publications(rows, out)

            with out.open("r", encoding="utf-8", newline="") as handle:
                by_key = {
                    (row["symbol"], row["conference"], row["year"]): row
                    for row in csv.DictReader(handle)
                }
            self.assertEqual(by_key[("GOOGL", "NeurIPS", "2025")]["accepted_count"], "1")
            self.assertEqual(by_key[("GOOGL", "NeurIPS", "2025")]["oral_spotlight_count"], "1")
            self.assertEqual(by_key[("META", "ICML", "2025")]["accepted_count"], "1")
            self.assertNotIn(("NVDA", "CVPR", "2025"), by_key)
            self.assertNotIn(("GOOGL", "KDD", "2025"), by_key)

    def test_aggregates_jsonl_exports_from_openalex_and_semantic_scholar_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw = root / "raw.jsonl"
            out = root / "ai_lab_publications.csv"
            records = [
                {
                    "title": "Gemini representation learning",
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
                },
                {
                    "title": "GPU vision systems",
                    "venue": "IEEE/CVF Conference on Computer Vision and Pattern Recognition",
                    "year": 2025,
                    "presentation_type": "oral",
                    "authors": [
                        {"name": "B Researcher", "affiliations": ["NVIDIA Research"]},
                    ],
                },
            ]
            raw.write_text("\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8")

            rows = ingest.aggregate_publications(raw, REPO_ROOT / "data" / "ai_lab_quality_seed.yaml")
            ingest.write_publications(rows, out)

            with out.open("r", encoding="utf-8", newline="") as handle:
                by_key = {
                    (row["symbol"], row["conference"], row["year"]): row
                    for row in csv.DictReader(handle)
                }
            self.assertEqual(by_key[("GOOGL", "ICLR", "2025")]["accepted_count"], "1")
            self.assertEqual(by_key[("NVDA", "CVPR", "2025")]["accepted_count"], "1")
            self.assertEqual(by_key[("NVDA", "CVPR", "2025")]["oral_spotlight_count"], "1")

    def test_aggregates_openreview_content_json_shape_when_affiliations_are_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw = root / "openreview.json"
            raw.write_text(
                json.dumps(
                    {
                        "notes": [
                            {
                                "content": {
                                    "title": {"value": "Llama training systems"},
                                    "venue": {"value": "ICML 2025 Poster"},
                                    "year": {"value": 2025},
                                    "authors": {"value": ["A Researcher"]},
                                    "affiliations": {"value": ["Meta AI"]},
                                }
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            rows = ingest.aggregate_publications(raw, REPO_ROOT / "data" / "ai_lab_quality_seed.yaml")

            self.assertEqual(rows[0]["symbol"], "META")
            self.assertEqual(rows[0]["conference"], "ICML")
            self.assertEqual(rows[0]["accepted_count"], 1)


if __name__ == "__main__":
    unittest.main()
