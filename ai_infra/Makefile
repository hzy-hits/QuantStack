.PHONY: rebuild verify reports bootstrap

bootstrap:
	python3 -m venv .venv
	. .venv/bin/activate && python -m pip install -U pip

rebuild:
	python3 scripts/build_universe_system.py
	python3 scripts/generate_source_verification_queue.py
	python3 scripts/scaffold_evidence_cards.py
	python3 scripts/generate_us_alpha_mining_queue.py

verify:
	python3 tests/test_no_private_data_leak.py

reports: rebuild
