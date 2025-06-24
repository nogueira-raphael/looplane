.PHONY: install format lint typecheck check run-example clean

install:
	poetry install

format:
	poetry run black looplane

format-check:
	poetry run black --check looplane

lint:
	poetry run ruff check looplane

typecheck:
	poetry run mypy looplane

check: format-check lint typecheck

run-example:
	poetry run python examples/main.py

clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -exec rm -r {} +
