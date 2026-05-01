.PHONY: install test lint docs clean

install:
	pip install -e ".[dev]"

test:
	pytest tests/ -v --cov=vortex

lint:
	ruff check vortex/ tests/

docs:
	mkdocs serve

build:
	python -m build

clean:
	rm -rf build/ dist/ *.egg-info .pytest_cache __pycache__/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

docker-build:
	docker compose build

docker-up:
	docker compose up -d

docker-down:
	docker compose down
