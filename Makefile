.PHONY: help install lint format test run precommit etl

help:
	@echo "make install|lint|format|test|etl|precommit"

install:
	poetry install
	poetry run pre-commit install

lint:
	poetry run black --check .
	poetry run isort --check-only .
	poetry run flake8 .
	poetry run mypy src

format:
	poetry run isort .
	poetry run black .

test:
	poetry run pytest -q

etl:
	poetry run python -c "from spectorr_pipeline import etl; import sys; df = etl.extract(sys.argv[1:]); etl.load(etl.transform(df))" s3://your-bucket/raw/

precommit:
	poetry run pre-commit run --all-files
