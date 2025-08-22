# Makefile for data-pipeline-test

.PHONY: setup run db-start db-stop clean

setup:
	uv venv --clear
	. .venv/bin/activate && uv pip install pandas sqlalchemy psycopg2-binary python-dotenv

run:
	. .venv/bin/activate && python run_pipeline.py

db-start:
	docker-compose up -d

db-stop:
	docker-compose down

clean:
	rm -rf .venv
	rm -f requirements.txt
	find . -name "__pycache__" -type d -exec rm -r {} +
