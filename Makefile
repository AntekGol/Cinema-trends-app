.PHONY: help setup install lint test airflow-up airflow-down django-run django-migrate seed extract

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: install django-migrate ## Full project setup
	@echo "Done. Copy .env.example to .env and fill in your API keys."

install: ## Install dependencies
	pip install -r requirements.txt

lint: ## Run ruff linter
	ruff check etl/ dashboard/ airflow/ tests/

format: ## Auto-format code
	ruff format etl/ dashboard/ airflow/ tests/

test: ## Run tests
	pytest tests/ -v --cov=etl --cov-report=term-missing

test-django: ## Run Django tests
	cd dashboard && python manage.py test trends -v 2

extract: ## Manual TMDB extraction (test run)
	python -m etl.extractors.tmdb_extractor

airflow-up: ## Start Airflow via Docker
	docker-compose up -d

airflow-down: ## Stop Airflow
	docker-compose down

django-run: ## Start Django dev server
	cd dashboard && python manage.py runserver

django-migrate: ## Run migrations
	cd dashboard && python manage.py makemigrations && python manage.py migrate

seed: ## Seed DB with sample data
	cd dashboard && python manage.py seed_data

collectstatic: ## Collect static files (for deploy)
	cd dashboard && python manage.py collectstatic --noinput
