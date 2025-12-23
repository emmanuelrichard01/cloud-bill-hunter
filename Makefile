# --- 🛠️ SETUP & DATA ---
setup:
	pip install -r requirements.txt

data:
	@echo "🎲 Generating synthetic AWS billing data..."
	python src/generate_data.py

# --- 🐳 DOCKER CONTROL CENTER ---
build:
	@echo "🏗️ Building Docker images..."
	docker-compose build

up:
	@echo "🚀 Launching Cloud Bill Hunter Platform..."
	docker-compose up

down:
	@echo "🛑 Stopping all services..."
	docker-compose down

clean:
	@echo "🧹 Cleaning up pycache and temp files..."
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf data/uploads/*

# --- 🧪 TESTING & QA ---
test:
	@echo "🧪 Running Unit Tests..."
	python -m pytest tests/

api-test:
	@echo "🔌 Pinging API Health Check..."
	curl http://localhost:8000/