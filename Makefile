.PHONY: help install run-api run-bot run-worker run-all dev-up dev-down dev-logs docker-up docker-down docker-logs test lint format clean

help:
	@echo "Доступные команды:"
	@echo ""
	@echo "🚀 Разработка:"
	@echo "  make dev-up       - Запустить в фоне с hot reload (РЕКОМЕНДУЕТСЯ)"
	@echo "  make dev-down     - Остановить dev окружение"
	@echo "  make dev-logs     - Показать логи dev окружения"
	@echo ""
	@echo "📦 Локальный запуск:"
	@echo "  make install      - Установить зависимости"
	@echo "  make run-api      - Запустить API сервер"
	@echo "  make run-bot      - Запустить Telegram бота"
	@echo "  make run-worker   - Запустить background worker"
	@echo "  make run-all      - Запустить все сервисы локально"
	@echo ""
	@echo "🐳 Docker (dev):"
	@echo "  make docker-up    - Запустить все сервисы в Docker"
	@echo "  make docker-down  - Остановить Docker контейнеры"
	@echo "  make docker-logs  - Показать логи Docker"
	@echo ""
	@echo "🚀 Production:"
	@echo "  make prod-up      - Запустить production окружение"
	@echo "  make prod-down    - Остановить production"
	@echo "  make prod-logs    - Показать production логи"
	@echo "  make prod-restart - Перезапустить production"
	@echo "  make prod-build   - Пересобрать production образы"
	@echo ""
	@echo "🛠 Утилиты:"
	@echo "  make format       - Форматировать код"
	@echo "  make clean        - Очистить временные файлы"

install:
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

dev-up:
	@echo "🚀 Запускаю dev окружение с hot reload..."
	docker-compose -f docker-compose.dev.yml up -d --build
	@echo "✅ Сервисы запущены в фоне!"
	@echo "📝 Логи: make dev-logs"
	@echo "🛑 Остановить: make dev-down"

dev-down:
	docker-compose -f docker-compose.dev.yml down

dev-logs:
	docker-compose -f docker-compose.dev.yml logs -f

dev-restart:
	docker-compose -f docker-compose.dev.yml restart

run-api:
	python run_api.py

run-bot:
	python run_bot.py

run-worker:
	python run_worker.py

run-all:
	@echo "Запуск всех сервисов..."
	@(trap 'kill 0' SIGINT; \
		python run_api.py & \
		python run_bot.py & \
		python run_worker.py & \
		wait)

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

docker-rebuild:
	docker-compose up -d --build

format:
	black src/
	isort src/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.log" -delete

db-migrate:
	alembic revision --autogenerate -m "$(msg)"

db-upgrade:
	alembic upgrade head

db-downgrade:
	alembic downgrade -1

prod-up:
	@echo "🚀 Запускаю production окружение..."
	docker-compose -f docker-compose.prod.yml up -d --build
	@echo "✅ Production запущен!"
	@echo "📝 Логи: make prod-logs"
	@echo "🛑 Остановить: make prod-down"

prod-down:
	docker-compose -f docker-compose.prod.yml down

prod-logs:
	docker-compose -f docker-compose.prod.yml logs -f

prod-restart:
	docker-compose -f docker-compose.prod.yml restart

prod-build:
	docker-compose -f docker-compose.prod.yml build --no-cache

prod-status:
	docker-compose -f docker-compose.prod.yml ps

