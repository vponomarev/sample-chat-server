#!/usr/bin/env bash
# Запуск кластера через Docker Compose

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "🚀 Запуск кластера через Docker Compose..."

# Сборка образов
docker-compose build

# Запуск сервисов
docker-compose up -d

echo ""
echo "✅ Кластер запущен!"
echo ""
echo "📊 Сервисы:"
echo "   - Chat Server 1 (master): http://localhost:8080"
echo "   - Chat Server 2 (slave):  http://localhost:8081"
echo "   - Chat Server 3 (slave):  http://localhost:8082"
echo "   - Prometheus:             http://localhost:9090"
echo "   - Grafana:                http://localhost:3000 (admin/admin)"
echo ""
echo "📋 Логи:"
echo "   docker-compose logs -f"
echo ""
echo "🛑 Остановка:"
echo "   ./scripts/docker-stop.sh"
