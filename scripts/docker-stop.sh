#!/usr/bin/env bash
# Остановка кластера Docker Compose

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "🛑 Остановка кластера..."

docker-compose down

echo ""
echo "✅ Кластер остановлен!"
echo ""
echo "🗑️  Для удаления данных также выполните:"
echo "   docker-compose down -v"
