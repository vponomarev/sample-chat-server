#!/usr/bin/env bash
# Скрипт для запуска кластера из 3 серверов для тестирования

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

# Активация venv
if [ -f venv/bin/activate ]; then
    source venv/bin/activate
else
    echo "❌ venv не найден. Создайте: python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# Остановка существующих серверов
echo "🛑 Остановка существующих серверов..."
pkill -f "python.*main.py" 2>/dev/null || true
sleep 1

# Очистка данных
echo "🧹 Очистка данных..."
rm -rf data/*.db data/*.db-journal data/*.db-wal 2>/dev/null || true

# Запуск серверов
echo "🚀 Запуск сервера 1 (port 8081)..."
SERVER_ID=server1 CHAT_PORT=8081 PEERS="localhost:8082,localhost:8083" CLUSTER_ENABLED=true \
    python server/main.py > /tmp/server1.log 2>&1 &
PID1=$!
echo "   Server 1 PID: $PID1"

sleep 1

echo "🚀 Запуск сервера 2 (port 8082)..."
SERVER_ID=server2 CHAT_PORT=8082 PEERS="localhost:8081,localhost:8083" CLUSTER_ENABLED=true \
    python server/main.py > /tmp/server2.log 2>&1 &
PID2=$!
echo "   Server 2 PID: $PID2"

sleep 1

echo "🚀 Запуск сервера 3 (port 8083)..."
SERVER_ID=server3 CHAT_PORT=8083 PEERS="localhost:8081,localhost:8082" CLUSTER_ENABLED=true \
    python server/main.py > /tmp/server3.log 2>&1 &
PID3=$!
echo "   Server 3 PID: $PID3"

sleep 3

echo ""
echo "✅ Кластер запущен!"
echo ""
echo "📊 Проверка состояния:"
echo "   Server 1: http://localhost:8081/health"
echo "   Server 2: http://localhost:8082/health"
echo "   Server 3: http://localhost:8083/health"
echo ""
echo "📋 Состояние кластера:"
echo "   curl http://localhost:8081/cluster/state"
echo ""
echo "🛑 Для остановки: ./scripts/stop_cluster.sh"
echo ""

# Сохранение PID
echo "$PID1" > /tmp/server1.pid
echo "$PID2" > /tmp/server2.pid
echo "$PID3" > /tmp/server3.pid
