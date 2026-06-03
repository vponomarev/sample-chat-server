#!/usr/bin/env bash
# Скрипт для остановки кластера

set -e

echo "🛑 Остановка кластера..."

# Чтение PID
if [ -f /tmp/server1.pid ]; then
    PID1=$(cat /tmp/server1.pid)
    echo "   Остановка сервера 1 (PID: $PID1)..."
    kill $PID1 2>/dev/null || true
    rm /tmp/server1.pid
fi

if [ -f /tmp/server2.pid ]; then
    PID2=$(cat /tmp/server2.pid)
    echo "   Остановка сервера 2 (PID: $PID2)..."
    kill $PID2 2>/dev/null || true
    rm /tmp/server2.pid
fi

if [ -f /tmp/server3.pid ]; then
    PID3=$(cat /tmp/server3.pid)
    echo "   Остановка сервера 3 (PID: $PID3)..."
    kill $PID3 2>/dev/null || true
    rm /tmp/server3.pid
fi

# Дополнительная очистка
pkill -f "python.*main.py" 2>/dev/null || true

sleep 1
echo "✅ Кластер остановлен"
