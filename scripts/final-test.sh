#!/usr/bin/env bash
# Финальное тестирование сценариев использования

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "🧪 Финальное тестирование сценариев"
echo "===================================="
echo ""

# Цвета
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Счётчики
PASSED=0
FAILED=0

# Функция для проверки
check() {
    local name="$1"
    local cmd="$2"
    
    echo -n "Проверка: $name ... "
    
    if eval "$cmd" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ PASSED${NC}"
        ((PASSED++))
    else
        echo -e "${RED}✗ FAILED${NC}"
        ((FAILED++))
    fi
}

# Функция для проверки HTTP
check_http() {
    local name="$1"
    local url="$2"
    local expected_status="$3"
    
    echo -n "Проверка: $name ... "
    
    status=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null || echo "000")
    
    if [ "$status" == "$expected_status" ]; then
        echo -e "${GREEN}✓ PASSED${NC} (status: $status)"
        ((PASSED++))
    else
        echo -e "${RED}✗ FAILED${NC} (expected: $expected_status, got: $status)"
        ((FAILED++))
    fi
}

echo "1. Проверка сервера (одиночный режим)"
echo "--------------------------------------"

# Запускаем сервер в фоне
echo "Запуск сервера..."
source venv/bin/activate
python server/main.py &
SERVER_PID=$!
sleep 3

# Проверки
check_http "Health check" "http://localhost:8080/health" "200"
check_http "Metrics endpoint" "http://localhost:8080/metrics" "200"
check_http "Static HTML" "http://localhost:8080/" "200"
check_http "Static CSS" "http://localhost:8080/static/css/style.css" "200"
check_http "Static JS" "http://localhost:8080/static/js/app.js" "200"
check_http "PWA Manifest" "http://localhost:8080/static/manifest.json" "200"
check_http "404 для несуществующего" "http://localhost:8080/nonexistent" "404"

# Остановка сервера
echo "Остановка сервера..."
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true
sleep 1

echo ""
echo "2. Проверка тестов"
echo "------------------"

check "Unit тесты" "PYTHONPATH=server pytest server/tests/ -q"

echo ""
echo "3. Проверка структуры проекта"
echo "------------------------------"

check "server/main.py существует" "test -f server/main.py"
check "server/cluster/heartbeat.py" "test -f server/cluster/heartbeat.py"
check "server/cluster/election.py" "test -f server/cluster/election.py"
check "server/cluster/replication.py" "test -f server/cluster/replication.py"
check "server/cluster/manager.py" "test -f server/cluster/manager.py"
check "server/observability/metrics.py" "test -f server/observability/metrics.py"
check "client/index.html" "test -f client/index.html"
check "client/sw.js" "test -f client/sw.js"
check "docker-compose.yml" "test -f docker-compose.yml"
check "Dockerfile" "test -f Dockerfile"
check "monitoring/prometheus.yml" "test -f monitoring/prometheus.yml"
check "monitoring/alerts.yml" "test -f monitoring/alerts.yml"
check "scripts/docker-start.sh" "test -f scripts/docker-start.sh"
check "scripts/docker-stop.sh" "test -f scripts/docker-stop.sh"

echo ""
echo "===================================="
echo "ИТОГИ:"
echo -e "  ${GREEN}PASSED: $PASSED${NC}"
echo -e "  ${RED}FAILED: $FAILED${NC}"
echo "===================================="

if [ $FAILED -gt 0 ]; then
    echo ""
    echo -e "${RED}⚠ Тестирование завершено с ошибками${NC}"
    exit 1
else
    echo ""
    echo -e "${GREEN}✅ Все тесты пройдены!${NC}"
    exit 0
fi
