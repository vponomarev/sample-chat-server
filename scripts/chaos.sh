#!/usr/bin/env bash
# Хаос-тестирование кластера (issue B22, Этап 5.2).
#
# Поднимает кластер из 3 узлов и в цикле случайно «роняет» по одному узлу
# (SIGKILL — имитация краха), проверяя, что кластер сохраняет инвариант
# отказоустойчивости: среди живых узлов есть РОВНО ОДИН master и у него есть
# кворум. Затем узел поднимается обратно и должен вернуться в строй.
#
# Роняем не больше одного узла одновременно: большинство (2 из 3) остаётся живо,
# поэтому кластер обязан оставаться доступным. Если убить большинство — кворум
# пропадёт и master'а не будет by design (см. Этап 4.1), это не баг.
#
# Сетевые сбои (задержки/потери) включаются переменными окружения — они
# прокидываются в процессы узлов (см. server/cluster/faults.py):
#   CHAOS_LATENCY_MS=200 CHAOS_DROP_PCT=10 ./scripts/chaos.sh
#
# Число раундов задаётся первым аргументом (по умолчанию 5):
#   ./scripts/chaos.sh 10

set -u

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

ROUNDS="${1:-5}"

if [ -f venv/bin/activate ]; then
    source venv/bin/activate
else
    echo "❌ venv не найден. Создайте: python3 -m venv venv && pip install -r requirements.txt"
    exit 1
fi

IDS=(server1 server2 server3)
PORTS=(8081 8082 8083)
PEERS=(
    "server2@localhost:8082,server3@localhost:8083"
    "server1@localhost:8081,server3@localhost:8083"
    "server1@localhost:8081,server2@localhost:8082"
)
PIDS=(0 0 0)

log() { echo "[$(date +%H:%M:%S)] $*"; }

start_node() {
    local i=$1
    SERVER_ID="${IDS[$i]}" CHAT_PORT="${PORTS[$i]}" \
        CHAT_DB_PATH="data/chaos-${IDS[$i]}.db" \
        PEERS="${PEERS[$i]}" CLUSTER_ENABLED=true \
        LOG_LEVEL="${LOG_LEVEL:-WARNING}" \
        python server/main.py > "/tmp/chaos-${IDS[$i]}.log" 2>&1 &
    PIDS[$i]=$!
    log "▶ поднят ${IDS[$i]} (port ${PORTS[$i]}, pid ${PIDS[$i]})"
}

kill_node() {
    local i=$1
    if node_alive "$i"; then
        kill -9 "${PIDS[$i]}" 2>/dev/null || true
        wait "${PIDS[$i]}" 2>/dev/null || true
        log "✖ убит ${IDS[$i]} (pid ${PIDS[$i]})"
        PIDS[$i]=0
    fi
}

node_alive() {
    local i=$1
    [ "${PIDS[$i]}" -ne 0 ] && kill -0 "${PIDS[$i]}" 2>/dev/null
}

cleanup() {
    log "🧹 остановка кластера..."
    for i in 0 1 2; do kill_node "$i"; done
    pkill -f "python.*main.py" 2>/dev/null || true
}
trap cleanup EXIT

# Возвращает "role has_quorum" узла по его /cluster/state или "DOWN".
node_status() {
    local port=$1
    curl -s --max-time 2 "http://localhost:${port}/cluster/state" 2>/dev/null | python -c '
import sys, json
try:
    d = json.load(sys.stdin)
    role = d.get("election", {}).get("role", "?")
    q = d.get("quorum", {}).get("has_quorum")
    print(f"{role} {q}")
except Exception:
    print("DOWN")
' 2>/dev/null || echo "DOWN"
}

# Ждёт, пока перезапущенный узел снова начнёт отвечать (вернулся в строй).
wait_for_node_up() {
    local i=$1 timeout="${2:-30}"
    local deadline=$(( $(date +%s) + timeout ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        [ "$(node_status "${PORTS[$i]}")" != "DOWN" ] && {
            log "  ✔ ${IDS[$i]} вернулся в строй"; return 0; }
        sleep 1
    done
    log "  ✗ ${IDS[$i]} не вернулся за ${timeout}s"
    return 1
}

# Ждёт инвариант: ровно один master среди живых узлов и у него есть кворум.
# Возвращает 0 при успехе, 1 по таймауту.
wait_for_invariant() {
    local timeout="${1:-25}"
    local deadline=$(( $(date +%s) + timeout ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        local masters=0 quorum_ok=0 detail=""
        for i in 0 1 2; do
            node_alive "$i" || continue
            local st; st=$(node_status "${PORTS[$i]}")
            detail="$detail ${IDS[$i]}=[$st]"
            local role="${st%% *}" hq="${st##* }"
            if [ "$role" = "master" ]; then
                masters=$((masters + 1))
                [ "$hq" = "True" ] && quorum_ok=1
            fi
        done
        if [ "$masters" -eq 1 ] && [ "$quorum_ok" -eq 1 ]; then
            log "  ✔ инвариант ОК:$detail"
            return 0
        fi
        sleep 1
    done
    log "  ✗ инвариант НЕ достигнут за ${timeout}s:$detail"
    return 1
}

# --- Прогон ---
rm -f data/chaos-*.db* 2>/dev/null || true
pkill -f "python.*main.py" 2>/dev/null || true
sleep 1

log "🚀 старт кластера (3 узла)"
[ -n "${CHAOS_LATENCY_MS:-}" ] && log "  сетевая задержка: ${CHAOS_LATENCY_MS}ms"
[ -n "${CHAOS_DROP_PCT:-}" ] && log "  потеря пакетов: ${CHAOS_DROP_PCT}%"
for i in 0 1 2; do start_node "$i"; sleep 1; done

log "⏳ ждём формирования кластера..."
if ! wait_for_invariant 45; then
    log "❌ кластер не сформировался — прерываю"
    exit 1
fi

FAILURES=0
for round in $(seq 1 "$ROUNDS"); do
    log "═══ раунд $round/$ROUNDS ═══"
    # Выбираем случайный ЖИВОЙ узел.
    alive_idx=()
    for i in 0 1 2; do node_alive "$i" && alive_idx+=("$i"); done
    victim="${alive_idx[$((RANDOM % ${#alive_idx[@]}))]}"

    kill_node "$victim"
    if ! wait_for_invariant 25; then
        log "  ❌ после падения ${IDS[$victim]} инвариант нарушен"
        FAILURES=$((FAILURES + 1))
    fi

    start_node "$victim"
    wait_for_node_up "$victim" 30 || FAILURES=$((FAILURES + 1))
    if ! wait_for_invariant 25; then
        log "  ❌ после возврата ${IDS[$victim]} инвариант нарушен"
        FAILURES=$((FAILURES + 1))
    fi
done

echo ""
if [ "$FAILURES" -eq 0 ]; then
    log "✅ ХАОС ПРОЙДЕН: кластер пережил все $ROUNDS раундов"
    exit 0
else
    log "❌ ХАОС ВЫЯВИЛ ПРОБЛЕМЫ: нарушений инварианта — $FAILURES"
    exit 1
fi
