#!/usr/bin/env python3
"""
Нагрузочный тест чат-сервера (issue B23, Этап 5.3).

Цель — не «прогнать один раз», а **найти предел** и **увидеть деградацию**: с
ростом числа одновременных клиентов пропускная способность (сообщений/с) сначала
растёт, потом упирается в потолок, а латентность (особенно хвост — p95/p99)
начинает расти. Именно этот перегиб и есть «предел», который важно знать заранее,
а не на проде.

Модель нагрузки: N одновременных клиентов, каждый по WebSocket регистрируется,
входит в #general и шлёт M сообщений строго последовательно (послал → дождался
ACK → следующее). Латентность одного сообщения = время до его ACK. Пропускная
способность = всего сообщений / время стадии. Прогоняем несколько стадий с
растущим N и печатаем таблицу — по ней виден потолок и рост хвоста латентности.

Запуск (по умолчанию сам поднимает одиночный сервер на свободном порту):
    python scripts/loadtest.py
    python scripts/loadtest.py --stages 10,50,100,200 --messages 20
    python scripts/loadtest.py --url http://localhost:8080   # по внешнему серверу
"""

import argparse
import asyncio
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import aiohttp

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SERVER_MAIN = PROJECT_ROOT / "server" / "main.py"


# --- Статистика (чистые функции — покрыты юнит-тестами) ---

def percentile(samples_sorted, p):
    """
    p-й перцентиль (p в [0, 100]) отсортированного по возрастанию списка методом
    ближайшего ранга. Пустой список → 0.0.
    """
    if not samples_sorted:
        return 0.0
    if p <= 0:
        return samples_sorted[0]
    if p >= 100:
        return samples_sorted[-1]
    # Ближайший ранг: k = ceil(p/100 * n), индекс k-1.
    import math
    k = math.ceil(p / 100.0 * len(samples_sorted))
    return samples_sorted[max(0, k - 1)]


class Stats:
    """Накопитель латентностей (в мс) с расчётом сводки."""

    def __init__(self):
        self.samples = []
        self.errors = 0

    def add(self, latency_ms):
        self.samples.append(latency_ms)

    def add_error(self):
        self.errors += 1

    def summary(self):
        s = sorted(self.samples)
        n = len(s)
        return {
            "count": n,
            "errors": self.errors,
            "mean_ms": round(sum(s) / n, 1) if n else 0.0,
            "p50_ms": round(percentile(s, 50), 1),
            "p95_ms": round(percentile(s, 95), 1),
            "p99_ms": round(percentile(s, 99), 1),
            "max_ms": round(s[-1], 1) if n else 0.0,
        }


# --- Нагрузка ---

# Размер батча при установке соединений — чтобы фаза подключения не била по
# серверу «громовым стадом» (иначе искажает замер уже самой отправки сообщений).
CONNECT_BATCH = 20


class LoadClient:
    """Клиент нагрузки: держит WS-сессию, шлёт сообщения с замером ACK."""

    def __init__(self, url, nick):
        self.url = url
        self.nick = nick
        self._session = None
        self._ws = None

    async def connect(self):
        """Открывает соединение, регистрируется и входит в #general."""
        try:
            self._session = aiohttp.ClientSession()
            # Ограничиваем время установки соединения через wait_for (у ws_connect
            # float-timeout устарел и относится к ws-close, а не к подключению).
            self._ws = await asyncio.wait_for(
                self._session.ws_connect(f"{self.url}/ws"), timeout=10)
            await self._ws.send_json(
                {"cmd": "REGISTER", "nick": self.nick, "password": "pw"})
            if not await _wait_event(self._ws, "OK"):
                await self.close()
                return False
            await self._ws.send_json({"cmd": "JOIN", "room": "#general"})
            await _wait_event(self._ws, "OK")
            return True
        except Exception:
            await self.close()
            return False

    async def send_burst(self, num_messages, stats):
        """Шлёт M сообщений последовательно, копя латентность ACK в stats."""
        for i in range(num_messages):
            cmid = f"{self.nick}-{i}"
            t0 = time.perf_counter()
            try:
                await self._ws.send_json({
                    "cmd": "MSG", "room": "#general",
                    "text": f"msg {i}", "client_msg_id": cmid,
                })
                if await _wait_ack(self._ws, cmid):
                    stats.add((time.perf_counter() - t0) * 1000.0)
                else:
                    stats.add_error()
            except Exception:
                stats.add_error()

    async def close(self):
        try:
            if self._ws:
                await self._ws.close()
        except Exception:
            pass
        try:
            if self._session:
                await self._session.close()
        except Exception:
            pass


async def _wait_event(ws, event, timeout=10.0):
    """Ждёт событие с полем event==<event> (True/False по успеху/таймауту)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            msg = await ws.receive(timeout=timeout)
        except Exception:
            return False
        if msg.type != aiohttp.WSMsgType.TEXT:
            return False
        data = __import__("json").loads(msg.data)
        if data.get("event") == event:
            return True
        if data.get("event") == "ERROR":
            return False
    return False


async def _wait_ack(ws, client_msg_id, timeout=10.0):
    """Ждёт ACK именно для нашего client_msg_id (пропуская чужие MESSAGE-эхо)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            msg = await ws.receive(timeout=timeout)
        except Exception:
            return False
        if msg.type != aiohttp.WSMsgType.TEXT:
            return False
        data = __import__("json").loads(msg.data)
        if data.get("event") == "ACK" and data.get("client_msg_id") == client_msg_id:
            return True
        if data.get("event") == "ERROR":
            return False
    return False


async def run_stage(url, concurrency, messages_per_client):
    """
    Одна стадия. Фаза 1 — подключаем/регистрируем клиентов батчами (не мерим).
    Фаза 2 — все подключённые одновременно шлют сообщения, меряем throughput и
    латентность именно отправки. Возвращает сводку.
    """
    clients = [LoadClient(url, f"load-{concurrency}-{c}") for c in range(concurrency)]

    # Фаза 1: установка соединений батчами (изолирует «стадо» от замера).
    connected = []
    for i in range(0, len(clients), CONNECT_BATCH):
        batch = clients[i:i + CONNECT_BATCH]
        results = await asyncio.gather(*(c.connect() for c in batch))
        connected += [c for c, ok in zip(batch, results) if ok]
    connect_failed = concurrency - len(connected)

    # Фаза 2: замеряемая нагрузка.
    stats = Stats()
    t0 = time.perf_counter()
    await asyncio.gather(*(c.send_burst(messages_per_client, stats) for c in connected))
    elapsed = time.perf_counter() - t0

    await asyncio.gather(*(c.close() for c in connected))

    summary = stats.summary()
    summary["concurrency"] = concurrency
    summary["connected"] = len(connected)
    summary["connect_failed"] = connect_failed
    summary["elapsed_s"] = round(elapsed, 2)
    summary["throughput_msg_s"] = round(summary["count"] / elapsed, 0) if elapsed else 0.0
    return summary


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def _wait_server_ready(url, timeout=20.0):
    deadline = time.monotonic() + timeout
    async with aiohttp.ClientSession() as session:
        while time.monotonic() < deadline:
            try:
                async with session.get(f"{url}/health", timeout=2) as r:
                    if r.status == 200:
                        return True
            except Exception:
                pass
            await asyncio.sleep(0.3)
    return False


def _print_table(rows):
    header = ("clients", "conn✗", "msgs", "errs", "thr msg/s",
              "p50 ms", "p95 ms", "p99 ms", "max ms")
    print("\n{:>8} {:>6} {:>7} {:>6} {:>10} {:>8} {:>8} {:>8} {:>8}".format(*header))
    print("-" * 78)
    for r in rows:
        print("{:>8} {:>6} {:>7} {:>6} {:>10} {:>8} {:>8} {:>8} {:>8}".format(
            r["concurrency"], r["connect_failed"], r["count"], r["errors"],
            int(r["throughput_msg_s"]),
            r["p50_ms"], r["p95_ms"], r["p99_ms"], r["max_ms"],
        ))
    print()


async def main():
    parser = argparse.ArgumentParser(description="Нагрузочный тест чат-сервера (Этап 5.3)")
    parser.add_argument("--url", help="URL внешнего сервера (иначе поднимаем свой)")
    parser.add_argument("--stages", default="10,25,50,100",
                        help="Список значений конкуренции через запятую")
    parser.add_argument("--messages", type=int, default=20,
                        help="Сообщений на клиента в стадии")
    args = parser.parse_args()

    stages = [int(x) for x in args.stages.split(",") if x.strip()]

    proc = None
    tmp_db = None
    url = args.url
    if not url:
        port = _free_port()
        tmp_db = PROJECT_ROOT / "data" / f"loadtest-{port}.db"
        env = os.environ.copy()
        env.update({
            "CHAT_HOST": "127.0.0.1", "CHAT_PORT": str(port),
            "CHAT_DB_PATH": str(tmp_db), "LOG_LEVEL": "WARNING",
            # Поднимаем anti-flood лимит: бенчмарк меряет потолок сервера, а не
            # срабатывание rate limit (иначе часть MSG отклонялась бы штатно).
            "RATE_LIMIT_MSGS": "1000000",
        })
        proc = subprocess.Popen(
            [sys.executable, str(SERVER_MAIN)], cwd=str(PROJECT_ROOT), env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        url = f"http://127.0.0.1:{port}"
        print(f"⏳ поднимаю сервер на {url} ...")
        if not await _wait_server_ready(url):
            print("❌ сервер не поднялся")
            proc.terminate()
            return 1

    try:
        print(f"🔺 нагрузка: стадии={stages}, сообщений/клиент={args.messages}")
        rows = []
        for c in stages:
            row = await run_stage(url, c, args.messages)
            rows.append(row)
            print(f"  стадия {c:>4} клиентов: "
                  f"{int(row['throughput_msg_s']):>6} msg/s, "
                  f"p95={row['p95_ms']}ms, errs={row['errors']}, "
                  f"conn✗={row['connect_failed']}")
        _print_table(rows)

        # Подсказка про перегиб: где p95 заметно вырос относительно первой стадии.
        base = rows[0]["p95_ms"] or 1.0
        knee = next((r["concurrency"] for r in rows if r["p95_ms"] > base * 3), None)
        if knee:
            print(f"📉 деградация хвоста латентности заметна около {knee} клиентов "
                  f"(p95 вырос более чем в 3× относительно старта).")
        else:
            print("✅ в пределах заданных стадий выраженной деградации не видно.")
    finally:
        if proc:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except Exception:
                proc.kill()
        if tmp_db:
            for suffix in ("", "-wal", "-shm", "-journal"):
                p = Path(str(tmp_db) + suffix)
                if p.exists():
                    p.unlink()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
