"""
Интеграционный тест отказоустойчивости кластера (Этап 5.1).

В отличие от остальных тестов кластера (юниты с моками), здесь поднимаются
три **настоящих** сервера в отдельных процессах, между ними образуется кластер,
затем мастер убивается (SIGKILL — имитация краха), и проверяется, что:

  1. кластер выбирает нового мастера среди выживших узлов;
  2. сообщение, записанное до сбоя, успело реплицироваться и переживает failover
     (данные не теряются).

Это дорогой тест (несколько секунд на выборы и heartbeat). Он помечен маркером
``integration`` — при желании его можно исключить из быстрых прогонов:

    pytest -m "not integration"

Тайминги подобраны с запасом относительно констант кластера:
heartbeat 2s × порог 3 ≈ 6s до признания мастера мёртвым, выборы ~3s.
"""

import asyncio
import os
import socket
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

import aiohttp
import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent
SERVER_MAIN = PROJECT_ROOT / "server" / "main.py"

# Запас по времени на образование кластера и на failover.
CLUSTER_FORM_TIMEOUT = 30.0
FAILOVER_TIMEOUT = 40.0
REPLICATION_TIMEOUT = 15.0
POLL_INTERVAL = 0.5


def _free_port() -> int:
    """Свободный TCP-порт на localhost (небольшая гонка допустима в тестах)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _Node:
    """Один серверный процесс кластера."""

    def __init__(self, server_id: str, port: int, db_path: Path, peers: str, log_path: Path):
        self.server_id = server_id
        self.port = port
        self.db_path = db_path
        self.peers = peers
        self.log_path = log_path
        self.proc = None

    def start(self):
        env = os.environ.copy()
        env.update({
            "SERVER_ID": self.server_id,
            "CHAT_HOST": "127.0.0.1",
            "CHAT_PORT": str(self.port),
            "CHAT_DB_PATH": str(self.db_path),
            "CLUSTER_ENABLED": "true",
            "PEERS": self.peers,
            "LOG_LEVEL": "WARNING",
            "LOG_FORMAT": "text",
        })
        # Запуск ровно как в scripts/start_cluster.sh: `python server/main.py`
        # из корня проекта (тогда server/ попадает в sys.path[0]).
        self._log = open(self.log_path, "w")
        self.proc = __import__("subprocess").Popen(
            [sys.executable, str(SERVER_MAIN)],
            cwd=str(PROJECT_ROOT),
            env=env,
            stdout=self._log,
            stderr=self._log,
        )

    @property
    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def kill(self):
        """Жёсткое убийство процесса (имитация краха мастера)."""
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except Exception:
                self.proc.kill()
        try:
            self._log.close()
        except Exception:
            pass

    def count_messages(self, client_msg_id: str) -> int:
        """Прямое чтение файла БД узла (read-only), в обход сервера."""
        uri = f"file:{self.db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5)
        try:
            cur = conn.execute(
                "SELECT COUNT(*) FROM messages WHERE client_msg_id = ?",
                (client_msg_id,),
            )
            return cur.fetchone()[0]
        finally:
            conn.close()


async def _get_health(session: aiohttp.ClientSession, port: int):
    """Возвращает dict из /cluster/health или None, если узел недоступен."""
    try:
        async with session.get(
            f"http://127.0.0.1:{port}/cluster/health",
            timeout=aiohttp.ClientTimeout(total=2),
        ) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None


async def _wait_for_stable_cluster(session, nodes, expected_master, timeout):
    """
    Ждёт полной стабилизации: все узлы живы, отвечают и согласны, что мастер —
    ``expected_master``. Только после этого имеет смысл писать сообщение: иначе
    реплика, ещё не завершившая старт, пропустит единственную WAL-запись и,
    без последующих записей, не догонит её (догон запускается разрывом seq).
    Возвращает id мастера. Бросает AssertionError по таймауту.
    """
    deadline = time.monotonic() + timeout
    last_seen = {}
    while time.monotonic() < deadline:
        healths = {n.server_id: await _get_health(session, n.port) for n in nodes}
        last_seen = {
            sid: (h.get("role"), h.get("master_id")) if h else None
            for sid, h in healths.items()
        }
        if all(healths.values()):
            masters = [sid for sid, h in healths.items() if h.get("role") == "master"]
            agree = all(h.get("master_id") == expected_master for h in healths.values())
            if masters == [expected_master] and agree:
                return expected_master
        await asyncio.sleep(POLL_INTERVAL)
    raise AssertionError(
        f"Кластер не стабилизировался на мастере {expected_master} за {timeout}s. "
        f"Последнее состояние (role, master_id): {last_seen}"
    )


async def _wait_for_master(session, nodes, timeout, exclude=None):
    """
    Ждёт, пока ровно один живой узел (не из exclude) объявит себя мастером.
    Возвращает server_id мастера. Бросает AssertionError по таймауту.
    """
    exclude = exclude or set()
    deadline = time.monotonic() + timeout
    last_seen = {}
    while time.monotonic() < deadline:
        masters = []
        for node in nodes:
            if node.server_id in exclude or not node.alive:
                continue
            health = await _get_health(session, node.port)
            if health:
                last_seen[node.server_id] = health.get("role")
                if health.get("role") == "master":
                    masters.append(node.server_id)
        if len(masters) == 1:
            return masters[0]
        await asyncio.sleep(POLL_INTERVAL)
    raise AssertionError(
        f"За {timeout}s не образовался ровно один мастер. "
        f"Последние роли: {last_seen}"
    )


async def _send_message(port: int, nick: str, text: str, client_msg_id: str) -> str:
    """
    Подключается по WS к узлу, регистрируется и шлёт сообщение.
    Возвращает msg_id из ACK. Бросает, если не дождались OK/ACK.
    """
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(f"http://127.0.0.1:{port}/ws") as ws:
            await ws.send_json({"cmd": "REGISTER", "nick": nick, "password": "pw"})
            await _expect_event(ws, "OK", "REGISTER")

            await ws.send_json({
                "cmd": "MSG",
                "room": "#general",
                "text": text,
                "client_msg_id": client_msg_id,
            })
            ack = await _expect_event(ws, "ACK", "MSG")
            return ack["msg_id"]


async def _expect_event(ws, event: str, ctx: str) -> dict:
    """Читает события WS, пока не встретит нужное (или таймаут/ERROR)."""
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        msg = await ws.receive(timeout=10)
        if msg.type != aiohttp.WSMsgType.TEXT:
            continue
        data = __import__("json").loads(msg.data)
        if data.get("event") == event:
            return data
        if data.get("event") == "ERROR":
            raise AssertionError(f"{ctx}: сервер вернул ERROR: {data.get('message')}")
    raise AssertionError(f"{ctx}: не дождались события {event}")


@pytest.fixture
def cluster():
    """Поднимает кластер из 3 узлов и гарантированно гасит его после теста."""
    tmpdir = tempfile.mkdtemp(prefix="chat-failover-")
    ports = {f"server{i}": _free_port() for i in (1, 2, 3)}

    def peers_for(sid):
        return ",".join(
            f"{other}@127.0.0.1:{ports[other]}"
            for other in ports if other != sid
        )

    nodes = [
        _Node(
            server_id=sid,
            port=ports[sid],
            db_path=Path(tmpdir) / f"{sid}.db",
            peers=peers_for(sid),
            log_path=Path(tmpdir) / f"{sid}.log",
        )
        for sid in ("server1", "server2", "server3")
    ]
    for node in nodes:
        node.start()

    yield nodes

    for node in nodes:
        node.stop()
    __import__("shutil").rmtree(tmpdir, ignore_errors=True)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_master_failover_preserves_data(cluster):
    """Убитый мастер → выбран новый + записанное до сбоя сообщение уцелело."""
    nodes = cluster
    by_id = {n.server_id: n for n in nodes}

    async with aiohttp.ClientSession() as session:
        # 1. Кластер полностью стабилизируется, все узлы согласны о мастере.
        #    По алгоритму Bully мастер — узел с наибольшим ID (server3).
        master_id = await _wait_for_stable_cluster(
            session, nodes, expected_master="server3", timeout=CLUSTER_FORM_TIMEOUT
        )
        master = by_id[master_id]

        # 2. Пишем сообщение на мастер.
        client_msg_id = "failover-msg-1"
        msg_id = await _send_message(
            master.port, nick="alice", text="before failover",
            client_msg_id=client_msg_id,
        )
        assert msg_id

        # 3. Сообщение должно реплицироваться на все реплики (любая из них
        #    может стать новым мастером после сбоя — см. _wait_for_replication).
        replicas = [n for n in nodes if n.server_id != master_id]
        replicated = await _wait_for_replication(replicas, client_msg_id)
        assert replicated, "Сообщение реплицировалось не на все реплики"

        # 4. Убиваем мастер (имитация краха).
        master.kill()
        assert not master.alive

        # 5. Среди выживших выбирается новый мастер (другой узел).
        new_master_id = await _wait_for_master(
            session, nodes, FAILOVER_TIMEOUT, exclude={master_id}
        )
        assert new_master_id != master_id
        assert by_id[new_master_id].alive

    # 6. Данные пережили failover: сообщение есть в БД нового мастера.
    count = await _wait_for_count(by_id[new_master_id], client_msg_id, 1, timeout=5.0)
    assert count == 1, (
        f"Сообщение потеряно после failover (в БД нового мастера {new_master_id} "
        f"найдено {count}, ожидалось 1) — репликация/восстановление не сработали"
    )


async def _wait_for_replication(replicas, client_msg_id: str) -> bool:
    """
    Ждёт появления сообщения на **всех** репликах.

    Важно ждать именно все, а не одну: после убийства мастера новым мастером
    станет один из выживших, и мы проверяем данные именно на нём. Если ждать
    лишь одну реплику, возможна гонка — мастером окажется другая, ещё не
    догнавшая, и данные «пропадут» (мастеру не у кого догонять — старый мёртв).
    """
    deadline = time.monotonic() + REPLICATION_TIMEOUT
    while time.monotonic() < deadline:
        if all(_count_safe(node, client_msg_id) >= 1 for node in replicas):
            return True
        await asyncio.sleep(POLL_INTERVAL)
    return False


def _count_safe(node, client_msg_id: str) -> int:
    """count_messages, устойчивый к временной занятости файла БД."""
    try:
        return node.count_messages(client_msg_id)
    except sqlite3.Error:
        return 0


async def _wait_for_count(node, client_msg_id: str, expected: int, timeout: float) -> int:
    """Опрашивает БД узла, пока не увидит нужное число сообщений (или таймаут)."""
    deadline = time.monotonic() + timeout
    last = -1
    while time.monotonic() < deadline:
        last = _count_safe(node, client_msg_id)
        if last == expected:
            return last
        await asyncio.sleep(POLL_INTERVAL)
    return last


async def _get_state(session: aiohttp.ClientSession, port: int):
    """Возвращает dict из /cluster/state или None, если узел недоступен."""
    try:
        async with session.get(
            f"http://127.0.0.1:{port}/cluster/state",
            timeout=aiohttp.ClientTimeout(total=2),
        ) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None


@pytest.fixture
def lonely_node():
    """
    Поднимает ОДИН узел из кластера на 3 (пиры указывают на порты, которые
    никто не слушает). Узел — в меньшинстве и не должен становиться master.
    """
    tmpdir = tempfile.mkdtemp(prefix="chat-minority-")
    port = _free_port()
    # Два «мёртвых» пира: свободные порты, на которых никто не поднимается.
    dead_ports = [_free_port(), _free_port()]
    peers = (
        f"server2@127.0.0.1:{dead_ports[0]},"
        f"server3@127.0.0.1:{dead_ports[1]}"
    )
    node = _Node(
        server_id="server1",
        port=port,
        db_path=Path(tmpdir) / "server1.db",
        peers=peers,
        log_path=Path(tmpdir) / "server1.log",
    )
    node.start()

    yield node

    node.stop()
    __import__("shutil").rmtree(tmpdir, ignore_errors=True)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_minority_node_never_becomes_master(lonely_node):
    """
    Анти-split-brain (issue #11, Этап 4.1): узел в меньшинстве (видит только
    себя из кластера в 3 узла) НИКОГДА не становится master и не готов
    принимать трафик — иначе при сетевом разделении появился бы второй master.

    Проверяем устойчиво в течение окна времени (а не разово): узел не должен
    даже кратко «мелькнуть» мастером, пока не подтвердит большинство.
    """
    node = lonely_node

    async with aiohttp.ClientSession() as session:
        # Даём узлу время на старт, начальные выборы и несколько тиков
        # heartbeat/метрик — достаточно, чтобы «мелькнуть» мастером, если бы
        # кворум не защищал.
        observed = []
        deadline = time.monotonic() + 20.0
        # Сначала дождёмся, что узел вообще поднялся и отвечает.
        while time.monotonic() < deadline:
            state = await _get_state(session, node.port)
            if state:
                observed.append(state)
                break
            await asyncio.sleep(POLL_INTERVAL)
        assert observed, "Одиночный узел не поднялся / не отвечает на /cluster/state"

        # Наблюдаем ~12s: роль всё это время должна оставаться slave, кворума нет.
        watch_until = time.monotonic() + 12.0
        while time.monotonic() < watch_until:
            state = await _get_state(session, node.port)
            assert state is not None
            role = state["election"]["role"]
            quorum = state.get("quorum", {})
            assert role == "slave", (
                f"Узел в меньшинстве стал '{role}' — split-brain возможен! "
                f"quorum={quorum}"
            )
            assert quorum.get("has_quorum") is False, f"Неожиданный кворум: {quorum}"
            assert state["replication"]["is_master"] is False
            await asyncio.sleep(POLL_INTERVAL)

        # И не готов принимать трафик (readiness): мастера нет.
        async with session.get(
            f"http://127.0.0.1:{node.port}/health/ready"
        ) as resp:
            assert resp.status == 503
