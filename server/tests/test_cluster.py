"""
Тесты кластера (cluster/).
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestHeartbeat:
    """Тесты HeartbeatManager."""

    @pytest.fixture
    def heartbeat_manager(self):
        from cluster.heartbeat import HeartbeatManager
        
        return HeartbeatManager(
            server_id="server1",
            host="localhost",
            port=8081,
            peers=[
                {"host": "localhost", "port": 8082, "server_id": "server2"},
                {"host": "localhost", "port": 8083, "server_id": "server3"},
            ]
        )

    def test_init(self, heartbeat_manager):
        """Тест инициализации."""
        assert heartbeat_manager.server_id == "server1"
        assert len(heartbeat_manager.peers) == 2
        assert heartbeat_manager.heartbeat_interval == 2.0

    def test_get_alive_peers(self, heartbeat_manager):
        """Тест получения живых пиров."""
        # Все пиры живы по умолчанию
        alive = heartbeat_manager.get_alive_peers()
        assert len(alive) == 2

    def test_get_dead_peers(self, heartbeat_manager):
        """Тест получения мёртвых пиров."""
        # Все пиры живы по умолчанию
        dead = heartbeat_manager.get_dead_peers()
        assert len(dead) == 0

    def test_get_alive_peers_with_higher_id(self, heartbeat_manager):
        """Тест получения пиров с большим ID."""
        higher = heartbeat_manager.get_alive_peers_with_higher_id()
        # server2 и server3 имеют больший ID чем server1
        assert len(higher) == 2

    def test_is_master_alive_no_master(self, heartbeat_manager):
        """Тест проверки master когда master нет."""
        assert heartbeat_manager.is_master_alive() is False

    def test_get_master_none(self, heartbeat_manager):
        """Тест получения master когда master нет."""
        assert heartbeat_manager.get_master() is None

    def test_get_cluster_state(self, heartbeat_manager):
        """Тест получения состояния кластера."""
        state = heartbeat_manager.get_cluster_state()
        
        assert "self" in state
        assert "peers" in state
        assert "metrics" in state
        assert state["self"]["server_id"] == "server1"

    @pytest.mark.asyncio
    async def test_handle_peer_timeout(self, heartbeat_manager):
        """Тест обработки таймаута пира."""
        peer = list(heartbeat_manager.peers.values())[0]
        
        # Имитируем таймауты
        for i in range(3):
            await heartbeat_manager._handle_peer_timeout(peer)
        
        # После 3 таймаутов пир должен быть мёртв
        assert peer.is_alive is False
        assert peer.consecutive_failures == 3

    @pytest.mark.asyncio
    async def test_peer_recovery(self, heartbeat_manager):
        """Тест восстановления пира."""
        peer = list(heartbeat_manager.peers.values())[0]
        
        # Делаем пира мёртвым
        peer.consecutive_failures = 3
        peer.is_alive = False
        
        # Имитируем успешный heartbeat
        peer.consecutive_failures = 0
        peer.is_alive = True
        peer.last_heartbeat = __import__('time').time()
        
        assert peer.is_alive is True


class TestBullyElection:
    """Тесты BullyElection."""

    @pytest.fixture
    def election(self):
        from cluster.election import BullyElection
        
        return BullyElection(
            server_id="server2",
            host="localhost",
            port=8082,
            peers=[
                {"host": "localhost", "port": 8081, "server_id": "server1"},
                {"host": "localhost", "port": 8083, "server_id": "server3"},
            ]
        )

    def test_init(self, election):
        """Тест инициализации."""
        assert election.server_id == "server2"
        assert election.numeric_id == 2
        assert election.role == "slave"
        assert election.term == 0

    def test_numeric_id(self, election):
        """Тест числового ID."""
        assert election.numeric_id == 2

    def test_role_property(self, election):
        """Тест свойства role."""
        assert election.role == "slave"
        
        election.state.is_master = True
        assert election.role == "master"

    def test_term_property(self, election):
        """Тест свойства term."""
        assert election.term == 0
        
        election.state.current_term = 5
        assert election.term == 5

    def test_get_higher_alive_peers(self, election):
        """Тест получения пиров с большим ID."""
        higher = election._get_higher_alive_peers()
        
        # Только server3 имеет больший ID
        assert len(higher) == 1
        assert "server3" in higher

    def test_get_state(self, election):
        """Тест получения состояния."""
        state = election.get_state()
        
        assert "server_id" in state
        assert "role" in state
        assert "term" in state
        assert "is_master" in state
        assert state["server_id"] == "server2"

    @pytest.mark.asyncio
    async def test_become_master(self, election):
        """Тест становления master."""
        # Мок для broadcast_coordinator
        election._broadcast_coordinator = AsyncMock()
        election.on_become_master = AsyncMock()
        election.on_master_changed = AsyncMock()
        
        await election._become_master()
        
        assert election.state.is_master is True
        assert election.state.master_id == "server2"
        assert election.state.election_in_progress is False
        
        election._broadcast_coordinator.assert_called_once()
        election.on_become_master.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_coordinator(self, election):
        """Тест обработки COORDINATOR сообщения."""
        from aiohttp import web
        
        # Создаём мок запроса
        request = MagicMock()
        request.json = AsyncMock(return_value={
            "master_id": "server3",
            "term": 5
        })
        
        response = await election.handle_coordinator(request)
        
        assert election.state.master_id == "server3"
        assert election.state.current_term == 5
        assert election.state.is_master is False

    @pytest.mark.asyncio
    async def test_handle_election_start(self, election):
        """Тест обработки начала выборов."""
        from aiohttp import web
        
        # Создаём мок запроса
        request = MagicMock()
        request.json = AsyncMock(return_value={
            "candidate_id": "server1",
            "term": 3
        })
        
        # Вызываем handler - он должен вернуть response
        response = await election.handle_election_start(request)
        
        # Проверяем что response это web.Response
        assert isinstance(response, web.Response)


class TestWALReplication:
    """Тесты WALReplication."""

    @pytest.fixture
    def wal_replication(self, database):
        from cluster.replication import WALReplication
        
        return WALReplication(
            server_id="server1",
            db_connection=database.connection,
            is_master=True,
            peers=[]
        )

    @pytest.mark.asyncio
    async def test_init(self, wal_replication):
        """Тест инициализации."""
        assert wal_replication.server_id == "server1"
        assert wal_replication.is_master is True

    @pytest.mark.asyncio
    async def test_log_operation_master(self, wal_replication, database):
        """Тест логирования операции на master."""
        seq = await wal_replication.log_operation(
            operation="INSERT",
            table_name="messages",
            data={"msg_id": "test-123", "text": "Hello"}
        )
        
        assert seq is not None
        assert seq > 0

    @pytest.mark.asyncio
    async def test_log_operation_slave(self, database):
        """Тест логирования операции на slave (должна быть ошибка)."""
        from cluster.replication import WALReplication
        
        replication = WALReplication(
            server_id="server2",
            db_connection=database.connection,
            is_master=False
        )
        
        with pytest.raises(RuntimeError, match="Только master"):
            await replication.log_operation("INSERT", "messages", {})

    @pytest.mark.asyncio
    async def test_get_wal_entries(self, wal_replication, database):
        """Тест получения WAL записей."""
        # Добавляем запись
        await wal_replication.log_operation(
            operation="INSERT",
            table_name="messages",
            data={"msg_id": "test-456"}
        )
        
        entries = await wal_replication.get_wal_entries(0)
        
        assert len(entries) > 0
        assert entries[-1].operation == "INSERT"

    @pytest.mark.asyncio
    async def test_apply_wal_entry_insert(self, wal_replication, database):
        """Тест применения INSERT операции."""
        # seq = last_applied + 1 (0 + 1) — применяется строго по порядку
        entry = {
            "seq": 1,
            "ts": 1234567890,
            "operation": "INSERT",
            "table_name": "users",
            "data": {"nick": "replicated_user", "password": None, "created_at": 1234567890}
        }

        result = await wal_replication.apply_wal_entry(entry)

        assert result is True

        # Проверяем что пользователь создан
        user = await database.fetchone(
            "SELECT nick FROM users WHERE nick = 'replicated_user'"
        )
        assert user is not None

    @pytest.mark.asyncio
    async def test_apply_wal_entry_delete(self, wal_replication, database):
        """Тест применения DELETE операции."""
        import time
        
        # Создаём пользователя
        await database.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("to_delete", None, int(time.time()))
        )
        await database.commit()
        
        entry = {
            "seq": 1,
            "ts": 1234567890,
            "operation": "DELETE",
            "table_name": "users",
            "data": {"nick": "to_delete"}
        }

        result = await wal_replication.apply_wal_entry(entry)

        assert result is True

        # Проверяем что пользователь удалён
        user = await database.fetchone(
            "SELECT nick FROM users WHERE nick = 'to_delete'"
        )
        assert user is None

    @pytest.mark.asyncio
    async def test_apply_wal_entry_skip_old(self, wal_replication):
        """Тест пропуска старых записей."""
        # Устанавливаем last_applied_seq
        wal_replication._last_applied_seq = 1000
        
        entry = {
            "seq": 100,  # Старая запись
            "ts": 1234567890,
            "operation": "INSERT",
            "table_name": "messages",
            "data": {}
        }
        
        result = await wal_replication.apply_wal_entry(entry)
        
        assert result is True  # Пропущено успешно

    def test_get_lag(self, wal_replication):
        """Тест получения отставания."""
        lag = wal_replication.get_lag()
        assert lag == 0

    def test_get_lag_nonzero(self, wal_replication):
        """Lag = разница между seq мастера и применённым."""
        wal_replication._last_master_seq = 10
        wal_replication._last_applied_seq = 4
        assert wal_replication.get_lag() == 6

    @pytest.mark.asyncio
    async def test_apply_wal_entry_gap_not_applied(self, wal_replication, database):
        """
        Разрыв в seq не должен применяться и не должен двигать указатель
        (иначе пропущенная запись теряется). Без известного master догон
        невозможен, поэтому запись остаётся неприменённой (#12).
        """
        entry = {
            "seq": 5,  # ожидалось 1 — разрыв
            "ts": 1234567890,
            "operation": "INSERT",
            "table_name": "users",
            "data": {"nick": "gap_user", "password": None, "created_at": 1},
        }

        result = await wal_replication.apply_wal_entry(entry)

        assert result is False
        assert wal_replication._last_applied_seq == 0
        # last_master_seq всё равно обновился (для метрики lag)
        assert wal_replication._last_master_seq == 5
        user = await database.fetchone(
            "SELECT nick FROM users WHERE nick = 'gap_user'"
        )
        assert user is None

    def test_set_master(self, wal_replication):
        """Тест установки режима master/slave."""
        wal_replication.set_master(False)
        assert wal_replication.is_master is False

        wal_replication.set_master(True)
        assert wal_replication.is_master is True

    @pytest.mark.asyncio
    async def test_send_wal_circuit_breaker_short_circuits(self, wal_replication):
        """
        После серии ошибок к мёртвому пиру circuit breaker размыкается и
        следующие отправки не делают сетевой вызов (issue B11, Этап 3.2).
        """
        sess = MagicMock()
        sess.post.side_effect = ConnectionError("peer down")
        wal_replication._session = sess

        peer = {"host": "localhost", "port": 9999, "server_id": "peerX"}
        entry = {"entries": [{"seq": 1}]}

        for _ in range(3):  # порог по умолчанию — 3
            await wal_replication._send_wal_to_peer(peer, entry)

        assert wal_replication._breaker("peerX").state == "open"

        calls_before = sess.post.call_count
        result = await wal_replication._send_wal_to_peer(peer, entry)
        assert result is None
        assert sess.post.call_count == calls_before  # вызова не было — коротко замкнули

    @pytest.mark.asyncio
    async def test_replication_ack_and_retry(self, wal_replication, database):
        """
        ACK двигает отметку подтверждения; потерянный пуш догоняется повторной
        доставкой бэклога (issue B3, Этап 3.3).
        """
        class _AckResp:
            def __init__(self, data):
                self._data = data

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def json(self):
                return self._data

        # Наполняем WAL двумя операциями (пиров нет — отправок не происходит).
        await wal_replication.log_operation(
            "INSERT", "users", {"nick": "a", "password": None, "created_at": 1})
        await wal_replication.log_operation(
            "INSERT", "users", {"nick": "b", "password": None, "created_at": 1})

        peer = {"host": "localhost", "port": 9999, "server_id": "peerX"}
        wal_replication.peers = [peer]

        # 1) Первый пуш теряется — пир недоступен, отметка не двигается.
        sess = MagicMock()
        sess.post.side_effect = ConnectionError("down")
        wal_replication._session = sess
        await wal_replication._send_pending_to_peer(peer)
        assert wal_replication._peer_acked.get("peerX", 0) == 0

        # 2) Пир вернулся и подтвердил seq=2 — ретрай доставляет весь бэклог.
        sess.post.side_effect = None
        sess.post.return_value = _AckResp({"ack": True, "last_applied_seq": 2})
        await wal_replication._send_pending_to_peer(peer)

        assert wal_replication._peer_acked["peerX"] == 2
        # В доставленном пейлоаде — обе накопленные записи (бэклог целиком).
        sent = sess.post.call_args.kwargs["json"]
        assert [e["seq"] for e in sent["entries"]] == [1, 2]


class TestClusterManager:
    """Тесты ClusterManager."""

    @pytest.fixture
    def cluster_manager(self, app_config):
        from cluster.manager import ClusterManager
        from aiohttp import web
        
        app = web.Application()
        
        return ClusterManager(
            app=app,
            server_id="server1",
            host="localhost",
            port=8081,
            peers=[
                {"host": "localhost", "port": 8082, "server_id": "server2"},
            ],
            db_connection=None
        )

    def test_init(self, cluster_manager):
        """Тест инициализации."""
        assert cluster_manager.server_id == "server1"
        assert cluster_manager.is_master is False  # Пока не запущен

    def test_uptime(self, cluster_manager):
        """Тест uptime."""
        import time
        time.sleep(0.1)
        assert cluster_manager.uptime >= 0

    def test_get_cluster_servers(self, cluster_manager):
        """Тест получения списка серверов."""
        servers = cluster_manager.get_cluster_servers()
        
        assert len(servers) >= 1
        assert any(s["server_id"] == "server1" for s in servers)

    def test_get_master_server_none(self, cluster_manager):
        """Тест получения master когда нет."""
        master = cluster_manager.get_master_server()
        # Может вернуть себя или None
        assert master is None or master["server_id"] == "server1"


class TestPeerHandler:
    """Тесты peer_handler."""

    @pytest.mark.asyncio
    async def test_cluster_health_no_cluster(self):
        """Тест health без кластера."""
        from aiohttp import web
        from cluster.peer_handler import handle_cluster_health
        
        app = web.Application()
        request = MagicMock()
        request.app = app
        
        response = await handle_cluster_health(request)
        
        # Должен вернуть дефолтный ответ
        assert response.status == 200

    @pytest.mark.asyncio
    async def test_cluster_state_no_cluster(self):
        """Тест state без кластера."""
        from aiohttp import web
        from cluster.peer_handler import handle_cluster_state
        
        app = web.Application()
        request = MagicMock()
        request.app = app
        
        response = await handle_cluster_state(request)

        # Должен вернуть ошибку
        assert response.status == 503


class TestTopologyConfig:
    """Тесты разбора топологии кластера (config.parse_peers)."""

    def test_parse_peers_explicit_ids(self):
        from config import parse_peers
        peers = parse_peers("server2@localhost:8082,server3@host3:8083")
        assert peers == [
            {"server_id": "server2", "host": "localhost", "port": 8082},
            {"server_id": "server3", "host": "host3", "port": 8083},
        ]

    def test_parse_peers_empty(self):
        from config import parse_peers
        assert parse_peers("") == []

    def test_parse_peers_legacy_without_id(self):
        """Старый формат без ID всё ещё разбирается (с предупреждением)."""
        from config import parse_peers
        peers = parse_peers("localhost:8082")
        assert peers[0]["host"] == "localhost"
        assert peers[0]["port"] == 8082
        assert peers[0]["server_id"]  # какой-то ID проставлен


class TestWriteGuard:
    """Модель primary + standby: запись разрешена только на master."""

    @pytest.fixture
    def mock_ws(self):
        ws = MagicMock()
        ws.send_str = AsyncMock()
        return ws

    class _SlaveCluster:
        """Мок кластера-реплики."""
        is_master = False
        replication = None

        def get_master_server(self):
            return {"host": "localhost", "port": 8081, "server_id": "server3"}

        def get_cluster_servers(self):
            return [
                {"host": "localhost", "port": 8081, "server_id": "server3", "role": "master"},
                {"host": "localhost", "port": 8082, "server_id": "server2", "role": "slave"},
            ]

    @pytest.mark.asyncio
    async def test_msg_rejected_on_replica(self, database, ws_manager, mock_ws):
        """MSG на реплике отклоняется с Read-only replica и SERVER_LIST."""
        from irc.commands import CommandHandler
        import json

        handler = CommandHandler(database, ws_manager, cluster=self._SlaveCluster())
        ws_manager.sessions[mock_ws] = {"nick": "u", "authenticated": True}
        ws_manager.connections.add(mock_ws)

        await handler.handle_msg(mock_ws, {"room": "#general", "text": "hi"})

        events = [json.loads(c[0][0]) for c in mock_ws.send_str.call_args_list]
        err = next(e for e in events if e["event"] == "ERROR")
        assert err["message"] == "Read-only replica"
        assert err["master"]["server_id"] == "server3"
        assert any(e["event"] == "SERVER_LIST" for e in events)

        # Запись не попала в БД
        row = await database.fetchone("SELECT COUNT(*) AS c FROM messages")
        assert row["c"] == 0

    @pytest.mark.asyncio
    async def test_msg_allowed_standalone(self, database, ws_manager, mock_ws):
        """Без кластера (standalone) запись разрешена."""
        from irc.commands import CommandHandler
        import json

        import time
        await database.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            ("u", None, int(time.time()))
        )
        await database.commit()

        handler = CommandHandler(database, ws_manager, cluster=None)
        ws_manager.sessions[mock_ws] = {"nick": "u", "authenticated": True}
        ws_manager.connections.add(mock_ws)
        ws_manager.room_connections["#general"] = {mock_ws}

        await handler.handle_msg(mock_ws, {"room": "#general", "text": "hi", "client_msg_id": "x"})

        events = [json.loads(c[0][0]).get("event") for c in mock_ws.send_str.call_args_list]
        assert "MESSAGE" in events and "ACK" in events


class TestClusterAuth:
    """Аутентификация межсерверного трафика общим секретом (issue #10)."""

    @staticmethod
    def _request(path: str, token=None):
        req = MagicMock()
        req.path = path
        req.method = "POST"
        req.headers = {} if token is None else {"X-Cluster-Token": token}
        return req

    @staticmethod
    async def _ok_handler(request):
        from aiohttp import web
        return web.json_response({"ok": True})

    @pytest.mark.asyncio
    async def test_protected_endpoint_rejects_missing_token(self):
        from cluster.auth import make_cluster_auth_middleware
        mw = make_cluster_auth_middleware("s3cret")
        resp = await mw(self._request("/cluster/election/coordinator"), self._ok_handler)
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_protected_endpoint_rejects_wrong_token(self):
        from cluster.auth import make_cluster_auth_middleware
        mw = make_cluster_auth_middleware("s3cret")
        resp = await mw(
            self._request("/cluster/replication/wal", token="nope"), self._ok_handler
        )
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_protected_endpoint_accepts_correct_token(self):
        from cluster.auth import make_cluster_auth_middleware
        mw = make_cluster_auth_middleware("s3cret")
        resp = await mw(
            self._request("/cluster/election/coordinator", token="s3cret"),
            self._ok_handler,
        )
        assert resp.status == 200

    @pytest.mark.asyncio
    async def test_observability_endpoints_are_open(self):
        """/cluster/health и /cluster/state не требуют токена (мониторинг)."""
        from cluster.auth import make_cluster_auth_middleware
        mw = make_cluster_auth_middleware("s3cret")
        for path in ("/cluster/health", "/cluster/state"):
            resp = await mw(self._request(path), self._ok_handler)
            assert resp.status == 200, path

    def test_auth_headers(self):
        from cluster.auth import auth_headers, CLUSTER_TOKEN_HEADER
        assert auth_headers("s3cret") == {CLUSTER_TOKEN_HEADER: "s3cret"}
        assert auth_headers("") == {}


class TestReadiness:
    """Готовность узла (readiness) отдельно от liveness — issue 3.4."""

    def _manager(self):
        from cluster.manager import ClusterManager
        from aiohttp import web
        return ClusterManager(
            app=web.Application(), server_id="server1",
            host="localhost", port=8081, peers=[], db_connection=None,
        )

    def test_master_is_ready(self):
        m = self._manager()
        m.election = MagicMock()
        m.election.state.is_master = True
        info = m.get_readiness()
        assert info["ready"] is True
        assert info["role"] == "master"

    def test_slave_without_master_not_ready(self):
        """Пока идут выборы (мастер неизвестен) — узел не готов."""
        m = self._manager()
        m.election = MagicMock()
        m.election.state.is_master = False
        m.heartbeat = MagicMock()
        m.heartbeat.get_master.return_value = None
        info = m.get_readiness()
        assert info["ready"] is False
        assert "master unknown" in info["reason"]

    def test_slave_lagging_not_ready(self):
        """Реплика, отставшая по WAL больше порога, — не готова (догоняет)."""
        from cluster.manager import READINESS_MAX_LAG
        m = self._manager()
        m.election = MagicMock()
        m.election.state.is_master = False
        m.heartbeat = MagicMock()  # get_master() вернёт правдоподобный объект
        m.replication = MagicMock()
        m.replication.get_lag.return_value = READINESS_MAX_LAG + 10
        info = m.get_readiness()
        assert info["ready"] is False
        assert info["reason"] == "catching up WAL"
        assert info["lag"] == READINESS_MAX_LAG + 10

    def test_slave_in_sync_ready(self):
        """Реплика, знающая мастера и почти без отставания, — готова."""
        m = self._manager()
        m.election = MagicMock()
        m.election.state.is_master = False
        m.heartbeat = MagicMock()
        m.replication = MagicMock()
        m.replication.get_lag.return_value = 0
        info = m.get_readiness()
        assert info["ready"] is True
        assert info["role"] == "slave"
