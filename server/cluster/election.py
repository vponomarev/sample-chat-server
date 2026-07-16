"""
Leader Election (Bully Algorithm).
Сервер с наибольшим ID среди живых становится master.
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass

import aiohttp


@dataclass
class ElectionState:
    """Состояние выборов."""
    current_term: int = 0
    voted_for: Optional[str] = None
    is_master: bool = False
    master_id: Optional[str] = None
    election_in_progress: bool = False
    last_election_time: float = 0


class BullyElection:
    """
    Bully Algorithm для выбора лидера.
    
    Алгоритм:
    1. Когда сервер обнаруживает что master мёртв, он начинает выборы
    2. Отправляет ELECTION сообщение всем серверам с большим ID
    3. Если кто-то отвечает OK - этот сервер начинает свои выборы
    4. Если никто не ответил за таймаут - этот сервер становится master
    5. Новый master рассылает COORDINATOR сообщение всем
    """
    
    def __init__(
        self,
        server_id: str,
        host: str,
        port: int,
        peers: List[Dict],
        on_become_master: Optional[Callable] = None,
        on_become_slave: Optional[Callable] = None,
        on_master_changed: Optional[Callable] = None,
        secret: str = ""
    ):
        self.server_id = server_id
        self.host = host
        self.port = port
        self.secret = secret
        
        # Пирсы
        self.peers = {
            p.get("server_id", f"{p['host']}:{p['port']}"): p
            for p in peers
        }
        
        # Callbacks
        self.on_become_master = on_become_master
        self.on_become_slave = on_become_slave
        self.on_master_changed = on_master_changed
        
        # Состояние
        self.state = ElectionState()
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False

        # Источник живых пиров с большим ID (инъектируется из ClusterManager,
        # опирается на heartbeat). Если не задан — считаем всех пиров живыми.
        self._alive_higher_source: Optional[Callable] = None

        # Источник кворума (инъектируется из ClusterManager): есть ли у нас
        # большинство живых узлов кластера. Master становятся только при
        # кворуме — иначе меньшинство при сетевом разделении выбрало бы своего
        # master и получился бы split-brain (issue #11, Этап 4.1). Если источник
        # не задан (одиночный узел / часть тестов) — считаем, что кворум есть.
        self._quorum_source: Optional[Callable] = None

        # Источник живого master (обычно heartbeat.get_master). Если кластер уже
        # видит живого master через heartbeat, узел принимает его напрямую, не
        # дожидаясь COORDINATOR и не устраивая выборы. Это критично при кворум-
        # задержке становления master на старте (Этап 4.1): иначе младший узел
        # крутил бы выборы, инфлируя term, и потом отвергал бы COORDINATOR
        # старшего по своему же раздутому term. Возвращает объект с полями
        # server_id/term или None.
        self._master_alive_source: Optional[Callable] = None

        # Таймауты
        self.election_timeout = 3.0  # секунды ожидания ответа на выборы
        self.coordinator_timeout = 2.0  # секунды на рассылку coordinator

        logging.info(f"[Election] Инициализирован для {server_id}")

    def set_liveness_source(self, source: Callable):
        """
        Задаёт источник живых пиров с большим ID (обычно
        ``heartbeat.get_alive_peers_with_higher_id``). Возвращает список
        объектов с атрибутом ``server_id``.
        """
        self._alive_higher_source = source

    def set_quorum_source(self, source: Callable):
        """
        Задаёт функцию, возвращающую ``True``, если узел видит большинство
        живых узлов кластера (кворум). Обычно ``ClusterManager._has_quorum``.
        """
        self._quorum_source = source

    def _has_quorum(self) -> bool:
        """Есть ли кворум. Без источника (одиночный режим) — считаем, что да."""
        if self._quorum_source is None:
            return True
        return bool(self._quorum_source())

    def set_master_alive_source(self, source: Callable):
        """Задаёт источник живого master (обычно ``heartbeat.get_master``)."""
        self._master_alive_source = source

    def _adopt_if_master_present(self) -> bool:
        """
        Если через heartbeat виден живой master (не мы) — принимаем его: берём
        его master_id и авторитетный term, прекращаем выборы. Возвращает True,
        если приняли. Так младший узел не воюет за лидерство и не отвергает
        репликацию из-за раздутого собственного term (Этап 4.1/4.2).
        """
        if self._master_alive_source is None:
            return False
        master = self._master_alive_source()
        if not master or getattr(master, "server_id", None) == self.server_id:
            return False

        master_id = master.server_id
        master_term = getattr(master, "term", self.state.current_term)
        changed = self.state.master_id != master_id
        self.state.master_id = master_id
        self.state.is_master = False
        self.state.election_in_progress = False
        # Принимаем авторитетный term master'а (в т.ч. если наш раздулся выше).
        self.state.current_term = master_term
        if changed:
            logging.info(
                f"[Election] Принят живой master {master_id} (term={master_term}) "
                f"без COORDINATOR (по heartbeat)"
            )
        return True

    @property
    def numeric_id(self) -> int:
        """Числовой ID сервера."""
        try:
            return int(self.server_id.replace("server", ""))
        except ValueError:
            return 0
    
    @property
    def role(self) -> str:
        """Текущая роль."""
        return "master" if self.state.is_master else "slave"
    
    @property
    def term(self) -> int:
        """Текущий термин."""
        return self.state.current_term
    
    async def start(self, http_app):
        """Запуск election менеджера."""
        from cluster.auth import auth_headers
        from cluster.faults import chaos_trace_configs
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5),
            headers=auth_headers(self.secret),
            trace_configs=chaos_trace_configs(),
        )
        self._running = True

        # Входящие election-запросы обслуживают роуты /cluster/election/*
        # (регистрируются в peer_handler.setup_cluster_routes и делегируют сюда).
        logging.info(f"[Election] Запущен (role={self.role}, term={self.term})")

    async def stop(self):
        """Остановка election менеджера."""
        self._running = False
        if self._session:
            await self._session.close()
        logging.info("[Election] Остановлен")

    async def start_election(self):
        """Начало выборов."""
        if self.state.election_in_progress:
            logging.debug("[Election] Выборы уже идут")
            return

        # Уже есть живой master (узнали через heartbeat)? Принимаем его вместо
        # выборов — не воюем за лидерство и не плодим шторм на старте.
        if self._adopt_if_master_present():
            return

        self.state.election_in_progress = True
        self.state.current_term += 1
        self.state.voted_for = self.server_id
        
        logging.info(f"[Election] Начало выборов (term={self.state.current_term})")
        
        # Получаем пиров с большим ID
        higher_peers = self._get_higher_alive_peers()
        
        if not higher_peers:
            # Нет пиров с большим ID - становимся master
            await self._become_master()
            return
        
        # Отправляем ELECTION всем с большим ID
        logging.debug(f"[Election] Отправка ELECTION {len(higher_peers)} пирам с большим ID")
        
        ok_received = False
        tasks = []
        
        for peer_id in higher_peers:
            peer = self.peers[peer_id]
            tasks.append(self._send_election_message(peer))
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            ok_received = any(r is True for r in results)
        
        if ok_received:
            # Кто-то ответил OK - ждём coordinator
            logging.debug("[Election] Получен OK, ждём coordinator...")
            await asyncio.sleep(self.election_timeout)
            
            # Если coordinator не пришёл — сперва проверяем, не появился ли уже
            # живой master (частая ситуация при кворум-задержке на старте: старший
            # узел стал master и виден по heartbeat, но COORDINATOR разошёлся с
            # нашим term). Если да — принимаем его. Иначе перезапускаем выборы.
            if self.state.election_in_progress:
                self.state.election_in_progress = False
                if self._adopt_if_master_present():
                    return
                logging.warning("[Election] Coordinator не получен, перезапуск выборов")
                await self.start_election()
        else:
            # Никто не ответил - становимся master
            await self._become_master()
    
    async def _send_election_message(self, peer: Dict) -> bool:
        """Отправка ELECTION сообщения пиру."""
        try:
            url = f"http://{peer['host']}:{peer['port']}/cluster/election/start"
            async with self._session.post(
                url,
                json={
                    "candidate_id": self.server_id,
                    "term": self.state.current_term
                },
                raise_for_status=True
            ) as response:
                data = await response.json()
                return data.get("ok", False)
        except Exception as e:
            logging.debug(f"[Election] Ошибка отправки ELECTION {peer.get('server_id')}: {e}")
            return False
    
    async def handle_election_start(self, request) -> aiohttp.web.Response:
        """Обработка входящего ELECTION сообщения."""
        from aiohttp import web
        
        data = await request.json()
        candidate_id = data.get("candidate_id")
        term = data.get("term", 0)
        
        logging.debug(f"[Election] Получен ELECTION от {candidate_id} (term={term})")

        if not self._running:
            return web.json_response({"ok": False}, status=503)

        # Принимаем более высокий term (кандидат мог уйти вперёд из-за
        # повторных выборов). Это важно, чтобы наш ответный COORDINATOR нёс
        # term >= term кандидата и был им принят.
        if term > self.state.current_term:
            self.state.current_term = term
            self.state.voted_for = None

        # Если мы уже master — не уступаем лидерство, а переподтверждаем его,
        # рассылая COORDINATOR (в т.ч. на текущем, возможно поднятом, term).
        # Без этого «мечущийся» узел бесконечно перезапускал бы выборы.
        if self.state.is_master:
            asyncio.create_task(self._broadcast_coordinator())
            return web.json_response({
                "ok": True,
                "term": self.state.current_term,
                "server_id": self.server_id,
                "is_master": True,
            })

        # Иначе — отвечаем OK и запускаем свои выборы (Bully: у нас ID больше,
        # чем у кандидата, значит шанс стать master есть). Не плодим выборы,
        # если они уже идут.
        if not self.state.election_in_progress:
            asyncio.create_task(self.start_election())

        return web.json_response({
            "ok": True,
            "term": self.state.current_term,
            "server_id": self.server_id
        })
    
    async def handle_coordinator(self, request) -> aiohttp.web.Response:
        """Обработка COORDINATOR сообщения (новый master)."""
        from aiohttp import web
        
        data = await request.json()
        new_master_id = data.get("master_id")
        term = data.get("term", 0)
        
        logging.info(f"[Election] Получен COORDINATOR: новый master = {new_master_id} (term={term})")
        
        # Обновляем состояние
        if term >= self.state.current_term:
            self.state.current_term = term
            self.state.master_id = new_master_id
            self.state.is_master = (new_master_id == self.server_id)
            self.state.election_in_progress = False
            
            if not self.state.is_master and self.on_become_slave:
                await self.on_become_slave()
            
            if self.on_master_changed:
                await self.on_master_changed(new_master_id)
        
        return web.json_response({"received": True})
    
    async def step_down(self):
        """
        Сложить полномочия master и перевыбраться. Вызывается при обнаружении
        живого master с большим ID (разрешение split-brain после гонок старта).
        """
        if self.state.is_master:
            logging.warning(f"[Election] {self.server_id} слагает полномочия master")
            self.state.is_master = False
            self.state.master_id = None
        await self.start_election()

    async def _become_master(self):
        """Стать master."""
        # Кворум (issue #11, Этап 4.1): не становимся master, если не видим
        # большинство узлов. Так меньшинство при сетевом разделении остаётся
        # без master (недоступно на запись), но не плодит второго master —
        # split-brain по записи невозможен.
        if not self._has_quorum():
            logging.warning(
                "[Election] Нет кворума — остаёмся slave, master не становимся"
            )
            self.state.election_in_progress = False
            self.state.is_master = False
            return

        self.state.is_master = True
        self.state.master_id = self.server_id
        self.state.election_in_progress = False
        self.state.last_election_time = time.time()
        
        logging.info(f"[Election] {self.server_id} стал MASTER (term={self.state.current_term})")
        
        # Рассылаем COORDINATOR всем
        await self._broadcast_coordinator()
        
        if self.on_become_master:
            await self.on_become_master()
        
        if self.on_master_changed:
            await self.on_master_changed(self.server_id)
    
    async def _broadcast_coordinator(self):
        """Рассылка COORDINATOR сообщения всем пирам."""
        logging.debug(f"[Election] Рассылка COORDINATOR (term={self.state.current_term})")
        
        tasks = []
        for peer_id, peer in self.peers.items():
            if peer_id != self.server_id:
                tasks.append(self._send_coordinator_to_peer(peer))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _send_coordinator_to_peer(self, peer: Dict):
        """Отправка COORDINATOR пиру."""
        try:
            url = f"http://{peer['host']}:{peer['port']}/cluster/election/coordinator"
            async with self._session.post(
                url,
                json={
                    "master_id": self.server_id,
                    "term": self.state.current_term
                },
                timeout=aiohttp.ClientTimeout(total=self.coordinator_timeout)
            ) as response:
                await response.json()
                logging.debug(f"[Election] COORDINATOR отправлен {peer.get('server_id')}")
        except Exception as e:
            logging.debug(f"[Election] Ошибка отправки COORDINATOR {peer.get('server_id')}: {e}")
    
    def _get_higher_alive_peers(self) -> List[str]:
        """Получение ID живых пиров с большим ID."""
        # Актуальную живость даёт heartbeat (инъектируется через
        # set_liveness_source). Иначе — деградируем к «все пиры живы».
        if self._alive_higher_source is not None:
            return [p.server_id for p in self._alive_higher_source()]

        return [
            pid for pid in self.peers.keys()
            if pid != self.server_id and self._get_numeric_id(pid) > self.numeric_id
        ]
    
    def _get_numeric_id(self, server_id: str) -> int:
        """Извлечение числового ID."""
        try:
            return int(server_id.replace("server", ""))
        except ValueError:
            return 0
    
    def get_state(self) -> Dict:
        """Получение состояния."""
        return {
            "server_id": self.server_id,
            "role": self.role,
            "term": self.state.current_term,
            "master_id": self.state.master_id,
            "is_master": self.state.is_master,
            "election_in_progress": self.state.election_in_progress,
        }
