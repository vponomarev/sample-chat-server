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
        on_master_changed: Optional[Callable] = None
    ):
        self.server_id = server_id
        self.host = host
        self.port = port
        
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
        
        # Таймауты
        self.election_timeout = 3.0  # секунды ожидания ответа на выборы
        self.coordinator_timeout = 2.0  # секунды на рассылку coordinator
        
        # HTTP сессия для election сообщений
        self._http_app: Optional = None  # aiohttp application для входящих запросов
        
        logging.info(f"[Election] Инициализирован для {server_id}")
    
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
        self._http_app = http_app
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5))
        self._running = True
        
        # Регистрируем handlers для входящих election запросов
        self._register_routes()
        
        logging.info(f"[Election] Запущен (role={self.role}, term={self.term})")
    
    async def stop(self):
        """Остановка election менеджера."""
        self._running = False
        if self._session:
            await self._session.close()
        logging.info("[Election] Остановлен")
    
    def _register_routes(self):
        """Регистрация HTTP routes для election."""
        if self._http_app:
            self._http_app.router.add_post("/election/start", self.handle_election_start)
            self._http_app.router.add_post("/election/ok", self.handle_election_ok)
            self._http_app.router.add_post("/election/coordinator", self.handle_coordinator)
    
    async def start_election(self):
        """Начало выборов."""
        if self.state.election_in_progress:
            logging.debug("[Election] Выборы уже идут")
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
            
            # Если coordinator не пришёл - начинаем новые выборы
            if self.state.election_in_progress:
                logging.warning("[Election] Coordinator не получен, перезапуск выборов")
                self.state.election_in_progress = False
                await self.start_election()
        else:
            # Никто не ответил - становимся master
            await self._become_master()
    
    async def _send_election_message(self, peer: Dict) -> bool:
        """Отправка ELECTION сообщения пиру."""
        try:
            url = f"http://{peer['host']}:{peer['port']}/election/start"
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
        
        # Если термин выше - обновляем
        if term > self.state.current_term:
            self.state.current_term = term
            self.state.voted_for = None
            self.state.is_master = False
        
        # Отвечаем OK (мы живы и можем участвовать)
        if self._running:
            # Начинаем свои выборы
            asyncio.create_task(self.start_election())
            
            return web.json_response({
                "ok": True,
                "term": self.state.current_term,
                "server_id": self.server_id
            })
        
        return web.json_response({"ok": False}, status=503)
    
    async def handle_election_ok(self, request) -> aiohttp.web.Response:
        """Обработка OK ответа."""
        from aiohttp import web
        
        data = await request.json()
        logging.debug(f"[Election] Получен OK от {data.get('server_id')}")
        return web.json_response({"received": True})
    
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
    
    async def _become_master(self):
        """Стать master."""
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
            url = f"http://{peer['host']}:{peer['port']}/election/coordinator"
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
        # В простой версии считаем всех пиров живыми
        # Heartbeat менеджер предоставит актуальную информацию
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
