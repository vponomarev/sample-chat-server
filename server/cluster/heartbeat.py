"""
Heartbeat механизм для кластера.
Отправляет периодические ping между серверами для обнаружения живых узлов.
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional
from dataclasses import dataclass, field

import aiohttp


@dataclass
class ServerInfo:
    """Информация о сервере в кластере."""
    host: str
    port: int
    server_id: str
    role: str = "slave"  # master, slave
    term: int = 0
    is_alive: bool = True
    last_heartbeat: float = field(default_factory=time.time)
    consecutive_failures: int = 0
    uptime: int = 0
    
    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"
    
    @property
    def health_url(self) -> str:
        # /cluster/health отдаёт role/term/master_id/uptime — без этого сосед
        # не знает роль пира и не может обнаружить падение master.
        return f"{self.url}/cluster/health"


class HeartbeatManager:
    """Управление heartbeat между серверами кластера."""
    
    def __init__(
        self,
        server_id: str,
        host: str,
        port: int,
        peers: List[Dict],
        on_peer_down=None,
        on_peer_up=None,
        secret: str = ""
    ):
        self.server_id = server_id
        self.host = host
        self.port = port
        self.secret = secret
        self.start_time = time.time()
        
        # Список пиров
        self.peers: Dict[str, ServerInfo] = {}
        for peer in peers:
            peer_id = peer.get("server_id", f"{peer['host']}:{peer['port']}")
            self.peers[peer_id] = ServerInfo(
                host=peer["host"],
                port=peer["port"],
                server_id=peer_id,
                role="slave"
            )
        
        # Callbacks
        self.on_peer_down = on_peer_down
        self.on_peer_up = on_peer_up
        
        # Настройки
        self.heartbeat_interval = 2.0  # секунды
        self.timeout_threshold = 3  # количество пропусков до считания мёртвым
        
        # Состояние
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Метрики
        self.heartbeat_failures = 0
        
        logging.info(f"[Heartbeat] Инициализирован для {server_id} с {len(self.peers)} пирами")
    
    async def start(self):
        """Запуск heartbeat цикла."""
        self._running = True
        from cluster.auth import auth_headers
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5),
            headers=auth_headers(self.secret),
        )
        self._task = asyncio.create_task(self._heartbeat_loop())
        logging.info("[Heartbeat] Запущен")
    
    async def stop(self):
        """Остановка heartbeat."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.close()
        logging.info("[Heartbeat] Остановлен")
    
    async def _heartbeat_loop(self):
        """Основной цикл heartbeat."""
        while self._running:
            try:
                await self._send_heartbeats()
                await asyncio.sleep(self.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"[Heartbeat] Ошибка в цикле: {e}")
                await asyncio.sleep(self.heartbeat_interval)
    
    async def _send_heartbeats(self):
        """Отправка heartbeat всем пирам."""
        tasks = []
        for peer_id, peer in self.peers.items():
            tasks.append(self._send_heartbeat_to_peer(peer))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _send_heartbeat_to_peer(self, peer: ServerInfo):
        """Отправка heartbeat конкретному пиру."""
        try:
            async with self._session.get(
                peer.health_url,
                raise_for_status=True
            ) as response:
                data = await response.json()
                
                # Обновление информации о пире
                was_alive = peer.is_alive
                peer.is_alive = True
                peer.last_heartbeat = time.time()
                peer.consecutive_failures = 0
                peer.role = data.get("role", "slave")
                peer.term = data.get("term", 0)
                peer.uptime = data.get("uptime", 0)
                
                # Если пир был мёртв и ожил
                if not was_alive and self.on_peer_up:
                    await self.on_peer_up(peer)
                
                logging.debug(f"[Heartbeat] {peer.server_id}: OK (role={peer.role}, term={peer.term})")
                
        except asyncio.TimeoutError:
            await self._handle_peer_timeout(peer)
        except aiohttp.ClientError as e:
            await self._handle_peer_timeout(peer)
        except Exception as e:
            logging.debug(f"[Heartbeat] {peer.server_id}: Ошибка {e}")
            await self._handle_peer_timeout(peer)
    
    async def _handle_peer_timeout(self, peer: ServerInfo):
        """Обработка таймаута heartbeat."""
        peer.consecutive_failures += 1
        self.heartbeat_failures += 1
        
        was_alive = peer.is_alive
        peer.is_alive = peer.consecutive_failures < self.timeout_threshold
        
        logging.debug(
            f"[Heartbeat] {peer.server_id}: TIMEOUT "
            f"(failures={peer.consecutive_failures}/{self.timeout_threshold}, "
            f"alive={peer.is_alive})"
        )
        
        # Если пир стал считаться мёртвым
        if was_alive and not peer.is_alive:
            logging.warning(f"[Heartbeat] Пир {peer.server_id} помечен как мёртвый")
            if self.on_peer_down:
                await self.on_peer_down(peer)
    
    def get_alive_peers(self) -> List[ServerInfo]:
        """Получение списка живых пиров."""
        return [p for p in self.peers.values() if p.is_alive]
    
    def get_dead_peers(self) -> List[ServerInfo]:
        """Получение списка мёртвых пиров."""
        return [p for p in self.peers.values() if not p.is_alive]
    
    def get_alive_peers_with_higher_id(self) -> List[ServerInfo]:
        """Получение живых пиров с большим ID (для Bully algorithm)."""
        try:
            my_id = int(self.server_id.replace("server", ""))
        except ValueError:
            my_id = 0
        
        return [
            p for p in self.get_alive_peers()
            if self._get_peer_numeric_id(p.server_id) > my_id
        ]
    
    def _get_peer_numeric_id(self, server_id: str) -> int:
        """Извлечение числового ID из server_id."""
        try:
            return int(server_id.replace("server", ""))
        except ValueError:
            return 0
    
    def is_master_alive(self) -> bool:
        """Проверка жив ли master."""
        for peer in self.peers.values():
            if peer.role == "master" and peer.is_alive:
                return True
        return False
    
    def get_master(self) -> Optional[ServerInfo]:
        """Получение информации о master."""
        for peer in self.peers.values():
            if peer.role == "master" and peer.is_alive:
                return peer
        return None
    
    def get_cluster_state(self) -> Dict:
        """Получение состояния кластера."""
        return {
            "self": {
                "server_id": self.server_id,
                "host": self.host,
                "port": self.port,
                "uptime": int(time.time() - self.start_time),
            },
            "peers": [
                {
                    "server_id": p.server_id,
                    "host": p.host,
                    "port": p.port,
                    "role": p.role,
                    "term": p.term,
                    "is_alive": p.is_alive,
                    "consecutive_failures": p.consecutive_failures,
                }
                for p in self.peers.values()
            ],
            "metrics": {
                "heartbeat_failures": self.heartbeat_failures,
            }
        }
