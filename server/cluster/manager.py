"""
Cluster Manager - интеграция heartbeat, election и replication.
"""

import asyncio
import logging
import time
from typing import List, Dict, Optional

from aiohttp import web

from cluster.heartbeat import HeartbeatManager
from cluster.election import BullyElection
from cluster.replication import WALReplication
from cluster.peer_handler import setup_cluster_routes
from observability.metrics import (
    update_cluster_is_master,
    update_replication_lag,
    update_uptime,
    update_peers_alive
)


class ClusterManager:
    """
    Менеджер кластера - объединяет heartbeat, election и replication.
    """

    def __init__(
        self,
        app: web.Application,
        server_id: str,
        host: str,
        port: int,
        peers: List[Dict],
        db_connection
    ):
        self.app = app
        self.server_id = server_id
        self.host = host
        self.port = port
        self.db = db_connection
        self.peers = peers
        self.start_time = time.time()

        # Компоненты
        self.heartbeat: Optional[HeartbeatManager] = None
        self.election: Optional[BullyElection] = None
        self.replication: Optional[WALReplication] = None

        # Состояние
        self._running = False

        # Задача для обновления метрик
        self._metrics_task: Optional[asyncio.Task] = None

        logging.info(f"[Cluster] Инициализирован для {server_id}")

    @property
    def uptime(self) -> int:
        return int(time.time() - self.start_time)

    @property
    def is_master(self) -> bool:
        return self.election.state.is_master if self.election else False

    async def start(self):
        """Запуск кластера."""
        logging.info("[Cluster] Запуск...")

        # Настройка маршрутов
        setup_cluster_routes(self.app)

        # Инициализация heartbeat
        self.heartbeat = HeartbeatManager(
            server_id=self.server_id,
            host=self.host,
            port=self.port,
            peers=self.peers,
            on_peer_down=self._on_peer_down,
            on_peer_up=self._on_peer_up
        )

        # Инициализация election
        self.election = BullyElection(
            server_id=self.server_id,
            host=self.host,
            port=self.port,
            peers=self.peers,
            on_become_master=self._on_become_master,
            on_become_slave=self._on_become_slave,
            on_master_changed=self._on_master_changed
        )

        # Инициализация replication
        self.replication = WALReplication(
            server_id=self.server_id,
            db_connection=self.db,
            is_master=True,  # Начинаем как master, election определит
            peers=self.peers
        )

        # Запуск компонентов
        await self.heartbeat.start()
        await self.election.start(self.app)
        await self.replication.start()

        # Начинаем выборы (если нет известного master)
        await self.election.start_election()

        # Запуск обновления метрик
        self._metrics_task = asyncio.create_task(self._update_metrics_loop())

        self._running = True
        logging.info("[Cluster] Запущен")

    async def stop(self):
        """Остановка кластера."""
        logging.info("[Cluster] Остановка...")

        self._running = False

        if self._metrics_task:
            self._metrics_task.cancel()
            try:
                await self._metrics_task
            except asyncio.CancelledError:
                pass

        if self.replication:
            await self.replication.stop()
        if self.election:
            await self.election.stop()
        if self.heartbeat:
            await self.heartbeat.stop()

        logging.info("[Cluster] Остановлен")

    async def _update_metrics_loop(self):
        """Периодическое обновление метрик."""
        while self._running:
            try:
                # Обновление метрик кластера
                update_cluster_is_master(self.is_master, self.server_id)
                update_uptime(self.uptime, self.server_id)

                # Количество живых пиров
                if self.heartbeat:
                    alive_count = len([p for p in self.heartbeat.peers.values() if p.is_alive])
                    update_peers_alive(alive_count, self.server_id)

                # Отставание репликации
                if self.replication and not self.is_master:
                    lag = self.replication.get_lag()
                    update_replication_lag(lag, self.server_id)
                else:
                    update_replication_lag(0, self.server_id)

                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"[Cluster] Ошибка обновления метрик: {e}")
                await asyncio.sleep(5)

    # === Callbacks ===

    async def _on_peer_down(self, peer):
        """Вызывается когда пир помечен как мёртвый."""
        logging.warning(f"[Cluster] Пир {peer.server_id} мёртв")

        # Если это был master - начинаем выборы
        if peer.role == "master" and self.election:
            logging.info("[Cluster] Master мёртв, начинаем выборы")
            await self.election.start_election()

    async def _on_peer_up(self, peer):
        """Вызывается когда пир ожил."""
        logging.info(f"[Cluster] Пир {peer.server_id} ожил")

        # Если мы master - синхронизируем нового пира
        if self.is_master and self.replication:
            logging.info(f"[Cluster] Синхронизация пира {peer.server_id}")
            # Репликация сама разберётся при получении запроса

    async def _on_become_master(self):
        """Вызывается когда становимся master."""
        logging.info(f"[Cluster] {self.server_id} стал MASTER")

        if self.replication:
            self.replication.set_master(True)

        # Уведомляем клиентов о смене master
        await self._notify_clients_master_changed()

    async def _on_become_slave(self):
        """Вызывается когда становимся slave."""
        logging.info(f"[Cluster] {self.server_id} стал SLAVE")

        if self.replication:
            self.replication.set_master(False)

        # Уведомляем клиентов о смене master
        await self._notify_clients_master_changed()

    async def _on_master_changed(self, new_master_id: str):
        """Вызывается при смене master."""
        logging.info(f"[Cluster] Новый master: {new_master_id}")

        # Уведомляем клиенты о смене master
        await self._notify_clients_master_changed()

    async def _notify_clients_master_changed(self):
        """Рассылка клиентам обновлённого списка серверов."""
        ws_manager = self.app.get("ws_manager")
        if not ws_manager:
            return

        # Формируем новый список серверов
        servers = self.get_cluster_servers()

        message = {
            "event": "SERVER_LIST",
            "servers": servers
        }

        # Рассылаем всем подключённым
        for ws in ws_manager.connections:
            try:
                await ws_manager.send_to(ws, message)
            except Exception as e:
                logging.debug(f"[Cluster] Ошибка отправки SERVER_LIST: {e}")

    def get_cluster_servers(self) -> List[Dict]:
        """Получение списка серверов кластера."""
        servers = []

        # Добавляем себя
        servers.append({
            "host": self.host,
            "port": self.port,
            "server_id": self.server_id,
            "role": "master" if self.is_master else "slave"
        })

        # Добавляем пиры
        if self.heartbeat:
            for peer in self.heartbeat.peers.values():
                servers.append({
                    "host": peer.host,
                    "port": peer.port,
                    "server_id": peer.server_id,
                    "role": peer.role if peer.is_alive else "dead"
                })

        return servers

    def get_master_server(self) -> Optional[Dict]:
        """Получение информации о master."""
        if self.is_master:
            return {
                "host": self.host,
                "port": self.port,
                "server_id": self.server_id
            }

        if self.heartbeat:
            master = self.heartbeat.get_master()
            if master:
                return {
                    "host": master.host,
                    "port": master.port,
                    "server_id": master.server_id
                }

        return None
