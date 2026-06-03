"""
WAL (Write-Ahead Log) репликация.
Master отправляет WAL записи всем slave серверам.
"""

import asyncio
import json
import logging
import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

import aiohttp
from aiosqlite import Connection


@dataclass
class WALEntry:
    """Запись WAL журнала."""
    seq: int
    ts: int
    operation: str  # INSERT, DELETE, UPDATE
    table_name: str
    data: Dict[str, Any]
    
    def to_dict(self) -> Dict:
        return {
            "seq": self.seq,
            "ts": self.ts,
            "operation": self.operation,
            "table_name": self.table_name,
            "data": self.data
        }
    
    @classmethod
    def from_dict(cls, d: Dict) -> 'WALEntry':
        return cls(
            seq=d["seq"],
            ts=d["ts"],
            operation=d["operation"],
            table_name=d["table_name"],
            data=d["data"]
        )


class WALReplication:
    """
    WAL репликация между master и slave.
    
    Master:
    - Записывает операции в WAL таблицу
    - Отправляет WAL записи всем slave
    
    Slave:
    - Получает WAL записи от master
    - Применяет к своей базе данных
    - Отправляет ACK master
    """
    
    def __init__(
        self,
        server_id: str,
        db_connection: Connection,
        is_master: bool = False,
        peers: List[Dict] = None
    ):
        self.server_id = server_id
        self.db = db_connection
        self.is_master = is_master
        self.peers = peers or []
        
        # Состояние
        self._last_applied_seq = 0
        self._pending_acks: Dict[int, asyncio.Event] = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        
        # Для slave - очередь получения записей
        self._wal_queue: asyncio.Queue = asyncio.Queue()
        self._apply_task: Optional[asyncio.Task] = None
        
        logging.info(f"[WAL] Инициализирован для {server_id} (master={is_master})")
    
    async def start(self):
        """Запуск репликации."""
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        self._running = True
        
        # Получаем последний применённый seq
        await self._load_last_seq()
        
        if not self.is_master:
            # Slave запускает задачу применения WAL
            self._apply_task = asyncio.create_task(self._apply_wal_loop())
        
        logging.info(f"[WAL] Запущен (last_seq={self._last_applied_seq})")
    
    async def stop(self):
        """Остановка репликации."""
        self._running = False
        
        if self._apply_task:
            self._apply_task.cancel()
            try:
                await self._apply_task
            except asyncio.CancelledError:
                pass
        
        if self._session:
            await self._session.close()
        
        logging.info("[WAL] Остановлен")
    
    async def _load_last_seq(self):
        """Загрузка последнего применённого seq."""
        try:
            async with self.db.execute(
                "SELECT value FROM cluster_meta WHERE key = 'last_applied_seq'"
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    self._last_applied_seq = int(row[0])
                else:
                    # Инициализация
                    await self.db.execute(
                        "INSERT OR REPLACE INTO cluster_meta (key, value) VALUES (?, ?)",
                        ("last_applied_seq", "0")
                    )
                    await self.db.commit()
        except Exception as e:
            logging.error(f"[WAL] Ошибка загрузки last_seq: {e}")
    
    # === Master методы ===
    
    async def log_operation(
        self,
        operation: str,
        table_name: str,
        data: Dict[str, Any]
    ) -> int:
        """
        Логирование операции в WAL (только для master).
        Возвращает seq записи.
        """
        if not self.is_master:
            raise RuntimeError("Только master может записывать в WAL")
        
        ts = int(time.time())
        
        # Вставка в WAL таблицу
        cursor = await self.db.execute(
            """
            INSERT INTO wal (ts, operation, table_name, data)
            VALUES (?, ?, ?, ?)
            """,
            (ts, operation, table_name, json.dumps(data))
        )
        await self.db.commit()
        
        # Получаем seq
        seq = cursor.lastrowid
        
        logging.debug(f"[WAL] Записана операция: seq={seq}, op={operation}, table={table_name}")
        
        # Отправка slave
        await self._broadcast_wal_entry(seq, ts, operation, table_name, data)
        
        return seq
    
    async def _broadcast_wal_entry(
        self,
        seq: int,
        ts: int,
        operation: str,
        table_name: str,
        data: Dict[str, Any]
    ):
        """Рассылка WAL записи всем slave."""
        entry = {
            "type": "WAL_APPEND",
            "entries": [{
                "seq": seq,
                "ts": ts,
                "operation": operation,
                "table_name": table_name,
                "data": data
            }]
        }
        
        tasks = []
        for peer in self.peers:
            if peer.get("server_id") != self.server_id:
                tasks.append(self._send_wal_to_peer(peer, entry))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _send_wal_to_peer(self, peer: Dict, entry: Dict):
        """Отправка WAL записи пиру."""
        try:
            # WebSocket для репликации (будет реализован в peer_handler)
            url = f"http://{peer['host']}:{peer['port']}/replication/wal"
            async with self._session.post(
                url,
                json=entry,
                raise_for_status=True
            ) as response:
                result = await response.json()
                logging.debug(f"[WAL] Отправлено {peer.get('server_id')}: seq={entry['entries'][0]['seq']}")
                return result
        except Exception as e:
            logging.error(f"[WAL] Ошибка отправки {peer.get('server_id')}: {e}")
            return None
    
    async def get_wal_entries(self, after_seq: int) -> List[WALEntry]:
        """Получение WAL записей после указанного seq (для sync slave)."""
        async with self.db.execute(
            "SELECT seq, ts, operation, table_name, data FROM wal WHERE seq > ? ORDER BY seq",
            (after_seq,)
        ) as cursor:
            entries = []
            async for row in cursor:
                entries.append(WALEntry(
                    seq=row[0],
                    ts=row[1],
                    operation=row[2],
                    table_name=row[3],
                    data=json.loads(row[4])
                ))
            return entries
    
    # === Slave методы ===
    
    async def apply_wal_entry(self, entry: Dict) -> bool:
        """Применение WAL записи (для slave)."""
        try:
            wal_entry = WALEntry.from_dict(entry)
            
            # Пропускаем если уже применено
            if wal_entry.seq <= self._last_applied_seq:
                logging.debug(f"[WAL] Пропущена запись seq={wal_entry.seq} (уже применено)")
                return True
            
            # Применяем операцию
            await self._apply_operation(wal_entry)
            
            # Обновляем last_applied_seq
            self._last_applied_seq = wal_entry.seq
            await self.db.execute(
                "UPDATE cluster_meta SET value = ? WHERE key = 'last_applied_seq'",
                (str(self._last_applied_seq),)
            )
            await self.db.commit()
            
            logging.debug(f"[WAL] Применена запись: seq={wal_entry.seq}, op={wal_entry.operation}")
            
            return True
            
        except Exception as e:
            logging.error(f"[WAL] Ошибка применения записи: {e}")
            return False
    
    async def _apply_operation(self, entry: WALEntry):
        """Применение операции к базе данных."""
        if entry.operation == "INSERT":
            await self._apply_insert(entry)
        elif entry.operation == "DELETE":
            await self._apply_delete(entry)
        elif entry.operation == "UPDATE":
            await self._apply_update(entry)
    
    async def _apply_insert(self, entry: WALEntry):
        """Применение INSERT операции."""
        table = entry.table_name
        data = entry.data
        
        if table == "users":
            await self.db.execute(
                "INSERT OR REPLACE INTO users (nick, password, created_at) VALUES (?, ?, ?)",
                (data["nick"], data.get("password"), data["created_at"])
            )
        elif table == "rooms":
            await self.db.execute(
                "INSERT OR REPLACE INTO rooms (name, owner, created_at) VALUES (?, ?, ?)",
                (data["name"], data["owner"], data["created_at"])
            )
        elif table == "room_members":
            await self.db.execute(
                "INSERT OR REPLACE INTO room_members (room, nick, joined_at) VALUES (?, ?, ?)",
                (data["room"], data["nick"], data["joined_at"])
            )
        elif table == "messages":
            await self.db.execute(
                "INSERT OR REPLACE INTO messages (msg_id, room, nick, text, ts) VALUES (?, ?, ?, ?, ?)",
                (data["msg_id"], data["room"], data["nick"], data["text"], data["ts"])
            )
        
        await self.db.commit()
    
    async def _apply_delete(self, entry: WALEntry):
        """Применение DELETE операции."""
        table = entry.table_name
        data = entry.data
        
        if table == "users":
            await self.db.execute("DELETE FROM users WHERE nick = ?", (data["nick"],))
        elif table == "rooms":
            await self.db.execute("DELETE FROM rooms WHERE name = ?", (data["name"],))
        elif table == "room_members":
            await self.db.execute(
                "DELETE FROM room_members WHERE room = ? AND nick = ?",
                (data["room"], data["nick"])
            )
        elif table == "messages":
            await self.db.execute("DELETE FROM messages WHERE msg_id = ?", (data["msg_id"],))
        
        await self.db.commit()
    
    async def _apply_update(self, entry: WALEntry):
        """Применение UPDATE операции."""
        # Заглушка для будущих UPDATE операций
        pass
    
    async def _apply_wal_loop(self):
        """Цикл применения WAL записей (для slave)."""
        while self._running:
            try:
                entry = await self._wal_queue.get()
                await self.apply_wal_entry(entry)
                self._wal_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"[WAL] Ошибка в цикле применения: {e}")
    
    async def request_sync(self, master_url: str) -> bool:
        """Запрос синхронизации с master (при подключении slave)."""
        try:
            url = f"{master_url}/replication/sync?after_seq={self._last_applied_seq}"
            async with self._session.get(url, raise_for_status=True) as response:
                data = await response.json()
                entries = data.get("entries", [])
                
                logging.info(f"[WAL] Синхронизация: получено {len(entries)} записей")
                
                for entry in entries:
                    await self.apply_wal_entry(entry)
                
                return True
        except Exception as e:
            logging.error(f"[WAL] Ошибка синхронизации: {e}")
            return False
    
    def get_lag(self) -> int:
        """Получение отставания репликации (для метрик)."""
        # В простой версии возвращаем 0
        # В полной - разница между last master seq и last_applied_seq
        return 0
    
    def set_master(self, is_master: bool):
        """Установка режима master/slave."""
        self.is_master = is_master
        logging.info(f"[WAL] Режим изменён: master={is_master}")


class WALWriter:
    """
    Обёртка для записи операций с автоматическим логированием в WAL.
    Используется на master.
    """
    
    def __init__(self, replication: WALReplication):
        self.replication = replication
    
    async def insert(self, table: str, data: Dict):
        """INSERT с WAL логированием."""
        seq = await self.replication.log_operation("INSERT", table, data)
        return seq
    
    async def delete(self, table: str, key_field: str, key_value: Any):
        """DELETE с WAL логированием."""
        seq = await self.replication.log_operation(
            "DELETE",
            table,
            {key_field: key_value}
        )
        return seq
