"""
WAL (Write-Ahead Log) репликация.
Master отправляет WAL записи всем slave серверам.
"""

import asyncio
import json
import logging
import time
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field

import aiohttp
from aiosqlite import Connection

from cluster.circuit_breaker import CircuitBreaker
from cluster.snapshot import SnapshotManager

# Период фоновой повторной доставки WAL отстающим пирам (Этап 3.3).
RETRY_INTERVAL_SEC = 2.0

# Порог размера WAL (записей), при превышении которого master делает снапшот и
# обрезает журнал (Этап 3.5). Небольшое значение — чтобы поведение было видно в
# демо; в проде порог был бы куда больше.
SNAPSHOT_WAL_THRESHOLD = 1000


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
        peers: List[Dict] = None,
        secret: str = ""
    ):
        self.server_id = server_id
        self.db = db_connection
        self.is_master = is_master
        self.peers = peers or []
        self.secret = secret
        
        # Состояние
        self._last_applied_seq = 0
        self._last_master_seq = 0  # максимальный seq, известный от master (для lag)
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._syncing = False  # флаг, чтобы не запускать несколько догонов сразу

        # Локатор master (инъектируется из ClusterManager) для запроса догона
        self._master_locator: Optional[Callable] = None

        # Fencing по term (issue #11, Этап 4.1): master штампует свой term в
        # каждом WAL-пуше, реплика отвергает пуш со term ниже уже виденного.
        # Так «старый» master, переживший сетевое разделение, после заживления
        # сети не перезапишет данные, принятые от нового master с большим term.
        # _fencing_term — наибольший term, от которого мы принимали записи;
        # _term_source отдаёт актуальный term из выборов (инъектируется).
        self._fencing_term = 0
        self._term_source: Optional[Callable] = None

        # По одному circuit breaker на пира: не долбим недоступную реплику
        # каждым сообщением (issue B11, Этап 3.2).
        self._breakers: Dict[str, CircuitBreaker] = {}

        # ACK-репликация (issue B3, Этап 3.3): master помнит, до какого seq
        # каждый пир подтвердил применение. Отправляем пиру всё после его
        # подтверждённого seq (бэклог + новое), а фоновый retry-loop повторяет
        # доставку отставшим — так пропущенная запись догоняется даже без новых
        # записей (это закрывает «single-write gap»). Пуш идемпотентен: уже
        # применённые записи реплика молча пропускает.
        self._peer_acked: Dict[str, int] = {}
        self._retry_task: Optional[asyncio.Task] = None

        # Снапшоты + компакция WAL (Этап 3.5)
        self._snapshots = SnapshotManager(db_connection)

        logging.info(f"[WAL] Инициализирован для {server_id} (master={is_master})")

    def _breaker(self, peer_id: str) -> CircuitBreaker:
        """Возвращает (создавая при необходимости) breaker для пира."""
        br = self._breakers.get(peer_id)
        if br is None:
            br = CircuitBreaker(name=f"wal->{peer_id}")
            self._breakers[peer_id] = br
        return br

    def set_master_locator(self, locator: Callable):
        """Задаёт функцию, возвращающую dict master-сервера {host, port} (или None)."""
        self._master_locator = locator

    def set_term_source(self, source: Callable):
        """Задаёт функцию, возвращающую текущий term выборов (для fencing)."""
        self._term_source = source

    def _current_term(self) -> int:
        """Актуальный term (из выборов). Без источника — 0."""
        return int(self._term_source()) if self._term_source else 0

    def should_accept_term(self, term: int) -> bool:
        """
        Fencing-проверка (Этап 4.1): принимать ли WAL-пуш с данным term.

        Отвергаем всё, что ниже наибольшего term, который мы уже видели (от
        выборов или от прежних принятых пушей) — это записи от устаревшего
        master. Пуш с term >= порога принимаем и запоминаем term как новый пол.
        """
        floor = max(self._fencing_term, self._current_term())
        if term < floor:
            return False
        if term > self._fencing_term:
            self._fencing_term = term
        return True
    
    async def start(self):
        """Запуск репликации."""
        from cluster.auth import auth_headers
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10),
            headers=auth_headers(self.secret),
        )
        self._running = True

        # Получаем последний применённый seq
        await self._load_last_seq()

        # Фоновая повторная доставка отстающим пирам (работает только на master)
        self._retry_task = asyncio.create_task(self._retry_loop())

        logging.info(f"[WAL] Запущен (last_seq={self._last_applied_seq})")

    async def stop(self):
        """Остановка репликации."""
        self._running = False

        if self._retry_task:
            self._retry_task.cancel()
            try:
                await self._retry_task
            except asyncio.CancelledError:
                pass

        if self._session:
            await self._session.close()

        logging.info("[WAL] Остановлен")

    @property
    def last_applied_seq(self) -> int:
        """Последний применённый seq (используется в ACK)."""
        return self._last_applied_seq

    async def _retry_loop(self):
        """Дослывает WAL отставшим пирам и компактит журнал (только master)."""
        while self._running:
            try:
                await asyncio.sleep(RETRY_INTERVAL_SEC)
                if self.is_master:
                    await self._replicate_pending()
                    await self._maybe_compact()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logging.error(f"[WAL] Ошибка retry-loop: {e}")

    async def _maybe_compact(self):
        """Если WAL перерос порог — делаем снапшот и обрезаем журнал."""
        async with self.db.execute("SELECT COUNT(*) FROM wal") as cursor:
            row = await cursor.fetchone()
        if row and row[0] > SNAPSHOT_WAL_THRESHOLD:
            await self.snapshot_and_compact()

    async def snapshot_and_compact(self) -> Optional[int]:
        """
        Master: снимает снапшот на текущий максимальный seq и обрезает WAL до
        него включительно. Возвращает seq снапшота (или None, если WAL пуст).

        Безопасность: отставший пир, чей acked оказался ниже обрезанного WAL,
        восстановится из снапшота (см. _recover_from_snapshot) и догонит хвост.
        """
        async with self.db.execute("SELECT MAX(seq) FROM wal") as cursor:
            row = await cursor.fetchone()
        max_seq = row[0] if row else None
        if not max_seq:
            return None

        await self._snapshots.create(max_seq)
        await self.db.execute("DELETE FROM wal WHERE seq <= ?", (max_seq,))
        await self.db.commit()
        logging.info(f"[WAL] Компакция: WAL обрезан до seq={max_seq}")
        return max_seq

    async def _recover_from_snapshot(self, master_url: str) -> bool:
        """
        Тянет снапшот у master и восстанавливает из него состояние, сдвигая
        last_applied_seq к seq снапшота. Хвост WAL догоняется отдельно.
        """
        try:
            url = f"{master_url}/cluster/replication/snapshot"
            async with self._session.get(url, raise_for_status=True) as response:
                snapshot = await response.json()
        except Exception as e:
            logging.error(f"[WAL] Не удалось получить снапшот: {e}")
            return False

        if not snapshot or snapshot.get("seq") is None:
            logging.warning("[WAL] У master нет снапшота для восстановления")
            return False

        await self._snapshots.restore(snapshot)
        self._last_applied_seq = int(snapshot["seq"])
        if self._last_applied_seq > self._last_master_seq:
            self._last_master_seq = self._last_applied_seq
        await self.db.execute(
            "UPDATE cluster_meta SET value = ? WHERE key = 'last_applied_seq'",
            (str(self._last_applied_seq),),
        )
        await self.db.commit()
        logging.info(f"[WAL] Состояние восстановлено из снапшота до seq={self._last_applied_seq}")
        return True
    
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

        # Отправка slave: шлём каждому пиру всё после его подтверждённого seq
        await self._replicate_pending()

        return seq

    def _peer_id(self, peer: Dict) -> str:
        return peer.get("server_id", f"{peer['host']}:{peer['port']}")

    async def _replicate_pending(self):
        """Досылает каждому пиру WAL-записи после его подтверждённого seq."""
        tasks = []
        for peer in self.peers:
            if self._peer_id(peer) != self.server_id:
                tasks.append(self._send_pending_to_peer(peer))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_pending_to_peer(self, peer: Dict):
        """
        Шлёт пиру все записи после подтверждённого им seq и по ACK обновляет
        отметку подтверждения. Пусто — ничего не делаем.
        """
        peer_id = self._peer_id(peer)
        acked = self._peer_acked.get(peer_id, 0)

        entries = await self.get_wal_entries(acked)
        if not entries:
            return

        payload = {
            "type": "WAL_APPEND",
            "term": self._current_term(),  # для fencing на стороне реплики
            "entries": [e.to_dict() for e in entries],
        }
        result = await self._send_wal_to_peer(peer, payload)
        if result is not None:
            # ACK: пир сообщает свой last_applied_seq — сдвигаем отметку
            peer_seq = int(result.get("last_applied_seq", acked))
            if peer_seq > acked:
                self._peer_acked[peer_id] = peer_seq
                logging.debug(f"[WAL] {peer_id} подтвердил seq={peer_seq}")

    async def _send_wal_to_peer(self, peer: Dict, entry: Dict):
        """Низкоуровневая отправка WAL-пейлоада пиру через circuit breaker."""
        peer_id = self._peer_id(peer)
        breaker = self._breaker(peer_id)

        # Цепь разомкнута — не тратим таймаут на заведомо мёртвого пира.
        if not breaker.allow():
            logging.debug(f"[WAL] Пропуск отправки {peer_id}: circuit {breaker.state}")
            return None

        try:
            url = f"http://{peer['host']}:{peer['port']}/cluster/replication/wal"
            async with self._session.post(
                url,
                json=entry,
                raise_for_status=True
            ) as response:
                result = await response.json()
                logging.debug(f"[WAL] Отправлено {peer_id}: seq={entry['entries'][0]['seq']}")
                breaker.record_success()
                return result
        except Exception as e:
            breaker.record_failure()
            logging.error(f"[WAL] Ошибка отправки {peer_id}: {e}")
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
        """
        Применение WAL записи (для slave) строго по порядку seq.

        Порядок важен: сообщения ссылаются на комнаты/пользователей (внешние
        ключи), поэтому применяем только следующую по счёту запись. Разрыв
        (пропущен seq) означает потерянную запись — не двигаем указатель, а
        догоняем через /replication/sync, чтобы ничего не потерять (#12).
        """
        try:
            wal_entry = WALEntry.from_dict(entry)

            # Отслеживаем максимальный seq мастера (для метрики lag)
            if wal_entry.seq > self._last_master_seq:
                self._last_master_seq = wal_entry.seq

            # Уже применено (дубль/старое) — пропускаем
            if wal_entry.seq <= self._last_applied_seq:
                logging.debug(f"[WAL] Пропущена запись seq={wal_entry.seq} (уже применено)")
                return True

            # Разрыв: пришло не следующее по счёту — догоняем и не теряем запись
            if wal_entry.seq != self._last_applied_seq + 1:
                logging.warning(
                    f"[WAL] Разрыв: получено seq={wal_entry.seq}, "
                    f"ожидалось {self._last_applied_seq + 1} — запуск догона"
                )
                await self._sync_if_possible()
                # Если догон закрыл разрыв — запись уже применена
                return wal_entry.seq <= self._last_applied_seq

            # Контиг: применяем следующую запись
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

    async def _sync_if_possible(self):
        """Догон недостающих WAL-записей у master (best-effort, без гонок)."""
        if self._syncing or not self._master_locator:
            return
        master = self._master_locator()
        if not master:
            logging.debug("[WAL] Догон отложен: master неизвестен")
            return
        self._syncing = True
        try:
            await self.request_sync(f"http://{master['host']}:{master['port']}")
        finally:
            self._syncing = False

    async def sync_from_master(self):
        """Публичный запуск догона (например, при становлении slave)."""
        await self._sync_if_possible()
    
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
                "INSERT OR REPLACE INTO messages "
                "(msg_id, room, nick, text, ts, client_msg_id) VALUES (?, ?, ?, ?, ?, ?)",
                (data["msg_id"], data["room"], data["nick"], data["text"],
                 data["ts"], data.get("client_msg_id"))
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
            # Либо конкретный участник (leave), либо все участники комнаты (delete_room)
            if "nick" in data:
                await self.db.execute(
                    "DELETE FROM room_members WHERE room = ? AND nick = ?",
                    (data["room"], data["nick"])
                )
            else:
                await self.db.execute(
                    "DELETE FROM room_members WHERE room = ?", (data["room"],)
                )
        elif table == "messages":
            # Либо одно сообщение, либо все сообщения комнаты (delete_room)
            if "msg_id" in data:
                await self.db.execute(
                    "DELETE FROM messages WHERE msg_id = ?", (data["msg_id"],)
                )
            else:
                await self.db.execute(
                    "DELETE FROM messages WHERE room = ?", (data["room"],)
                )
        
        await self.db.commit()
    
    async def _apply_update(self, entry: WALEntry):
        """Применение UPDATE операции."""
        # Заглушка для будущих UPDATE операций
        pass
    
    async def request_sync(self, master_url: str) -> bool:
        """
        Догон WAL у master. Если журнал мастера уже обрезан ниже нужного нам
        места (min_seq > last_applied+1), сперва восстанавливаемся из снапшота,
        затем повторно тянем хвост журнала.
        """
        try:
            url = f"{master_url}/cluster/replication/sync?after_seq={self._last_applied_seq}"
            async with self._session.get(url, raise_for_status=True) as response:
                data = await response.json()

            min_seq = data.get("min_seq")  # самый ранний seq, ещё хранящийся в WAL мастера

            # WAL не покрывает нашу позицию — нужен снапшот.
            if min_seq is not None and min_seq > self._last_applied_seq + 1:
                logging.warning(
                    f"[WAL] WAL мастера обрезан (min_seq={min_seq} > "
                    f"{self._last_applied_seq + 1}) — восстановление из снапшота"
                )
                if await self._recover_from_snapshot(master_url):
                    # После снапшота один раз добираем хвост WAL.
                    return await self.request_sync(master_url)
                return False

            entries = data.get("entries", [])
            logging.info(f"[WAL] Синхронизация: получено {len(entries)} записей")
            for entry in entries:
                await self.apply_wal_entry(entry)

            return True
        except Exception as e:
            logging.error(f"[WAL] Ошибка синхронизации: {e}")
            return False
    
    async def get_min_wal_seq(self) -> Optional[int]:
        """Самый ранний seq, ещё хранящийся в WAL (None, если журнал пуст)."""
        async with self.db.execute("SELECT MIN(seq) FROM wal") as cursor:
            row = await cursor.fetchone()
        return row[0] if row and row[0] is not None else None

    async def load_snapshot(self) -> Optional[Dict]:
        """Последний снапшот для отдачи отстающему узлу (или None)."""
        return await self._snapshots.load_latest()

    def get_lag(self) -> int:
        """Отставание репликации: сколько записей master ещё не применено."""
        return max(0, self._last_master_seq - self._last_applied_seq)
    
    def set_master(self, is_master: bool):
        """Установка режима master/slave."""
        self.is_master = is_master
        logging.info(f"[WAL] Режим изменён: master={is_master}")
