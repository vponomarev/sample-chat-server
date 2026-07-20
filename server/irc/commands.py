"""Обработчик IRC-команд."""

import asyncio
import time
import logging
import hashlib
import uuid
from collections import deque
from aiohttp import web

from network.ws_manager import ConnectionRegistry
from storage.database import Database

# Ограничения ввода (защита от переполнения БД и мусорных данных)
MAX_MESSAGE_LEN = 4000
MAX_NICK_LEN = 32
MAX_ROOM_LEN = 64

# Rate limit сообщений: не более N сообщений за окно W секунд на одно подключение.
# Значения конфигурируемы (env/yaml) через config — см. config.RATE_LIMIT_*.
from config import RATE_LIMIT_MSGS, RATE_LIMIT_WINDOW_SEC


class CommandHandler:
    """Обработчик IRC-команд.

    Экземпляр создаётся на каждое WebSocket-подключение, поэтому состояние
    rate limit (метки времени сообщений) живёт в пределах одного соединения.
    """

    def __init__(self, db: Database, ws_manager: ConnectionRegistry, cluster=None):
        self.db = db
        self.ws_manager = ws_manager
        self.cluster = cluster
        self._msg_times = deque()  # монотонные метки времени недавних MSG

    # === Кластер: репликация и роль ===

    async def _require_master(self, ws: web.WebSocketResponse, cmd: str) -> bool:
        """
        Модель primary + standby: писать может только master.
        На реплике отклоняем запись и подсказываем клиенту master, чтобы он
        переподключился туда. Вне кластера (standalone) — всегда разрешаем.
        """
        if self.cluster is None or self.cluster.is_master:
            return True

        master = self.cluster.get_master_server()
        await self.ws_manager.send_to(ws, {
            "event": "ERROR",
            "cmd": cmd,
            "message": "Read-only replica",
            "master": master,
        })
        # Актуальный список серверов — клиент уйдёт на master
        await self.ws_manager.send_to(ws, {
            "event": "SERVER_LIST",
            "servers": self.cluster.get_cluster_servers(),
        })
        return False

    async def _replicate(self, operation: str, table: str, data: dict):
        """Запись операции в WAL и рассылка на реплики (только master)."""
        if self.cluster and self.cluster.is_master and self.cluster.replication:
            try:
                await self.cluster.replication.log_operation(operation, table, data)
            except Exception as e:
                logging.error(f"Ошибка репликации {operation} {table}: {e}")

    def _rate_limited(self) -> bool:
        """
        Скользящее окно: True, если превышен лимит сообщений за окно.
        Защита от флуда одним подключением (issue #17).
        """
        now = time.monotonic()
        window_start = now - RATE_LIMIT_WINDOW_SEC
        while self._msg_times and self._msg_times[0] < window_start:
            self._msg_times.popleft()
        if len(self._msg_times) >= RATE_LIMIT_MSGS:
            return True
        self._msg_times.append(now)
        return False

    # === Аутентификация ===

    async def handle_register(self, ws: web.WebSocketResponse, data: dict):
        """Регистрация пользователя."""
        if not await self._require_master(ws, "REGISTER"):
            return

        nick = data.get("nick", "").strip()
        password = data.get("password", "").strip()

        if not nick:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "REGISTER",
                "message": "Nick is required"
            })
            return

        if len(nick) > MAX_NICK_LEN:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "REGISTER",
                "message": f"Nick too long (max {MAX_NICK_LEN} characters)"
            })
            return

        # Пароль обязателен: аккаунт без пароля мог бы занять любой (issue #15)
        if not password:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "REGISTER",
                "message": "Password is required"
            })
            return

        # Проверка существования пользователя
        existing = await self.db.fetchone(
            "SELECT nick FROM users WHERE nick = ?",
            (nick,)
        )

        if existing:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "REGISTER",
                "message": "Nick already exists"
            })
            return

        # Хэширование пароля
        password_hash = None
        if password:
            import bcrypt
            password_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

        # Создание пользователя
        created_at = int(time.time())
        async with self.db.transaction():
            await self.db.execute(
                "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
                (nick, password_hash, created_at)
            )
        await self._replicate("INSERT", "users", {
            "nick": nick, "password": password_hash, "created_at": created_at
        })

        # Установка сессии
        self.ws_manager.set_session(ws, nick, authenticated=True)

        logging.info(f"Пользователь зарегистрирован: {nick}")

        await self.ws_manager.send_to(ws, {
            "event": "OK",
            "cmd": "REGISTER",
            "nick": nick
        })

    async def handle_login(self, ws: web.WebSocketResponse, data: dict):
        """Логин пользователя."""
        nick = data.get("nick", "").strip()
        password = data.get("password", "").strip()

        if not nick:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "LOGIN",
                "message": "Nick is required"
            })
            return

        # Поиск пользователя
        user = await self.db.fetchone(
            "SELECT nick, password FROM users WHERE nick = ?",
            (nick,)
        )

        if not user:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "LOGIN",
                "message": "User not found"
            })
            return

        # Проверка пароля
        if user["password"]:
            import bcrypt
            if not bcrypt.checkpw(password.encode(), user["password"].encode()):
                await self.ws_manager.send_to(ws, {
                    "event": "ERROR",
                    "cmd": "LOGIN",
                    "message": "Invalid password"
                })
                return

        # Установка сессии
        self.ws_manager.set_session(ws, nick, authenticated=True)

        logging.info(f"Пользователь вошёл: {nick}")

        await self.ws_manager.send_to(ws, {
            "event": "OK",
            "cmd": "LOGIN",
            "nick": nick
        })

    # === Комнаты ===

    async def handle_list_rooms(self, ws: web.WebSocketResponse, data: dict):
        """Список комнат."""
        rooms = await self.db.fetchall("SELECT name FROM rooms ORDER BY name")
        room_list = [row["name"] for row in rooms]

        await self.ws_manager.send_to(ws, {
            "event": "ROOM_LIST",
            "rooms": room_list
        })

    async def handle_create_room(self, ws: web.WebSocketResponse, data: dict):
        """Создание комнаты."""
        if not await self._require_master(ws, "CREATE_ROOM"):
            return

        if not self.ws_manager.is_authenticated(ws):
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "CREATE_ROOM",
                "message": "Authentication required"
            })
            return

        room_name = data.get("room", "").strip()
        nick = self.ws_manager.get_nick(ws)

        if not room_name:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "CREATE_ROOM",
                "message": "Room name is required"
            })
            return

        if len(room_name) > MAX_ROOM_LEN:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "CREATE_ROOM",
                "message": f"Room name too long (max {MAX_ROOM_LEN} characters)"
            })
            return

        # Проверка существования комнаты
        existing = await self.db.fetchone(
            "SELECT name FROM rooms WHERE name = ?",
            (room_name,)
        )

        if existing:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "CREATE_ROOM",
                "message": "Room already exists"
            })
            return

        # Создание комнаты
        created_at = int(time.time())
        async with self.db.transaction():
            await self.db.execute(
                "INSERT INTO rooms (name, owner, created_at) VALUES (?, ?, ?)",
                (room_name, nick, created_at)
            )
        await self._replicate("INSERT", "rooms", {
            "name": room_name, "owner": nick, "created_at": created_at
        })

        logging.info(f"Комната создана: {room_name} (владелец: {nick})")

        await self.ws_manager.send_to(ws, {
            "event": "OK",
            "cmd": "CREATE_ROOM",
            "room": room_name
        })

        # Уведомляем всех о новой комнате
        await self._broadcast_room_list()
        await self._update_room_metrics(room_name)

    async def handle_delete_room(self, ws: web.WebSocketResponse, data: dict):
        """Удаление комнаты."""
        if not await self._require_master(ws, "DELETE_ROOM"):
            return

        if not self.ws_manager.is_authenticated(ws):
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "DELETE_ROOM",
                "message": "Authentication required"
            })
            return

        room_name = data.get("room", "").strip()
        nick = self.ws_manager.get_nick(ws)

        if not room_name:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "DELETE_ROOM",
                "message": "Room name is required"
            })
            return

        # Проверка существования и прав
        room = await self.db.fetchone(
            "SELECT name, owner FROM rooms WHERE name = ?",
            (room_name,)
        )

        if not room:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "DELETE_ROOM",
                "message": "Room not found"
            })
            return

        if room["owner"] != nick and room_name != "#general":
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "DELETE_ROOM",
                "message": "Only owner can delete the room"
            })
            return

        if room_name == "#general":
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "DELETE_ROOM",
                "message": "Cannot delete #general room"
            })
            return

        # Удаление комнаты — атомарно: либо всё, либо ничего
        async with self.db.transaction():
            await self.db.execute("DELETE FROM room_members WHERE room = ?", (room_name,))
            await self.db.execute("DELETE FROM messages WHERE room = ?", (room_name,))
            await self.db.execute("DELETE FROM rooms WHERE name = ?", (room_name,))
        # Реплицируем каскад (room-scoped delete на репликах)
        await self._replicate("DELETE", "room_members", {"room": room_name})
        await self._replicate("DELETE", "messages", {"room": room_name})
        await self._replicate("DELETE", "rooms", {"name": room_name})

        logging.info(f"Комната удалена: {room_name}")

        await self.ws_manager.send_to(ws, {
            "event": "OK",
            "cmd": "DELETE_ROOM",
            "room": room_name
        })

        # Уведомляем всех об удалении комнаты
        await self._broadcast_room_list()
        await self._update_room_metrics()

    async def handle_join(self, ws: web.WebSocketResponse, data: dict):
        """Присоединение к комнате."""
        if not await self._require_master(ws, "JOIN"):
            return

        if not self.ws_manager.is_authenticated(ws):
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "JOIN",
                "message": "Authentication required"
            })
            return

        room_name = data.get("room", "").strip()
        nick = self.ws_manager.get_nick(ws)

        if not room_name:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "JOIN",
                "message": "Room name is required"
            })
            return

        # Проверка существования комнаты
        room = await self.db.fetchone(
            "SELECT name FROM rooms WHERE name = ?",
            (room_name,)
        )

        if not room:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "JOIN",
                "message": "Room not found"
            })
            return

        # Проверка членства
        membership = await self.db.fetchone(
            "SELECT room FROM room_members WHERE room = ? AND nick = ?",
            (room_name, nick)
        )

        if not membership:
            # Добавление в комнату
            joined_at = int(time.time())
            async with self.db.transaction():
                await self.db.execute(
                    "INSERT INTO room_members (room, nick, joined_at) VALUES (?, ?, ?)",
                    (room_name, nick, joined_at)
                )
            await self._replicate("INSERT", "room_members", {
                "room": room_name, "nick": nick, "joined_at": joined_at
            })

        # Присоединение к WebSocket комнате
        await self.ws_manager.join_room(ws, room_name)
        await self._update_room_metrics(room_name)

        logging.info(f"Пользователь {nick} присоединился к {room_name}")

        # === Отправка истории сообщений (последние 50) ===
        messages = await self.db.fetchall(
            "SELECT msg_id, room, nick, text, ts FROM messages "
            "WHERE room = ? ORDER BY ts DESC LIMIT 50",
            (room_name,)
        )
        
        # Отправляем сообщения в обратном порядке (старые → новые)
        for msg in reversed(messages):
            await self.ws_manager.send_to(ws, {
                "event": "MESSAGE",
                "room": msg["room"],
                "nick": msg["nick"],
                "text": msg["text"],
                "ts": msg["ts"],
                "msg_id": msg["msg_id"],
                "history": True  # Флаг что это история
            })

        # Отправка списка пользователей в комнате
        await self._send_user_list(ws, room_name)

        # Уведомление других участников
        await self.ws_manager.broadcast_to_room(room_name, {
            "event": "JOINED",
            "room": room_name,
            "nick": nick
        }, exclude=ws)

        await self.ws_manager.send_to(ws, {
            "event": "JOINED",
            "room": room_name,
            "nick": nick
        })

    async def handle_leave(self, ws: web.WebSocketResponse, data: dict):
        """Покидание комнаты."""
        if not await self._require_master(ws, "LEAVE"):
            return

        if not self.ws_manager.is_authenticated(ws):
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "LEAVE",
                "message": "Authentication required"
            })
            return

        room_name = data.get("room", "").strip()
        nick = self.ws_manager.get_nick(ws)

        if not room_name:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "LEAVE",
                "message": "Room name is required"
            })
            return

        # Удаление из комнаты в БД
        async with self.db.transaction():
            await self.db.execute(
                "DELETE FROM room_members WHERE room = ? AND nick = ?",
                (room_name, nick)
            )
        await self._replicate("DELETE", "room_members", {
            "room": room_name, "nick": nick
        })

        # Покидание WebSocket комнаты
        self.ws_manager.leave_room(ws, room_name)
        await self._update_room_metrics(room_name)

        logging.info(f"Пользователь {nick} покинул {room_name}")

        # Уведомление других участников
        await self.ws_manager.broadcast_to_room(room_name, {
            "event": "LEFT",
            "room": room_name,
            "nick": nick
        }, exclude=ws)

        await self.ws_manager.send_to(ws, {
            "event": "LEFT",
            "room": room_name,
            "nick": nick
        })

    async def handle_who(self, ws: web.WebSocketResponse, data: dict):
        """Список пользователей в комнате."""
        if not self.ws_manager.is_authenticated(ws):
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "WHO",
                "message": "Authentication required"
            })
            return

        room_name = data.get("room", "").strip()

        if not room_name:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "WHO",
                "message": "Room name is required"
            })
            return

        await self._send_user_list(ws, room_name)

    # === Сообщения ===

    async def handle_msg(self, ws: web.WebSocketResponse, data: dict):
        """Отправка сообщения."""
        if not await self._require_master(ws, "MSG"):
            return

        if not self.ws_manager.is_authenticated(ws):
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "MSG",
                "message": "Authentication required"
            })
            return

        if self._rate_limited():
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "MSG",
                "message": (
                    f"Rate limit exceeded "
                    f"(max {RATE_LIMIT_MSGS} messages per {int(RATE_LIMIT_WINDOW_SEC)}s)"
                )
            })
            return

        room_name = data.get("room", "").strip()
        text = data.get("text", "").strip()
        client_msg_id = data.get("client_msg_id")

        if not room_name:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "MSG",
                "message": "Room name is required"
            })
            return

        if not text:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "MSG",
                "message": "Message text is required"
            })
            return

        if len(text) > MAX_MESSAGE_LEN:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "MSG",
                "message": f"Message too long (max {MAX_MESSAGE_LEN} characters)"
            })
            return

        nick = self.ws_manager.get_nick(ws)

        # Идемпотентность: повтор с тем же client_msg_id не создаёт дубль —
        # возвращаем ранее присвоенный msg_id (например, если ACK потерялся).
        if client_msg_id:
            existing = await self.db.fetchone(
                "SELECT msg_id FROM messages WHERE nick = ? AND client_msg_id = ?",
                (nick, client_msg_id)
            )
            if existing:
                logging.info(
                    f"Повтор сообщения (client_msg_id={client_msg_id}) от {nick} — дедуп"
                )
                await self.ws_manager.send_to(ws, {
                    "event": "ACK",
                    "client_msg_id": client_msg_id,
                    "msg_id": existing["msg_id"],
                    "duplicate": True,
                })
                return

        # Генерация ID сообщения
        msg_id = str(uuid.uuid4())
        ts = int(time.time())

        # Сохранение в БД
        async with self.db.transaction():
            await self.db.execute(
                "INSERT INTO messages (msg_id, room, nick, text, ts, client_msg_id) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (msg_id, room_name, nick, text, ts, client_msg_id)
            )
        await self._replicate("INSERT", "messages", {
            "msg_id": msg_id, "room": room_name, "nick": nick,
            "text": text, "ts": ts, "client_msg_id": client_msg_id
        })

        # Метрика
        from observability.metrics import increment_messages
        increment_messages(room_name)

        logging.info(f"Сообщение от {nick} в {room_name}: {text[:50]}...")

        # Рассылка сообщения всем в комнате
        message_data = {
            "event": "MESSAGE",
            "room": room_name,
            "nick": nick,
            "text": text,
            "ts": ts,
            "msg_id": msg_id
        }

        await self.ws_manager.broadcast_to_room(room_name, message_data)

        # Подтверждение клиенту
        if client_msg_id:
            await self.ws_manager.send_to(ws, {
                "event": "ACK",
                "client_msg_id": client_msg_id,
                "msg_id": msg_id
            })

    # === Вспомогательные методы ===

    async def _send_user_list(self, ws: web.WebSocketResponse, room_name: str):
        """Отправка списка пользователей в комнате."""
        members = await self.db.fetchall(
            "SELECT nick FROM room_members WHERE room = ? ORDER BY nick",
            (room_name,)
        )
        users = [row["nick"] for row in members]

        # Добавляем пользователей из WebSocket сессий
        for conn in self.ws_manager.room_connections.get(room_name, set()):
            nick = self.ws_manager.get_nick(conn)
            if nick and nick not in users:
                users.append(nick)

        await self.ws_manager.send_to(ws, {
            "event": "USER_LIST",
            "room": room_name,
            "users": sorted(users)
        })

    async def _broadcast_room_list(self):
        """Рассылка обновлённого списка комнат всем подключённым."""
        rooms = await self.db.fetchall("SELECT name FROM rooms ORDER BY name")
        room_list = [row["name"] for row in rooms]

        for ws in self.ws_manager.connections:
            await self.ws_manager.send_to(ws, {
                "event": "ROOM_LIST",
                "rooms": room_list
            })

    async def _update_room_metrics(self, room_name: str | None = None):
        """Обновление метрик по комнатам (всего/активных/участников)."""
        from observability.metrics import (
            update_rooms_total,
            update_rooms_active,
            update_room_members,
        )

        # Всего комнат в БД
        rooms = await self.db.fetchall("SELECT name FROM rooms")
        update_rooms_total(len(rooms))

        # Активные комнаты — те, где есть хотя бы одно подключение
        active = sum(
            1 for conns in self.ws_manager.room_connections.values() if conns
        )
        update_rooms_active(active)

        # Число участников конкретной комнаты (по WebSocket-подключениям)
        if room_name:
            update_room_members(
                room_name, self.ws_manager.get_room_members_count(room_name)
            )
