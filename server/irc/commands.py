"""Обработчик IRC-команд."""

import asyncio
import time
import logging
import hashlib
import uuid
from aiohttp import web

from network.ws_manager import WebSocketHandler
from storage.database import Database


class CommandHandler:
    """Обработчик IRC-команд."""

    def __init__(self, db: Database, ws_manager: WebSocketHandler, cluster=None):
        self.db = db
        self.ws_manager = ws_manager
        self.cluster = cluster

    # === Аутентификация ===

    async def handle_register(self, ws: web.WebSocketResponse, data: dict):
        """Регистрация пользователя."""
        nick = data.get("nick", "").strip()
        password = data.get("password", "").strip()

        if not nick:
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "REGISTER",
                "message": "Nick is required"
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
        await self.db.execute(
            "INSERT INTO users (nick, password, created_at) VALUES (?, ?, ?)",
            (nick, password_hash, int(time.time()))
        )
        await self.db.commit()

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
        await self.db.execute(
            "INSERT INTO rooms (name, owner, created_at) VALUES (?, ?, ?)",
            (room_name, nick, int(time.time()))
        )
        await self.db.commit()

        logging.info(f"Комната создана: {room_name} (владелец: {nick})")

        await self.ws_manager.send_to(ws, {
            "event": "OK",
            "cmd": "CREATE_ROOM",
            "room": room_name
        })

        # Уведомляем всех о новой комнате
        await self._broadcast_room_list()

    async def handle_delete_room(self, ws: web.WebSocketResponse, data: dict):
        """Удаление комнаты."""
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

        # Удаление комнаты
        await self.db.execute("DELETE FROM room_members WHERE room = ?", (room_name,))
        await self.db.execute("DELETE FROM messages WHERE room = ?", (room_name,))
        await self.db.execute("DELETE FROM rooms WHERE name = ?", (room_name,))
        await self.db.commit()

        logging.info(f"Комната удалена: {room_name}")

        await self.ws_manager.send_to(ws, {
            "event": "OK",
            "cmd": "DELETE_ROOM",
            "room": room_name
        })

        # Уведомляем всех об удалении комнаты
        await self._broadcast_room_list()

    async def handle_join(self, ws: web.WebSocketResponse, data: dict):
        """Присоединение к комнате."""
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
            await self.db.execute(
                "INSERT INTO room_members (room, nick, joined_at) VALUES (?, ?, ?)",
                (room_name, nick, int(time.time()))
            )
            await self.db.commit()

        # Присоединение к WebSocket комнате
        await self.ws_manager.join_room(ws, room_name)

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
        await self.db.execute(
            "DELETE FROM room_members WHERE room = ? AND nick = ?",
            (room_name, nick)
        )
        await self.db.commit()

        # Покидание WebSocket комнаты
        self.ws_manager.leave_room(ws, room_name)

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
        if not self.ws_manager.is_authenticated(ws):
            await self.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": "MSG",
                "message": "Authentication required"
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

        nick = self.ws_manager.get_nick(ws)

        # Генерация ID сообщения
        msg_id = str(uuid.uuid4())
        ts = int(time.time())

        # Сохранение в БД
        await self.db.execute(
            "INSERT INTO messages (msg_id, room, nick, text, ts) VALUES (?, ?, ?, ?, ?)",
            (msg_id, room_name, nick, text, ts)
        )
        await self.db.commit()

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
