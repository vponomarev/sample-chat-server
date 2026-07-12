"""Менеджер WebSocket подключений."""

import logging
from typing import Set, Dict
from aiohttp import web


class ConnectionRegistry:
    """Управление WebSocket подключениями."""

    def __init__(self):
        # Все активные подключения
        self.connections: Set[web.WebSocketResponse] = set()
        # Подключения по комнатам: room -> set of connections
        self.room_connections: Dict[str, Set[web.WebSocketResponse]] = {}
        # Сессии пользователей: connection -> {nick, authenticated}
        self.sessions: Dict[web.WebSocketResponse, dict] = {}

    async def add_connection(self, ws: web.WebSocketResponse):
        """Добавление нового подключения."""
        self.connections.add(ws)
        self.sessions[ws] = {"nick": None, "authenticated": False}
        logging.info(f"Новое подключение. Всего подключений: {len(self.connections)}")

    def remove_connection(self, ws: web.WebSocketResponse):
        """Удаление подключения."""
        # Получаем ник для логирования
        nick = self.get_nick(ws)
        
        self.connections.discard(ws)
        if ws in self.sessions:
            del self.sessions[ws]
        # Удаляем из всех комнат
        for room_conns in self.room_connections.values():
            room_conns.discard(ws)
        
        nick_str = f" ({nick})" if nick else ""
        logging.info(f"Подключение закрыто{nick_str}. Осталось подключений: {len(self.connections)}")

    def set_session(self, ws: web.WebSocketResponse, nick: str, authenticated: bool = True):
        """Установка сессии пользователя."""
        self.sessions[ws] = {"nick": nick, "authenticated": authenticated}
        logging.info(f"Пользователь {nick} аутентифицирован")

    def get_session(self, ws: web.WebSocketResponse) -> dict:
        """Получение сессии пользователя."""
        return self.sessions.get(ws, {"nick": None, "authenticated": False})

    def is_authenticated(self, ws: web.WebSocketResponse) -> bool:
        """Проверка аутентификации."""
        return self.sessions.get(ws, {}).get("authenticated", False)

    def get_nick(self, ws: web.WebSocketResponse) -> str | None:
        """Получение ника пользователя."""
        return self.sessions.get(ws, {}).get("nick")

    async def join_room(self, ws: web.WebSocketResponse, room: str):
        """Присоединение к комнате."""
        if room not in self.room_connections:
            self.room_connections[room] = set()
        self.room_connections[room].add(ws)
        nick = self.get_nick(ws)
        logging.info(f"Пользователь {nick} присоединился к {room}. В комнате: {len(self.room_connections[room])}")

    def leave_room(self, ws: web.WebSocketResponse, room: str):
        """Покидание комнаты."""
        if room in self.room_connections:
            self.room_connections[room].discard(ws)
            nick = self.get_nick(ws)
            logging.info(f"Пользователь {nick} покинул {room}. В комнате: {len(self.room_connections[room])}")

    async def broadcast_to_room(self, room: str, message: dict, exclude: web.WebSocketResponse | None = None):
        """Рассылка сообщения всем в комнате."""
        if room not in self.room_connections:
            return

        import json
        data = json.dumps(message)

        dead_connections = []
        for ws in self.room_connections[room]:
            if ws == exclude:
                continue
            try:
                await ws.send_str(data)
            except Exception:
                dead_connections.append(ws)

        # Удаляем мёртвые подключения
        for ws in dead_connections:
            self.remove_connection(ws)

    async def send_to(self, ws: web.WebSocketResponse, message: dict):
        """Отправка сообщения конкретному подключению."""
        import json
        try:
            await ws.send_str(json.dumps(message))
        except Exception:
            self.remove_connection(ws)

    def get_connected_count(self) -> int:
        """Количество подключений."""
        return len(self.connections)

    def get_room_members_count(self, room: str) -> int:
        """Количество участников в комнате."""
        return len(self.room_connections.get(room, set()))
