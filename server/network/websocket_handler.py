"""Обработчик WebSocket подключений."""

import asyncio
import json
import logging
import time
from aiohttp import web

from irc.commands import CommandHandler
from network.websocket_handler import WebSocketHandler


async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    """Обработчик WebSocket подключений."""
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)

    ws_manager: WebSocketHandler = request.app["ws_manager"]
    db = request.app["db"]
    command_handler = CommandHandler(db, ws_manager)

    await ws_manager.add_connection(ws)

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    await handle_message(ws, data, command_handler)
                except json.JSONDecodeError:
                    await ws_manager.send_to(ws, {
                        "event": "ERROR",
                        "message": "Invalid JSON format"
                    })
            elif msg.type == web.WSMsgType.ERROR:
                logging.error(f"WebSocket error: {ws.exception()}")
    finally:
        ws_manager.remove_connection(ws)

    return ws


async def handle_message(
    ws: web.WebSocketResponse,
    data: dict,
    command_handler: CommandHandler
):
    """Обработка входящего сообщения."""
    cmd = data.get("cmd", "").upper()

    if not cmd:
        await ws_manager.send_to(ws, {
            "event": "ERROR",
            "message": "Missing 'cmd' field"
        })
        return

    # Обработка команды
    start_time = time.time()

    try:
        handler = getattr(command_handler, f"handle_{cmd.lower()}", None)
        if handler:
            await handler(ws, data)
        else:
            await command_handler.ws_manager.send_to(ws, {
                "event": "ERROR",
                "cmd": cmd,
                "message": f"Unknown command: {cmd}"
            })
    except Exception as e:
        logging.exception(f"Error handling command {cmd}")
        await command_handler.ws_manager.send_to(ws, {
            "event": "ERROR",
            "cmd": cmd,
            "message": str(e)
        })
    finally:
        # Метрика времени выполнения
        duration = time.time() - start_time
        from observability.metrics import irc_command_duration_seconds
        irc_command_duration_seconds.labels(cmd=cmd).observe(duration)
