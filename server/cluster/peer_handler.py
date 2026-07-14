"""
Peer handler - HTTP handlers для общения между серверами кластера.
"""

import json
import logging
from aiohttp import web

from cluster.election import BullyElection
from cluster.replication import WALReplication
from cluster.heartbeat import HeartbeatManager


def setup_cluster_routes(app: web.Application):
    """Настройка маршрутов для кластера."""
    
    # Health check с информацией о кластере
    app.router.add_get("/cluster/health", handle_cluster_health)
    
    # Election endpoints
    app.router.add_post("/cluster/election/start", handle_election_start)
    app.router.add_post("/cluster/election/coordinator", handle_coordinator)
    
    # Replication endpoints
    app.router.add_post("/cluster/replication/wal", handle_wal_replication)
    app.router.add_get("/cluster/replication/sync", handle_wal_sync)
    app.router.add_get("/cluster/replication/snapshot", handle_snapshot)
    
    # Cluster state
    app.router.add_get("/cluster/state", handle_cluster_state)


async def handle_cluster_health(request: web.Request) -> web.Response:
    """Health check с информацией о кластере."""
    cluster = request.app.get("cluster")
    
    if not cluster:
        return web.json_response({
            "status": "ok",
            "role": "master",
            "term": 0,
            "uptime": 0
        })
    
    election_state = cluster.election.get_state() if cluster.election else {}
    
    return web.json_response({
        "status": "ok",
        "server_id": cluster.server_id,
        "role": election_state.get("role", "slave"),
        "term": election_state.get("term", 0),
        "master_id": election_state.get("master_id"),
        "uptime": int(cluster.uptime) if hasattr(cluster, 'uptime') else 0
    })


async def handle_election_start(request: web.Request) -> web.Response:
    """Обработка начала выборов — делегируется в BullyElection."""
    cluster = request.app.get("cluster")

    if not cluster or not cluster.election:
        return web.json_response({"ok": False}, status=503)

    # Вся логика Bully (обработка term, ответ OK, запуск своих выборов)
    # живёт в одном месте — BullyElection, чтобы не расходиться.
    return await cluster.election.handle_election_start(request)


async def handle_coordinator(request: web.Request) -> web.Response:
    """Обработка сообщения о новом master — делегируется в BullyElection."""
    cluster = request.app.get("cluster")

    if not cluster or not cluster.election:
        return web.json_response({"received": False}, status=503)

    return await cluster.election.handle_coordinator(request)


async def handle_wal_replication(request: web.Request) -> web.Response:
    """Получение WAL записей от master."""
    cluster = request.app.get("cluster")
    
    if not cluster or not cluster.replication:
        return web.json_response({"ack": False}, status=503)
    
    data = await request.json()
    entries = data.get("entries", [])
    term = data.get("term", 0)

    # Fencing (issue #11, Этап 4.1): отвергаем WAL от master со старым term —
    # это «зомби»-master, переживший сетевое разделение. 409 Conflict.
    if not cluster.replication.should_accept_term(term):
        logging.warning(
            f"[Cluster] WAL отклонён fencing'ом: term={term} ниже виденного"
        )
        return web.json_response(
            {"ack": False, "reason": "fenced (stale term)",
             "last_applied_seq": cluster.replication.last_applied_seq},
            status=409,
        )

    logging.debug(f"[Cluster] Получено WAL записей: {len(entries)} (term={term})")

    acked = []
    for entry in entries:
        success = await cluster.replication.apply_wal_entry(entry)
        if success:
            acked.append(entry.get("seq"))

    # last_applied_seq — точка подтверждения для ACK-репликации (Этап 3.3):
    # master по ней понимает, до какого seq реплика дошла, и что дослать.
    return web.json_response({
        "ack": True,
        "acked_seqs": acked,
        "last_applied_seq": cluster.replication.last_applied_seq,
    })


async def handle_wal_sync(request: web.Request) -> web.Response:
    """Синхронизация WAL для отстающих slave."""
    cluster = request.app.get("cluster")
    
    if not cluster or not cluster.replication:
        return web.json_response({"entries": []}, status=503)
    
    if not cluster.replication.is_master:
        return web.json_response(
            {"error": "Not master"},
            status=403
        )
    
    after_seq = int(request.query.get("after_seq", 0))

    entries = await cluster.replication.get_wal_entries(after_seq)
    min_seq = await cluster.replication.get_min_wal_seq()

    return web.json_response({
        "entries": [e.to_dict() for e in entries],
        "count": len(entries),
        # min_seq позволяет slave понять, что WAL обрезан ниже его позиции и
        # нужно восстановиться из снапшота (Этап 3.5).
        "min_seq": min_seq,
    })


async def handle_snapshot(request: web.Request) -> web.Response:
    """Отдаёт последний снапшот состояния отстающему узлу (Этап 3.5)."""
    cluster = request.app.get("cluster")

    if not cluster or not cluster.replication:
        return web.json_response({"error": "No replication"}, status=503)

    if not cluster.replication.is_master:
        return web.json_response({"error": "Not master"}, status=403)

    snapshot = await cluster.replication.load_snapshot()
    if not snapshot:
        return web.json_response({"seq": None, "tables": {}})

    return web.json_response(snapshot)


async def handle_cluster_state(request: web.Request) -> web.Response:
    """Получение состояния кластера."""
    cluster = request.app.get("cluster")
    
    if not cluster:
        return web.json_response({"error": "Cluster not initialized"}, status=503)
    
    state = {
        "server_id": cluster.server_id,
        "election": cluster.election.get_state() if cluster.election else {},
        "heartbeat": cluster.heartbeat.get_cluster_state() if cluster.heartbeat else {},
        "replication": {
            "is_master": cluster.replication.is_master if cluster.replication else False,
            "lag": cluster.replication.get_lag() if cluster.replication else 0
        } if cluster.replication else {},
        # Кворум (Этап 4.1): видно ли большинство узлов — ключевой признак,
        # может ли узел быть master (анти-split-brain).
        "quorum": cluster.get_quorum_status() if hasattr(cluster, "get_quorum_status") else {},
    }
    
    return web.json_response(state)
