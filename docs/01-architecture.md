# Архитектура проекта

## Общая схема

```
┌──────────────────────────┐         ┌───────────────────────────────────────┐
│  Браузерный клиент (PWA)  │  WS/HTTP │              Сервер (aiohttp)           │
│                          │◄────────►│                                         │
│  app.js    — UI/логика   │          │  network/ws_handler.py  — приём команд  │
│  connection.js — reconnect│         │  irc/commands.py        — бизнес-логика │
│  storage.js — IndexedDB  │          │  network/ws_manager.py  — рассылка      │
│  sw.js      — offline     │         │  storage/database.py    — SQLite (WAL)  │
└──────────────────────────┘          │  cluster/*              — heartbeat,    │
                                       │                           election,     │
                                       │                           replication   │
                                       │  observability/*        — логи, метрики │
                                       └───────────────────────────────────────┘
                                                    ▲          ▲
                                       heartbeat/   │          │  /metrics
                                       election/WAL  │          │
                                       ┌────────────┴───┐  ┌───┴──────────────┐
                                       │ server2, server3 │  │ Prometheus+Grafana│
                                       └──────────────────┘  └──────────────────┘
```

## Технологии

- **Сервер:** Python 3.14, `aiohttp` (HTTP + WebSocket), `aiosqlite`, `bcrypt`,
  `prometheus_client`.
- **Хранилище:** один файл SQLite в режиме WAL (`PRAGMA journal_mode=WAL`).
- **Клиент:** чистый HTML/CSS/JS без фреймворков, PWA (manifest + service worker),
  IndexedDB для офлайн-хранения.
- **Инфраструктура:** Docker Compose (3 сервера + Prometheus + Grafana), алерты Prometheus.

## Ключевые компоненты сервера

| Модуль | Назначение | Заметки |
|--------|-----------|---------|
| `server/main.py` | Точка входа, `on_startup`/`on_shutdown` | Поднимает БД, ws-менеджер, при `CLUSTER_ENABLED` — кластер |
| `server/config.py` | Конфигурация из переменных окружения | `config/config.yaml` **не читается** (мёртвый файл) |
| `server/storage/database.py` | Обёртка над SQLite | Таблицы `users, rooms, room_members, messages, wal, cluster_meta` |
| `server/irc/commands.py` | Обработчики команд (REGISTER, LOGIN, JOIN, MSG…) | Пишет в БД **напрямую**, WAL не задействует |
| `server/network/ws_handler.py` | Цикл чтения WebSocket, диспетчеризация по `handle_<cmd>` | Рабочий файл |
| `server/network/websocket_handler.py` | Почти дубликат `ws_handler.py` | **Мёртвый и сломанный**, нигде не импортируется |
| `server/network/ws_manager.py` | Реестр подключений, комнат, сессий; рассылка | Класс называется `WebSocketHandler` (путает с модулем-обработчиком) |
| `server/cluster/heartbeat.py` | Опрос соседей, детект «живой/мёртвый» | Опрашивает не тот endpoint (см. issues) |
| `server/cluster/election.py` | Bully-алгоритм выбора лидера | Регистрирует свои роуты `/election/*` |
| `server/cluster/replication.py` | WAL-репликация master→slave | Полностью не подключена к записи |
| `server/cluster/peer_handler.py` | HTTP-роуты `/cluster/*` для межсерверного обмена | Часть роутов дублирует `election.py` |
| `server/cluster/manager.py` | Связывает heartbeat + election + replication | |
| `server/observability/metrics.py` | Prometheus-метрики | Многие метрики объявлены, но не обновляются |
| `server/observability/logger.py` | Настройка логирования | |

## Поток «отправка сообщения» (как есть сейчас)

1. Клиент: `app._sendMessage()` → оптимистично рисует «pending» → `connection.sendWithRetry()`
   с `client_msg_id`. Если сокет закрыт — кладёт в retry-очередь (в памяти + IndexedDB).
2. Сервер: `ws_handler.handle_message()` → `commands.handle_msg()`:
   - генерирует серверный `msg_id = uuid4()`, `INSERT INTO messages`, `commit`;
   - `broadcast_to_room()` — рассылка **только локальным** подключениям этой комнаты;
   - отправляет `ACK { client_msg_id, msg_id }` автору.
3. Клиент: по `ACK` помечает сообщение как «sent».

**Пробелы в этом потоке** (детали — в [02-issues.md](02-issues.md)):
- Нет межсерверной доставки → адресаты на других узлах сообщение не получат.
- Запись не попадает в таблицу `wal` → на реплики ничего не уедет даже теоретически.
- Нет проверки `client_msg_id` на сервере → повторная отправка создаёт дубль в БД.

## Поток «кластер и отказ мастера» (как задумано vs как есть)

| Шаг | Задумано | Фактически |
|-----|----------|-----------|
| Обнаружение живости | heartbeat читает роль соседа с `/cluster/health` | читает `/health` → роль всегда `slave` |
| Падение мастера | `_on_peer_down` видит `role == "master"` → старт выборов | условие никогда не истинно → выборы не стартуют |
| Выборы | Bully по числовому ID | стартуют только «по цепочке» от входящего ELECTION; ID соседей вычисляются неверно |
| Репликация | master пишет в WAL и рассылает slave | `log_operation()` не вызывается → WAL пуст |

## Клиентская часть

- `connection.js` — реальный, содержательный код: экспоненциальный backoff,
  ротация серверов при разрыве, retry-очередь с персистентностью в IndexedDB.
- `storage.js` — IndexedDB: сессия, настройки сервера, список серверов, очередь.
- `sw.js` — service worker для офлайн-режима (PWA).
- Слабое место: после переподключения клиент **не переотправляет LOGIN** (см. issues #6).
