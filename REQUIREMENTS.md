# Требования к разработке приложения
В качестве финальной работы я решил сделать аналог IRC чата - серверное приложение на python, клиенты в виде web странички.
Меня попросили написать пилот, который позволит заранее понять суть и возможности.
Подскажи, можно ли web страничку сделать в варианте, позволяющем упаковать решение в приложение для android?

Требования к приложению - оно должно покрывать темы, которые мы обсуждали.
Вот неполный список для сервера - мониторинг и логи, отказоустойчивость и выбор мастера между серверами, хранение в локальном sqlite.
Вот требования к клиенту - он должен подключаться к известному серверу, получать список серверов и дальше работать с ними. Список может меняться, мастера могут сообщать кто сейчас master, а кто slave. Вся работа с активным мастером, мастера друг с другом синхронизируются.
Клиент должен иметь локальное хранение чатов, при отправке сообщения и недоступности сервера - положить его в очередь и отмечать “не отправлено”, делать ретрай с увеличивающимися периодами.

Работа будет как в IRC, а именно:
    Команда регистрации, вводим свой ник и нас узнают по нику, установка пароля опциональна 
    Команда логина при запуске, указываем свой ник и пароль, если пароль неверен или ника нет - получаем ошибку 
    Отдельные чат-румы, возможность создавать собственные чат румы и удалять их 

Приложение должно быть по возможности архитектурно простое и близкое к IRC.
Пропиши архитектуру, предлагаемое решение и шаги для получения результата.

# Предлагаемая архитектура
## Общая схема
┌─────────────────────────────────────────────────────────────┐
│                        КЛИЕНТЫ                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                   │
│  │ PWA/Web  │  │ PWA/Web  │  │ PWA/Web  │                   │
│  │ Client 1 │  │ Client 2 │  │ Client 3 │                   │
│  │          │  │          │  │          │                    │
│  │ IndexedDB│  │ IndexedDB│  │ IndexedDB│  ← локальный кэш  │
│  │ SW Queue │  │ SW Queue │  │ SW Queue │  ← офлайн-очередь │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘                   │
│       │              │              │                         │
│       └──────────────┼──────────────┘                        │
│                      │ WebSocket                             │
└──────────────────────┼───────────────────────────────────────┘
│
┌──────────────────────┼───────────────────────────────────────┐
│                 СЕРВЕРНЫЙ КЛАСТЕР                             │
│                      │                                       │
│           ┌──────────▼──────────┐                            │
│           │   Server 1 (MASTER) │                            │
│           │   Python + aiohttp  │                            │
│           │   SQLite            │◄────── Prometheus metrics  │
│           │   WAL replication ──┼──┐                         │
│           └─────────────────────┘  │                         │
│                                    │  replication            │
│           ┌────────────────────┐   │                         │
│           │  Server 2 (SLAVE)  │◄──┘                         │
│           │  Python + aiohttp  │                             │
│           │  SQLite            │◄────── Prometheus metrics   │
│           └────────────────────┘                             │
│                                                              │
│           ┌────────────────────┐                             │
│           │  Server 3 (SLAVE)  │◄── replication              │
│           │  Python + aiohttp  │                             │
│           │  SQLite            │◄────── Prometheus metrics   │
│           └────────────────────┘                             │
│                                                              │
│           Серверы общаются между собой:                       │
│           - heartbeat (кто жив)                              │
│           - leader election (кто master)                     │
│           - WAL replication (синхронизация данных)           │
└──────────────────────────────────────────────────────────────┘



## Серверная архитектура
server/
├── main.py                  # точка входа, запуск сервера
├── config.py                # конфигурация (порты, адреса пиров, таймауты)
│
├── network/
│   ├── websocket_handler.py # WebSocket: подключения клиентов, IRC-команды
│   ├── peer_handler.py      # HTTP/WebSocket: общение между серверами
│   └── routes.py            # HTTP: health check, metrics endpoint, server list
│
├── irc/
│   ├── commands.py          # парсинг и выполнение IRC-команд
│   ├── rooms.py             # управление комнатами (create, join, leave, delete)
│   └── users.py             # регистрация, логин, сессии
│
├── storage/
│   ├── database.py          # SQLite: таблицы, запросы
│   └── wal.py               # Write-Ahead Log: журнал операций для репликации
│
├── cluster/
│   ├── heartbeat.py         # периодический пинг между серверами
│   ├── election.py          # выбор master (упрощённый Raft/Bully)
│   └── replication.py       # отправка/приём WAL-записей между серверами
│
├── observability/
│   ├── logger.py            # structured logging (JSON)
│   ├── metrics.py           # Prometheus-метрики (counters, gauges, histograms)
│   └── health.py            # health check endpoint (/health)
│
└── tests/
├── test_commands.py
├── test_election.py
└── test_replication.py

## Клиентская архитектура
client/
├── index.html               # основная страница
├── manifest.json            # PWA manifest (иконка, имя, цвета)
├── sw.js                    # Service Worker: офлайн, очередь сообщений
│
├── css/
│   └── style.css            # стили (простой IRC-вид)
│
├── js/
│   ├── app.js               # точка входа, инициализация
│   ├── connection.js         # WebSocket: подключение, reconnect, server switching
│   ├── commands.js           # парсинг/отправка IRC-команд
│   ├── rooms.js              # UI комнат: список, переключение, создание
│   ├── messages.js           # отображение сообщений, статусы (sent/pending/failed)
│   ├── storage.js            # IndexedDB: локальный кэш сообщений и комнат
│   └── retry-queue.js        # очередь неотправленных сообщений + exponential backoff
│
└── icons/
├── icon-192.png
└── icon-512.png

# Протокол общения
## Клиент <-> Сервер (Websocket, JSON)
// Клиент → Сервер
{ "cmd": "REGISTER", "nick": "vasya", "password": "optional" }
{ "cmd": "LOGIN",    "nick": "vasya", "password": "secret" }
{ "cmd": "JOIN",     "room": "#general" }
{ "cmd": "LEAVE",    "room": "#general" }
{ "cmd": "MSG",      "room": "#general", "text": "привет!", "client_msg_id": "uuid-123" }
{ "cmd": "CREATE_ROOM", "room": "#my-room" }
{ "cmd": "DELETE_ROOM", "room": "#my-room" }
{ "cmd": "LIST_ROOMS" }
{ "cmd": "WHO",      "room": "#general" }

// Сервер → Клиент
{ "event": "OK",         "cmd": "LOGIN",  "nick": "vasya" }
{ "event": "ERROR",      "cmd": "LOGIN",  "message": "неверный пароль" }
{ "event": "MESSAGE",    "room": "#general", "nick": "petya", "text": "привет!", "ts": 1705312200, "msg_id": "server-uuid-456" }
{ "event": "JOINED",     "room": "#general", "nick": "vasya" }
{ "event": "LEFT",       "room": "#general", "nick": "vasya" }
{ "event": "ROOM_LIST",  "rooms": ["#general", "#random", "#my-room"] }
{ "event": "USER_LIST",  "room": "#general", "users": ["vasya", "petya"] }
{ "event": "ACK",        "client_msg_id": "uuid-123", "msg_id": "server-uuid-456" }

// Информация о кластере (при подключении и при смене master)
{ "event": "SERVER_LIST", "servers": [
{ "host": "server1.local", "port": 8080, "role": "master" },
{ "host": "server2.local", "port": 8080, "role": "slave" },
{ "host": "server3.local", "port": 8080, "role": "slave" }
]}

## Сервер <-> Сервер (HTTP/WebSocket, JSON)
// Heartbeat (HTTP GET каждые 2 сек)
GET /health → { "status": "ok", "role": "master", "term": 3, "uptime": 3600 }

// Leader Election (HTTP POST)
POST /vote    { "candidate_id": "server2", "term": 4 }
→ { "vote_granted": true, "term": 4 }

// WAL Replication (WebSocket между серверами)
{ "type": "WAL_APPEND", "entries": [
{ "seq": 101, "ts": 1705312200, "op": "INSERT", "table": "messages",
"data": { "room": "#general", "nick": "vasya", "text": "привет!" } },
{ "seq": 102, "ts": 1705312201, "op": "INSERT", "table": "rooms",
"data": { "name": "#new-room", "owner": "petya" } }
]}
→ { "type": "WAL_ACK", "last_seq": 102 }

# Схема базы данных SQLite
-- Пользователи
CREATE TABLE users (
nick        TEXT PRIMARY KEY,
password    TEXT,              -- bcrypt hash, NULL если без пароля
created_at  INTEGER NOT NULL   -- unix timestamp
);

-- Комнаты
CREATE TABLE rooms (
name        TEXT PRIMARY KEY,  -- "#general", "#random"
owner       TEXT NOT NULL,     -- ник создателя
created_at  INTEGER NOT NULL,
FOREIGN KEY (owner) REFERENCES users(nick)
);

-- Участники комнат (кто в какой комнате)
CREATE TABLE room_members (
room        TEXT NOT NULL,
nick        TEXT NOT NULL,
joined_at   INTEGER NOT NULL,
PRIMARY KEY (room, nick),
FOREIGN KEY (room) REFERENCES rooms(name),
FOREIGN KEY (nick) REFERENCES users(nick)
);

-- Сообщения
CREATE TABLE messages (
msg_id      TEXT PRIMARY KEY,  -- uuid, генерируется сервером
room        TEXT NOT NULL,
nick        TEXT NOT NULL,
text        TEXT NOT NULL,
ts          INTEGER NOT NULL,  -- unix timestamp
FOREIGN KEY (room) REFERENCES rooms(name),
FOREIGN KEY (nick) REFERENCES users(nick)
);

-- WAL журнал (для репликации)
CREATE TABLE wal (
seq         INTEGER PRIMARY KEY AUTOINCREMENT,
ts          INTEGER NOT NULL,
operation   TEXT NOT NULL,     -- "INSERT", "DELETE"
table_name  TEXT NOT NULL,     -- "users", "rooms", "messages", "room_members"
data        TEXT NOT NULL      -- JSON с данными операции
);

-- Метаданные кластера
CREATE TABLE cluster_meta (
key         TEXT PRIMARY KEY,
value       TEXT NOT NULL
);
-- Хранит: current_term, voted_for, last_applied_seq

-- Создаём комнату #general при инициализации
INSERT INTO rooms (name, owner, created_at) VALUES ('#general', 'system', 0);

# Ключевые механизмы
## Выбор мастера (упрощенный Bully)
Почему Bully, а не Raft: проще для пилота и понятнее школьникам.
Каждый сервер имеет числовой ID (из конфига).

1. Каждые 2 сек: отправляем GET /health всем известным серверам
2. Если текущий master не отвечает 3 раза подряд (6 сек):
    - Сервер с наибольшим ID среди живых объявляет себя master
    - Отправляет POST /elect { "new_master": "server3", "term": N+1 } всем
    - Остальные подтверждают (если term выше их текущего)
3. Новый master начинает принимать записи от клиентов
4. Клиентам рассылается обновлённый SERVER_LIST

## Репликация (WAL-based)
1. Каждая операция записи (INSERT/DELETE) на master:
   a) Записывается в таблицу wal с инкрементальным seq
   b) Применяется к основным таблицам
   c) Отправляется по WebSocket всем slave

2. Slave при получении WAL_APPEND:
   a) Проверяет seq — если пропуск, запрашивает недостающие
   b) Записывает в свою таблицу wal
   c) Применяет к основным таблицам
   d) Отвечает WAL_ACK

3. При рестарте slave:
    - Отправляет master свой последний seq
    - Master отдаёт все записи с seq > последний_seq_slave

## Клиентская очередь
// retry-queue.js — упрощённая логика

let queue = [];         // из IndexedDB при старте
let backoff_ms = 500;   // начальная пауза
const MAX_BACKOFF = 30000;

async function enqueue(message) {
message.status = 'pending';
message.attempts = 0;
await saveToIndexedDB(message);
queue.push(message);
processQueue();
}

async function processQueue() {
if (queue.length === 0) return;

    const msg = queue[0];
    try {
        await sendViaWebSocket(msg);
        // сервер ответит ACK → убираем из очереди
        queue.shift();
        await removeFromIndexedDB(msg.client_msg_id);
        backoff_ms = 500; // сброс
        processQueue();   // следующее
    } catch (e) {
        msg.attempts++;
        const jitter = backoff_ms * 0.2 * (Math.random() - 0.5);
        const delay = Math.min(backoff_ms + jitter, MAX_BACKOFF);
        backoff_ms = Math.min(backoff_ms * 2, MAX_BACKOFF);
        setTimeout(processQueue, delay);
    }
}

## Переключение сервера на клиенте
// connection.js — упрощённая логика

let servers = [];        // список от сервера
let currentIndex = 0;
let ws = null;

function connect() {
const server = servers[currentIndex];
ws = new WebSocket(`ws://${server.host}:${server.port}/ws`);

    ws.onopen = () => {
        log(`подключён к ${server.host} (${server.role})`);
        backoff_ms = 500;
    };

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        if (data.event === 'SERVER_LIST') {
            servers = data.servers;
            // если текущий сервер больше не master — переключиться
            const master = servers.find(s => s.role === 'master');
            if (master && (master.host !== server.host || master.port !== server.port)) {
                currentIndex = servers.indexOf(master);
                ws.close();
                connect();
            }
        }
        // ... обработка остальных событий
    };

    ws.onclose = () => {
        // переключение на следующий сервер
        currentIndex = (currentIndex + 1) % servers.length;
        const delay = Math.min(backoff_ms, 30000);
        backoff_ms *= 2;
        setTimeout(connect, delay);
    };
}

# Мониторинг и метрики
## Prometheus endpoint (GET /metrics)
# Подключения
irc_connected_clients{server="server1"} 15
irc_websocket_connections_total{server="server1"} 234

# Сообщения
irc_messages_total{server="server1",room="#general"} 1502
irc_messages_total{server="server1",room="#random"} 347

# Комнаты
irc_rooms_active{server="server1"} 5
irc_room_members{server="server1",room="#general"} 12

# Кластер
irc_server_role{server="server1"} 1          # 1=master, 0=slave
irc_cluster_term{server="server1"} 3
irc_replication_lag_seq{server="server1"} 0   # 0 для master
irc_replication_lag_seq{server="server2"} 5   # slave отстаёт на 5 операций
irc_heartbeat_failures_total{server="server1",peer="server2"} 2

# WAL
irc_wal_size{server="server1"} 15023
irc_wal_last_seq{server="server1"} 4021

# Производительность
irc_command_duration_seconds_bucket{cmd="MSG",le="0.01"} 450
irc_command_duration_seconds_bucket{cmd="MSG",le="0.05"} 498
irc_command_duration_seconds_bucket{cmd="MSG",le="0.1"} 500

## Structured logging
{"ts":"2025-01-15T14:30:05Z","level":"INFO","component":"websocket","event":"client_connected","nick":"vasya","room":"#general","server":"server1"}
{"ts":"2025-01-15T14:30:06Z","level":"INFO","component":"irc","event":"message","nick":"vasya","room":"#general","msg_id":"uuid-456","text_length":12}
{"ts":"2025-01-15T14:30:10Z","level":"WARN","component":"cluster","event":"heartbeat_timeout","peer":"server2","consecutive_failures":3}
{"ts":"2025-01-15T14:30:10Z","level":"INFO","component":"cluster","event":"election_start","term":4,"reason":"master_unreachable"}
{"ts":"2025-01-15T14:30:11Z","level":"INFO","component":"cluster","event":"elected_master","term":4,"new_master":"server1"}
{"ts":"2025-01-15T14:30:11Z","level":"INFO","component":"replication","event":"wal_sync","peer":"server3","from_seq":4015,"to_seq":4021}

# Зависимости
## Сервер python
# requirements.txt
aiohttp>=3.9          # HTTP + WebSocket сервер
aiosqlite>=0.19       # асинхронный SQLite
bcrypt>=4.1           # хэширование паролей
prometheus-client>=0.20  # экспорт метрик
structlog>=24.1       # structured logging
pyyaml>=6.0           # чтение конфига

## Клиент
Чистый HTML/CSS/JS, без фреймворков . Причины:

    Школьникам проще читать и понимать
    Нет этапа сборки (webpack/vite)
    Service Worker и IndexedDB — нативные браузерные API
    Файлы можно открыть прямо в браузере
     
# Шаги реализации
Фаза 1: Минимальный одиночный сервер + клиент (3–4 дня)




1
Цель: один сервер, один клиент, базовые IRC-команды, всё работает.



Шаг 1.1  — Каркас сервера (4 часа)

    main.py: запуск aiohttp, WebSocket endpoint /ws, HTTP endpoint /health
    config.py: YAML-конфиг (порт, имя сервера)
    database.py: создание таблиц SQLite при первом запуске, комната #general
    Проверка: сервер запускается, /health отвечает {"status": "ok"}


Шаг 1.2  — IRC-команды (6 часов)

    commands.py: парсинг JSON → выполнение команды → JSON-ответ
    users.py: REGISTER (сохранить ник + хэш пароля), LOGIN (проверить)
    rooms.py: JOIN, LEAVE, CREATE_ROOM, DELETE_ROOM, LIST_ROOMS, WHO
    websocket_handler.py: при подключении — ждём LOGIN/REGISTER, после — обрабатываем команды
    MSG: сохранить в БД + разослать всем участникам комнаты
    Проверка: через websocat (CLI WebSocket клиент) — регистрация, логин, отправка сообщения


Шаг 1.3  — Web-клиент, базовый (6 часов)

    index.html: поле ввода, область сообщений, список комнат, список пользователей
    connection.js: подключение по WebSocket, отправка/получение JSON
    commands.js: формирование JSON из текста (если начинается с / — команда, иначе — сообщение)
    messages.js: отображение сообщений с ником, временем, текстом
    rooms.js: переключение между комнатами, отображение списка
    Проверка: открыть 2 вкладки, залогиниться, пообщаться в #general


Шаг 1.4  — Логирование и метрики (3 часа)

    logger.py: structlog, JSON-формат, вывод в stdout + файл
    metrics.py: prometheus-client, счётчики сообщений/подключений
    routes.py: endpoint GET /metrics
    Логируем: каждое подключение, каждую команду, каждую ошибку
    Проверка: curl localhost:8080/metrics показывает метрики


Фаза 2: Клиентская надёжность (2–3 дня)




1
Цель: клиент работает при проблемах с сетью, не теряет сообщения.



Шаг 2.1  — Локальное хранилище (4 часа)

    storage.js: обёртка над IndexedDB. Таблицы: messages (room, nick, text, ts, status), rooms, servers
    При получении сообщения — сохранить в IndexedDB
    При открытии комнаты — показать сообщения из IndexedDB (мгновенно), потом обновить с сервера
    Проверка: закрыть вкладку, открыть — история на месте


Шаг 2.2  — Очередь неотправленных (4 часа)

    retry-queue.js: при отправке сообщения — сохранить в IndexedDB со статусом pending
    Отправить через WebSocket, при получении ACK — статус → sent
    Если WebSocket закрыт — сообщение остаётся pending, UI показывает «⏳ не отправлено»
    При восстановлении соединения — отправить все pending сообщения
    Exponential backoff: 500ms → 1s → 2s → 4s → ... → 30s (макс)
    Проверка: отключить сервер, отправить 3 сообщения, включить — все доставлены


Шаг 2.3  — PWA (3 часа)

    manifest.json: имя, иконки, цвета, display: standalone
    sw.js: кэширование HTML/CSS/JS (static assets), перехват fetch для офлайн
    Проверка: на телефоне в Chrome → «Добавить на главный экран» → открывается как приложение


Шаг 2.4  — Переключение серверов (3 часа)

    connection.js: список серверов (начальный — из конфига, обновляется через SERVER_LIST)
    При разрыве соединения: попробовать следующий сервер из списка
    При получении SERVER_LIST с новым master — переподключиться к master
    Проверка: подключены к server1, убиваем его — клиент переключается на server2


Фаза 3: Кластер серверов (3–4 дня)




1
Цель: 3 сервера, один master, два slave, автоматическое переключение.



Шаг 3.1  — Heartbeat (3 часа)

    heartbeat.py: каждые 2 сек — GET /health к каждому пиру
    Ответ включает: роль, term, uptime
    Если пир не ответил 3 раза — считать мёртвым
    Логируем каждый heartbeat miss
    Метрика: irc_heartbeat_failures_total


Шаг 3.2  — Leader election (4 часа)

    election.py: Bully-алгоритм
    Master не отвечает → сервер с наибольшим ID среди живых начинает выборы
    Отправляет POST /elect всем с бóльшим ID
    Если никто не возражает за 3 сек → становится master
    Рассылает SERVER_LIST всем подключённым клиентам
    cluster_meta: хранит текущий term, за кого голосовали
    Проверка: 3 сервера, убиваем master — через ~6 сек новый master выбран


Шаг 3.3  — WAL-репликация (6 часов)

    wal.py: каждая запись в БД → сначала в таблицу wal, потом в основную таблицу
    replication.py (master): при новой WAL-записи → отправить по WebSocket всем slave
    replication.py (slave): получил WAL → применить к своей БД → отправить ACK
    При подключении нового/перезапущенного slave: запрос GET /wal?after_seq=N → получить пропущенные записи
    Метрика: irc_replication_lag_seq
    Проверка: написать сообщение через master, проверить что появилось на slave. Убить slave, написать ещё, поднять slave — догоняет


Шаг 3.4  — Slave принимает только чтение (2 часа)

    Slave принимает WebSocket-подключения клиентов
    Команды чтения (LIST_ROOMS, WHO, GET истории) — обрабатывает сам
    Команды записи (MSG, CREATE_ROOM, REGISTER) — отвечает ERROR: not master, use <master_host>
    Клиент при получении такой ошибки — переключается на master
    Проверка: подключиться к slave, попробовать написать сообщение → клиент автоматически переключается


Фаза 4: Мониторинг и полировка (2 дня)




1
Цель: Grafana-дашборд, полировка UI, итоговое тестирование.



Шаг 4.1  — Docker Compose (3 часа)

    docker-compose.yml:
services:
server1:
build: ./server
environment:
SERVER_ID: 1
PEERS: "server2:8080,server3:8080"
ports: ["8081:8080"]

server2:
build: ./server
environment:
SERVER_ID: 2
PEERS: "server1:8080,server3:8080"
ports: ["8082:8080"]

server3:
build: ./server
environment:
SERVER_ID: 3
PEERS: "server1:8080,server2:8080"
ports: ["8083:8080"]

prometheus:
image: prom/prometheus
volumes: ["./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml"]

grafana:
image: grafana/grafana
volumes: ["./monitoring/dashboards:/var/lib/grafana/dashboards"]
ports: ["3000:3000"]



Шаг 4.2  — Grafana-дашборд (3 часа)

    Панель «Кластер»: роль каждого сервера, текущий term, uptime
    Панель «Сообщения»: RPS по комнатам, общее количество
    Панель «Репликация»: lag каждого slave, WAL size
    Панель «Клиенты»: подключённые, по серверам
    Алёрт: slave lag > 100, master down, 0 connected clients


Шаг 4.3  — Полировка клиента (3 часа)

    Статусбар: «🟢 подключён к server1 (master)» / «🟡 переподключение...» / «🔴 офлайн»
    Статусы сообщений: ✓ отправлено, ⏳ в очереди, ✗ ошибка
    Плавная прокрутка, уведомления о новых сообщениях в неактивных комнатах
    Отображение системных событий: «vasya присоединился», «petya вышел»


Шаг 4.4  — Итоговое тестирование (3 часа)

    Сценарий 1: 3 сервера, 5 клиентов, переписка в 2 комнатах — всё работает
    Сценарий 2: убить master → новый master за ~6 сек → клиенты переключились → сообщения не потеряны
    Сценарий 3: убить slave → master работает → поднять slave → догоняет
    Сценарий 4: клиент офлайн → 3 сообщения в очереди → онлайн → доставлены
    Сценарий 5: убить все серверы → клиент показывает «офлайн», сообщения в очереди → поднять серверы → всё доставлено


Маппинг на темы курса
Тема курса


Где в проекте


SPOF и избыточность

3 сервера, при падении одного — работаем

Active/Standby

Master/Slave серверы

Graceful degradation

Slave → только чтение; клиент офлайн → локальный кэш

Retry + backoff

retry-queue.js — очередь с exponential backoff

Circuit Breaker

connection.js — переключение сервера после N ошибок

Очереди

IndexedDB-очередь неотправленных сообщений

Репликация

WAL-based репликация master → slave

Leader election

Bully-алгоритм выбора master

WAL

Таблица wal, восстановление slave по журналу

Балансировка

Клиент знает список серверов, переключается

Health checks

Heartbeat между серверами, /health endpoint

Мониторинг

Prometheus метрики + Grafana дашборд

Structured logging

JSON-логи с компонентом, событием, контекстом

Хаос-тестирование

Сценарии тестирования: kill серверов, офлайн клиентов




Оценка трудоёмкости
Фаза


Дни


Результат


1: Одиночный сервер + клиент

3–4

Работающий чат, 1 сервер

2: Клиентская надёжность

2–3

Офлайн, очередь, PWA

3: Кластер

3–4

3 сервера, выборы, репликация

4: Мониторинг и полировка

2

Grafana, Docker Compose, тесты

Итого

10–13

Полный пилот





Фаза 1 уже является рабочим демо — можно показывать после первой недели. 


