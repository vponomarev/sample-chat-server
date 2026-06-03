# Sample Chat Server

IRC-подобный чат с веб-клиентом и кластеризацией серверов.

## Быстрый старт

### Установка зависимостей

```bash
# Создание виртуального окружения
python3 -m venv venv
source venv/bin/activate  # macOS/Linux
# или venv\Scripts\activate  # Windows

# Установка пакетов
pip install -r requirements.txt
```

### Запуск сервера

```bash
source venv/bin/activate
python server/main.py
```

Сервер запустится на `http://localhost:8080`

### Запуск клиента

Откройте `http://localhost:8080` в браузере.

Или через локальный веб-сервер:

```bash
# Вариант 1: Python 3 (в виртуальном окружении)
source venv/bin/activate
python -m http.server 8000 --directory client

# Вариант 1.5: Python 3 (без venv, macOS/Linux)
python3 -m http.server 8000 --directory client

# Вариант 2: Node.js (если установлен)
npx serve client
```

**Важно:** На macOS команда `python` может указывать на Python 2.7. Используйте `python3` или активируйте venv.

## Архитектура

- **Сервер**: Python + aiohttp (WebSocket + HTTP)
- **База данных**: SQLite с WAL-репликацией
- **Клиент**: Чистый HTML/CSS/JS (PWA-ready)

## Структура проекта

```
├── server/
│   ├── main.py              # Точка входа
│   ├── config.py            # Конфигурация
│   ├── network/             # WebSocket, HTTP handlers
│   ├── irc/                 # IRC-команды
│   ├── storage/             # База данных, WAL
│   ├── cluster/             # Кластеризация (Фаза 3)
│   ├── observability/       # Логирование, метрики
│   └── tests/               # Тесты
├── client/
│   ├── index.html           # Основная страница
│   ├── manifest.json        # PWA manifest
│   ├── sw.js                # Service Worker
│   ├── css/                 # Стили
│   ├── js/                  # Клиентская логика
│   └── icons/               # Иконки PWA
├── config/                  # Конфигурационные файлы
├── monitoring/              # Prometheus, Grafana
├── requirements.txt
└── README.md
```

## Этапы разработки

- [x] Фаза 1: Одиночный сервер + базовый клиент
- [x] Фаза 2: Клиентская надёжность (офлайн, очередь, PWA)
- [x] Фаза 3: Кластер серверов (heartbeat, election, репликация)
- [x] Фаза 4: Мониторинг и полировка (Grafana, Docker, алерты)

## Возможности

### Фаза 1
- Регистрация и аутентификация пользователей
- Создание и удаление комнат
- Отправка и получение сообщений
- Список пользователей в комнате
- Prometheus метрики
- Structured logging

### Фаза 2
- IndexedDB для локального хранения сообщений
- Очередь неотправленных сообщений
- Exponential backoff для повторных отправок
- Service Worker для офлайн-режима
- PWA manifest (установка на домашний экран)
- Индикатор статуса подключения
- Индикатор очереди сообщений

### Фаза 3
- Heartbeat между серверами (каждые 2 сек)
- Leader Election (Bully algorithm)
- WAL репликация (master → slave)
- Автоматическое переключение при падении master
- HTTP API для управления кластером

## Кластер (Фаза 3)

### Запуск кластера из 3 серверов

```bash
# Запуск
./scripts/start_cluster.sh

# Остановка
./scripts/stop_cluster.sh
```

### Переменные окружения

| Переменная | Описание | Пример |
|------------|----------|--------|
| `SERVER_ID` | ID сервера | `server1`, `server2`, `server3` |
| `CHAT_PORT` | Порт сервера | `8081`, `8082`, `8083` |
| `PEERS` | Список пиров | `localhost:8082,localhost:8083` |
| `CLUSTER_ENABLED` | Включить кластер | `true` |

### API кластера

- `GET /cluster/health` - Health с информацией о кластере
- `GET /cluster/state` - Полное состояние кластера
- `POST /cluster/election/start` - Начало выборов
- `POST /cluster/election/coordinator` - Уведомление о новом master
- `POST /cluster/replication/wal` - Получение WAL записей
- `GET /cluster/replication/sync?after_seq=N` - Синхронизация WAL

### Bully Algorithm

1. Сервер с наибольшим ID среди живых становится master
2. При падении master начинаются новые выборы
3. Выборы занимают ~6 секунд (3 heartbeat пропуска)

## Развёртывание (Docker Compose)

### Быстрый старт

```bash
# Запуск кластера (3 сервера + Prometheus + Grafana)
./scripts/docker-start.sh

# Просмотр логов
docker-compose logs -f

# Остановка
./scripts/docker-stop.sh
```

### Сервисы

| Сервис | Порт | Описание |
|--------|------|----------|
| Chat Server 1 | 8080 | Master (по умолчанию) |
| Chat Server 2 | 8081 | Slave |
| Chat Server 3 | 8082 | Slave |
| Prometheus | 9090 | Сбор метрик |
| Grafana | 3000 | Дашборды (admin/admin) |

### Мониторинг

**Grafana:**
- Откройте http://localhost:3000
- Логин: `admin`, пароль: `admin`
- Дашборд "Chat Server Cluster" доступен автоматически

**Prometheus:**
- Откройте http://localhost:9090
- Доступные метрики: `irc_*`, `cluster_*`

**Алерты:**
- `ClusterMasterDown` — master не отвечает > 1 мин
- `HighReplicationLag` — отставание репликации > 100 записей
- `NoConnectedClients` — нет клиентов > 5 мин
- `ServerDown` — сервер не отвечает > 1 мин
- `HighMessageRate` — высокая нагрузка > 100 msg/sec

### Очистка данных

```bash
# Удаление контейнеров и томов
docker-compose down -v
```

## API

### WebSocket команды

**Клиент → Сервер:**
```json
{ "cmd": "REGISTER", "nick": "vasya", "password": "optional" }
{ "cmd": "LOGIN",    "nick": "vasya", "password": "secret" }
{ "cmd": "JOIN",     "room": "#general" }
{ "cmd": "LEAVE",    "room": "#general" }
{ "cmd": "MSG",      "room": "#general", "text": "привет!", "client_msg_id": "uuid-123" }
{ "cmd": "CREATE_ROOM", "room": "#my-room" }
{ "cmd": "DELETE_ROOM", "room": "#my-room" }
{ "cmd": "LIST_ROOMS" }
{ "cmd": "WHO",      "room": "#general" }
```

**Сервер → Клиент:**
```json
{ "event": "OK",         "cmd": "LOGIN",  "nick": "vasya" }
{ "event": "ERROR",      "cmd": "LOGIN",  "message": "неверный пароль" }
{ "event": "MESSAGE",    "room": "#general", "nick": "petya", "text": "привет!", "ts": 1705312200, "msg_id": "server-uuid-456" }
{ "event": "JOINED",     "room": "#general", "nick": "vasya" }
{ "event": "LEFT",       "room": "#general", "nick": "vasya" }
{ "event": "ROOM_LIST",  "rooms": ["#general", "#random", "#my-room"] }
{ "event": "USER_LIST",  "room": "#general", "users": ["vasya", "petya"] }
{ "event": "ACK",        "client_msg_id": "uuid-123", "msg_id": "server-uuid-456" }
```

### HTTP endpoints

- `GET /health` - Health check
- `GET /metrics` - Prometheus метрики
- `GET /api/servers` - Список серверов кластера
- `GET /ws` - WebSocket endpoint
