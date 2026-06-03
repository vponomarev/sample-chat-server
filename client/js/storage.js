/**
 * Локальное хранилище (IndexedDB)
 * Фаза 2: Улучшенная версия с поддержкой очереди отправок
 */

const DB_NAME = 'SampleChatDB';
const DB_VERSION = 3;

class Storage {
    constructor() {
        this.db = null;
        this.initPromise = null;
    }

    async init() {
        if (this.initPromise) return this.initPromise;
        
        this.initPromise = new Promise((resolve, reject) => {
            const request = indexedDB.open(DB_NAME, DB_VERSION);

            request.onerror = () => {
                console.error('Ошибка открытия IndexedDB:', request.error);
                reject(request.error);
            };
            
            request.onsuccess = () => {
                this.db = request.result;
                console.log('IndexedDB инициализирована');
                resolve(this.db);
            };

            request.onupgradeneeded = (event) => {
                const db = event.target.result;
                const oldVersion = event.oldVersion;

                // Таблица сообщений (версия 1)
                if (oldVersion < 1 || !db.objectStoreNames.contains('messages')) {
                    const messagesStore = db.createObjectStore('messages', { keyPath: 'msg_id', autoIncrement: false });
                    messagesStore.createIndex('room', 'room', { unique: false });
                    messagesStore.createIndex('ts', 'ts', { unique: false });
                    messagesStore.createIndex('status', 'status', { unique: false });
                }

                // Таблица комнат (версия 1)
                if (oldVersion < 1 || !db.objectStoreNames.contains('rooms')) {
                    db.createObjectStore('rooms', { keyPath: 'name' });
                }

                // Таблица очереди отправок (версия 1)
                if (oldVersion < 1 || !db.objectStoreNames.contains('pending_messages')) {
                    const pendingStore = db.createObjectStore('pending_messages', { keyPath: 'client_msg_id' });
                    pendingStore.createIndex('status', 'status', { unique: false });
                    pendingStore.createIndex('created_at', 'created_at', { unique: false });
                }

                // Таблица сессии (версия 1)
                if (oldVersion < 1 || !db.objectStoreNames.contains('session')) {
                    db.createObjectStore('session', { keyPath: 'key' });
                }

                // Таблица серверов (версия 2)
                if (oldVersion < 2 || !db.objectStoreNames.contains('servers')) {
                    db.createObjectStore('servers', { keyPath: 'host_port' });
                }

                // Таблица настроек (версия 3)
                if (oldVersion < 3 || !db.objectStoreNames.contains('settings')) {
                    db.createObjectStore('settings', { keyPath: 'key' });
                }
            };
        });
        
        return this.initPromise;
    }

    // === Сообщения ===

    async addMessage(message) {
        return this._put('messages', message);
    }

    async getMessage(msgId) {
        return this._get('messages', msgId);
    }

    async getMessagesByRoom(room) {
        return this._getAllByIndex('messages', 'room', room);
    }

    async getAllMessages() {
        return this._getAll('messages');
    }

    async updateMessage(msgId, updates) {
        const message = await this.getMessage(msgId);
        if (message) {
            Object.assign(message, updates);
            return this._put('messages', message);
        }
        return null;
    }

    async deleteMessage(msgId) {
        return this._delete('messages', msgId);
    }

    async clearMessages() {
        return this._clear('messages');
    }

    async clearMessagesByRoom(room) {
        const messages = await this.getMessagesByRoom(room);
        for (const msg of messages) {
            await this.deleteMessage(msg.msg_id);
        }
    }

    // === Комнаты ===

    async addRoom(room) {
        return this._put('rooms', { name: room });
    }

    async getRoom(roomName) {
        return this._get('rooms', roomName);
    }

    async getAllRooms() {
        return this._getAll('rooms');
    }

    async removeRoom(roomName) {
        return this._delete('rooms', roomName);
    }

    async clearRooms() {
        return this._clear('rooms');
    }

    // === Очередь отправок ===

    async addToPendingQueue(message) {
        message.status = 'pending';
        message.attempts = message.attempts || 0;
        message.created_at = message.created_at || Date.now();
        message.next_retry = message.next_retry || Date.now();
        return this._put('pending_messages', message);
    }

    async getPendingMessage(clientMsgId) {
        return this._get('pending_messages', clientMsgId);
    }

    async getAllPendingMessages() {
        return this._getAll('pending_messages');
    }

    async getDuePendingMessages() {
        // Получить сообщения, готовые к отправке (время retry наступило)
        const all = await this.getAllPendingMessages();
        const now = Date.now();
        return all.filter(msg => (msg.next_retry || 0) <= now);
    }

    async updatePendingMessage(clientMsgId, updates) {
        const message = await this.getPendingMessage(clientMsgId);
        if (message) {
            Object.assign(message, updates);
            return this._put('pending_messages', message);
        }
        return null;
    }

    async removeFromPendingQueue(clientMsgId) {
        return this._delete('pending_messages', clientMsgId);
    }

    async clearPendingQueue() {
        return this._clear('pending_messages');
    }

    async getPendingQueueCount() {
        return this._count('pending_messages');
    }

    // === Серверы ===

    async setServers(servers) {
        // Очищаем и сохраняем новые
        await this._clear('servers');
        for (const server of servers) {
            const hostPort = `${server.host}:${server.port}`;
            await this._put('servers', { ...server, host_port: hostPort });
        }
    }

    async getServers() {
        return this._getAll('servers');
    }

    async getServer(host, port) {
        return this._get('servers', `${host}:${port}`);
    }

    // === Сессия ===

    async setSession(session) {
        return this._put('session', { key: 'current', ...session });
    }

    async getSession() {
        return this._get('session', 'current');
    }

    async clearSession() {
        return this._delete('session', 'current');
    }

    // === Настройки ===

    async setServerSettings(settings) {
        return this._put('settings', { key: 'server', ...settings });
    }

    async getServerSettings() {
        return this._get('settings', 'server');
    }

    async clearServerSettings() {
        return this._delete('settings', 'server');
    }

    // === Вспомогательные методы ===

    _transaction(storeName, mode = 'readonly') {
        return this.db.transaction([storeName], mode).objectStore(storeName);
    }

    _get(storeName, key) {
        return new Promise((resolve, reject) => {
            const store = this._transaction(storeName);
            const request = store.get(key);
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    _getAll(storeName) {
        return new Promise((resolve, reject) => {
            const store = this._transaction(storeName);
            const request = store.getAll();
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    _getAllByIndex(storeName, indexName, value) {
        return new Promise((resolve, reject) => {
            const store = this._transaction(storeName);
            const index = store.index(indexName);
            const request = index.getAll(value);
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    _put(storeName, item) {
        return new Promise((resolve, reject) => {
            const store = this._transaction(storeName, 'readwrite');
            const request = store.put(item);
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }

    _delete(storeName, key) {
        return new Promise((resolve, reject) => {
            const store = this._transaction(storeName, 'readwrite');
            const request = store.delete(key);
            request.onsuccess = () => resolve();
            request.onerror = () => reject(request.error);
        });
    }

    _clear(storeName) {
        return new Promise((resolve, reject) => {
            const store = this._transaction(storeName, 'readwrite');
            const request = store.clear();
            request.onsuccess = () => resolve();
            request.onerror = () => reject(request.error);
        });
    }

    _count(storeName) {
        return new Promise((resolve, reject) => {
            const store = this._transaction(storeName);
            const request = store.count();
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
    }
}

// Экспорт
window.storage = new Storage();
