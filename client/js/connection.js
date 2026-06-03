/**
 * Управление WebSocket подключением
 * Фаза 2: С поддержкой очереди отправок и retry
 */

class Connection {
    constructor() {
        this.ws = null;
        this.servers = [];
        this.currentServerIndex = 0;
        this.backoffMs = 500;
        this.maxBackoff = 30000;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = Infinity;
        this.onMessageCallback = null;
        this.onStatusChangeCallback = null;
        this.isConnected = false;
        this.isConnecting = false;
        
        // Retry queue
        this.retryQueue = [];
        this.retryTimer = null;
        this.initialBackoff = 500;
        this.maxRetryBackoff = 30000;
        this.currentBackoff = this.initialBackoff;
    }

    setServers(servers) {
        this.servers = servers;
        console.log('Список серверов:', servers);
        
        // Сохраняем в IndexedDB
        if (window.storage?.db) {
            window.storage.setServers(servers).catch(console.error);
        }
    }

    async loadServersFromStorage() {
        if (window.storage?.db && this.servers.length === 0) {
            const stored = await window.storage.getServers();
            if (stored && stored.length > 0) {
                this.servers = stored;
                console.log('Загружены серверы из хранилища:', this.servers);
            }
        }
    }

    connect() {
        if (this.isConnecting) {
            console.log('Уже подключаемся...');
            return;
        }

        if (this.servers.length === 0) {
            // Stand-alone режим: используем текущий хост
            this.servers = [{ 
                host: window.location.hostname || 'localhost', 
                port: 8080, 
                role: 'master' 
            }];
            console.log('Stand-alone режим: сервер по умолчанию', this.servers[0]);
        }

        const server = this.servers[this.currentServerIndex];
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${server.host}:${server.port}/ws`;

        console.log(`Подключение к ${wsUrl} (попытка ${this.reconnectAttempts + 1})...`);
        this.isConnecting = true;
        this._updateStatus('connecting');

        try {
            this.ws = new WebSocket(wsUrl);

            this.ws.onopen = () => {
                console.log('✅ Подключено к серверу');
                this.isConnected = true;
                this.isConnecting = false;
                this.backoffMs = this.initialBackoff;
                this.reconnectAttempts = 0;
                this._updateStatus('connected');
                
                // Обработка очереди после подключения
                this._processRetryQueue();
            };

            this.ws.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    if (this.onMessageCallback) {
                        this.onMessageCallback(data);
                    }
                } catch (e) {
                    console.error('Ошибка парсинга сообщения:', e);
                }
            };

            this.ws.onclose = (event) => {
                console.log(`❌ Соединение закрыто (code: ${event.code}, reason: ${event.reason})`);
                this.isConnected = false;
                this.isConnecting = false;
                this._handleDisconnect();
            };

            this.ws.onerror = (error) => {
                console.error('Ошибка WebSocket:', error);
            };

        } catch (e) {
            console.error('Ошибка создания WebSocket:', e);
            this.isConnecting = false;
            this._handleDisconnect();
        }
    }

    disconnect() {
        // Остановка retry таймера
        if (this.retryTimer) {
            clearTimeout(this.retryTimer);
            this.retryTimer = null;
        }
        
        if (this.ws) {
            this.ws.close();
            this.ws = null;
        }
        this.isConnected = false;
        this.isConnecting = false;
    }

    send(data) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify(data));
            return true;
        }
        return false;
    }

    /**
     * Отправка сообщения с поддержкой retry очереди
     * @param {Object} data - Данные для отправки
     * @param {boolean} useQueue - Использовать ли очередь при неудаче
     * @returns {boolean} - Успешно ли отправлено (или добавлено в очередь)
     */
    async sendWithRetry(data, useQueue = true) {
        if (this.send(data)) {
            return true;
        }
        
        if (useQueue && data.client_msg_id) {
            // Добавляем в retry очередь
            await this._addToRetryQueue(data);
            return false;
        }
        
        return false;
    }

    onMessage(callback) {
        this.onMessageCallback = callback;
    }

    onStatusChange(callback) {
        this.onStatusChangeCallback = callback;
    }

    _handleDisconnect() {
        this._updateStatus('disconnected');

        if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnectAttempts++;
            const delay = Math.min(
                this.backoffMs * Math.pow(1.5, this.reconnectAttempts - 1),
                this.maxBackoff
            );
            
            console.log(`🔄 Переподключение через ${delay}мс (попытка ${this.reconnectAttempts})`);
            
            this.retryTimer = setTimeout(() => {
                // Переключение на следующий сервер
                this.currentServerIndex = (this.currentServerIndex + 1) % this.servers.length;
                this.connect();
            }, delay);
        } else {
            console.log('⚠️ Превышено максимальное количество попыток подключения');
        }
    }

    _updateStatus(status) {
        if (this.onStatusChangeCallback) {
            this.onStatusChangeCallback(status);
        }
    }

    // === Retry Queue ===

    async _addToRetryQueue(data) {
        const queueItem = {
            data,
            attempts: 0,
            created_at: Date.now(),
            next_retry: Date.now()
        };
        
        this.retryQueue.push(queueItem);
        console.log(`📦 Сообщение добавлено в очередь retry. Всего в очереди: ${this.retryQueue.length}`);
        
        // Обновляем индикатор очереди
        if (window.app?.updateQueueIndicator) {
            window.app.updateQueueIndicator();
        }
        
        // Сохраняем в IndexedDB
        if (window.storage?.db) {
            await window.storage.addToPendingQueue({
                client_msg_id: data.client_msg_id,
                room: data.room,
                text: data.text,
                data: data,
                attempts: 0,
                created_at: Date.now(),
                next_retry: Date.now()
            });
        }
    }

    async _processRetryQueue() {
        if (this.retryQueue.length === 0) {
            console.log('✅ Очередь retry пуста');
            if (window.app?.updateQueueIndicator) {
                window.app.updateQueueIndicator();
            }
            return;
        }

        console.log(`🔄 Обработка retry очереди (${this.retryQueue.length} сообщений)...`);

        const stillPending = [];
        
        for (const item of this.retryQueue) {
            if (this.send(item.data)) {
                console.log(`✅ Сообщение из retry очереди отправлено`);
                // Удаляем из IndexedDB
                if (window.storage?.db && item.data.client_msg_id) {
                    await window.storage.removeFromPendingQueue(item.data.client_msg_id);
                }
            } else {
                // Не удалось отправить - увеличиваем backoff
                item.attempts++;
                const delay = Math.min(
                    this.initialBackoff * Math.pow(2, item.attempts),
                    this.maxRetryBackoff
                );
                item.next_retry = Date.now() + delay;
                stillPending.push(item);
                
                console.log(`⏳ Сообщение не отправлено, следующая попытка через ${delay}мс`);
            }
        }
        
        this.retryQueue = stillPending;
        
        // Обновляем индикатор
        if (window.app?.updateQueueIndicator) {
            window.app.updateQueueIndicator();
        }
        
        // Планируем следующую попытку
        if (this.retryQueue.length > 0) {
            const nextRetry = Math.min(...this.retryQueue.map(i => i.next_retry));
            const delay = Math.max(nextRetry - Date.now(), 100);
            
            console.log(`⏰ Следующая попытка retry через ${delay}мс`);
            this.retryTimer = setTimeout(() => this._processRetryQueue(), delay);
        }
    }

    async loadPendingFromStorage() {
        if (!window.storage?.db) return;
        
        const pending = await window.storage.getAllPendingMessages();
        for (const item of pending) {
            if (item.data) {
                this.retryQueue.push({
                    data: item.data,
                    attempts: item.attempts || 0,
                    created_at: item.created_at,
                    next_retry: item.next_retry || Date.now()
                });
            }
        }
        
        if (pending.length > 0) {
            console.log(`📦 Загружено ${pending.length} сообщений из очереди хранилища`);
        }
    }
}

// Экспорт
window.connection = new Connection();
