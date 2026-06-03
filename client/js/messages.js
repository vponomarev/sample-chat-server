/**
 * Управление сообщениями и UI
 * Фаза 2: С поддержкой статусов отправки
 */

class MessagesUI {
    constructor() {
        this.currentRoom = '#general';
        this.messagesContainer = null;
        this.messageMap = new Map(); // client_msg_id -> message element
        this.initialized = false;
    }

    init() {
        if (this.initialized) return;
        this.messagesContainer = document.getElementById('messages-container');
        this.initialized = true;
        console.log('💬 MessagesUI инициализирован');
    }

    setCurrentRoom(room) {
        this.init();
        this.currentRoom = room;
        if (document.getElementById('current-room')) {
            document.getElementById('current-room').textContent = room;
        }
        this.loadMessages(room);
    }

    addMessage(data) {
        const { room, nick, text, ts, msg_id, event, history } = data;
        const currentUser = window.app?.currentUser;

        // Системные сообщения
        if (event === 'JOINED') {
            this._addSystemMessage(`${nick} присоединился к ${room}`);
            return;
        }

        if (event === 'LEFT') {
            this._addSystemMessage(`${nick} покинул ${room}`);
            return;
        }

        // Обычные сообщения
        if (event !== 'MESSAGE') return;

        // Если это наше собственное сообщение - игнорируем
        // (оно уже отображено как pending, статус обновится через ACK)
        // Исключение: история от другого клиента
        if (nick === currentUser && !history) {
            console.log('🔁 Игнорируем своё сообщение (ждём ACK)');
            return;
        }

        const message = {
            msg_id,
            room,
            nick,
            text,
            ts,
            status: 'sent',
            own: nick === currentUser
        };

        // Сохранение в IndexedDB
        if (window.storage?.db) {
            window.storage.addMessage(message).catch(console.error);
        }

        // Отображение если в текущей комнате
        if (room === this.currentRoom) {
            this._renderMessage(message);
            this._scrollToBottom();
        }
    }

    addPendingMessage(clientMsgId, text) {
        const message = {
            client_msg_id: clientMsgId,
            msg_id: clientMsgId, // временно используем client_msg_id
            room: this.currentRoom,
            nick: window.app?.currentUser || 'me',
            text,
            ts: Math.floor(Date.now() / 1000),
            status: 'pending',
            own: true
        };

        // Сохранение в IndexedDB (и в messages и в pending)
        if (window.storage?.db) {
            window.storage.addMessage(message).catch(console.error);
            window.storage.addToPendingQueue({
                client_msg_id: clientMsgId,
                room: this.currentRoom,
                text,
                data: {
                    cmd: 'MSG',
                    room: this.currentRoom,
                    text,
                    client_msg_id: clientMsgId
                },
                attempts: 0,
                created_at: Date.now()
            }).catch(console.error);
        }

        this._renderMessage(message);
        this._scrollToBottom();

        return clientMsgId;
    }

    updateMessageStatus(clientMsgId, status, serverMsgId) {
        console.log(`Обновление статуса сообщения ${clientMsgId}: ${status}`);
        
        const msgElement = this.messageMap.get(clientMsgId);
        if (msgElement) {
            let statusEl = msgElement.querySelector('.message-status');
            if (!statusEl) {
                statusEl = document.createElement('div');
                statusEl.className = 'message-status';
                msgElement.appendChild(statusEl);
            }
            
            statusEl.textContent = this._getStatusText(status);
            statusEl.className = `message-status ${status}`;
            
            // Обновляем в хранилище
            if (window.storage?.db && status === 'sent') {
                window.storage.updateMessage(clientMsgId, { 
                    status: 'sent',
                    msg_id: serverMsgId 
                }).catch(console.error);
                
                // Удаляем из pending очереди
                window.storage.removeFromPendingQueue(clientMsgId).catch(console.error);
            }
        }
    }

    markMessageFailed(clientMsgId, error) {
        const msgElement = this.messageMap.get(clientMsgId);
        if (msgElement) {
            let statusEl = msgElement.querySelector('.message-status');
            if (!statusEl) {
                statusEl = document.createElement('div');
                statusEl.className = 'message-status';
                msgElement.appendChild(statusEl);
            }
            
            statusEl.textContent = `✗ Ошибка: ${error || 'Не отправлено'}`;
            statusEl.className = 'message-status failed';
            
            // Обновляем в хранилище
            if (window.storage?.db) {
                window.storage.updateMessage(clientMsgId, { status: 'failed' }).catch(console.error);
            }
        }
    }

    loadMessages(room) {
        if (!this.messagesContainer) return;
        
        this.messagesContainer.innerHTML = '';
        this.messageMap.clear();
        
        // Загрузка из IndexedDB
        if (window.storage?.db) {
            window.storage.getMessagesByRoom(room).then(messages => {
                // Сортировка по времени
                messages.sort((a, b) => (a.ts || 0) - (b.ts || 0));
                messages.forEach(msg => this._renderMessage(msg));
                this._scrollToBottom();
            }).catch(err => {
                console.error('Ошибка загрузки сообщений:', err);
            });
        }
    }

    _renderMessage(message) {
        if (!this.messagesContainer) return;
        
        const div = document.createElement('div');
        div.className = `message ${message.own ? 'own' : ''}`;
        
        // Сохраняем ссылку на элемент для обновления статуса
        if (message.client_msg_id) {
            this.messageMap.set(message.client_msg_id, div);
            div.setAttribute('data-client-id', message.client_msg_id);
        }
        if (message.msg_id) {
            div.setAttribute('data-msg-id', message.msg_id);
        }

        const time = new Date((message.ts || 0) * 1000).toLocaleTimeString('ru-RU', {
            hour: '2-digit',
            minute: '2-digit'
        });

        div.innerHTML = `
            <div class="message-header">
                <span class="message-nick">${this._escapeHtml(message.nick)}</span>
                <span class="message-time">${time}</span>
            </div>
            <div class="message-text">${this._escapeHtml(message.text)}</div>
            ${message.status ? `<div class="message-status ${message.status}">${this._getStatusText(message.status)}</div>` : ''}
        `;

        this.messagesContainer.appendChild(div);
    }

    _addSystemMessage(text) {
        if (!this.messagesContainer) return;
        
        const div = document.createElement('div');
        div.className = 'system-message';
        div.textContent = text;
        this.messagesContainer.appendChild(div);
        this._scrollToBottom();
    }

    _getStatusText(status) {
        switch (status) {
            case 'pending': return '⏳ Отправка...';
            case 'sent': return '✓';
            case 'failed': return '✗ Ошибка';
            case 'retry': return '🔄 Повтор...';
            default: return status;
        }
    }

    _scrollToBottom() {
        if (this.messagesContainer) {
            this.messagesContainer.scrollTop = this.messagesContainer.scrollHeight;
        }
    }

    _escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    clearCurrentRoom() {
        if (this.messagesContainer) {
            this.messagesContainer.innerHTML = '';
        }
        this.messageMap.clear();
    }
}

// Экспорт
window.messagesUI = new MessagesUI();
