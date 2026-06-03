/**
 * Точка входа приложения
 * Фаза 2: С поддержкой офлайн-очереди и retry
 */

class ChatApp {
    constructor() {
        this.currentUser = null;
        this.isAuthenticated = false;
        this.pendingMessages = new Map(); // client_msg_id -> {text, room, timestamp}
    }

    async init() {
        console.log('🚀 Инициализация приложения...');
        
        // Инициализация хранилища
        await window.storage.init();
        console.log('✅ Хранилище инициализировано');

        // Регистрация Service Worker
        await this._registerServiceWorker();

        // Загрузка настроек сервера
        const serverSettings = await window.storage.getServerSettings();
        if (serverSettings && serverSettings.host && serverSettings.port) {
            console.log('Загружены настройки сервера:', serverSettings);
            window.connection.setServers([{
                host: serverSettings.host,
                port: parseInt(serverSettings.port),
                role: 'master'
            }]);
            this._autoconnect = serverSettings.autoconnect || false;
        }

        // Проверка сохранённой сессии
        const session = await window.storage.getSession();
        if (session?.nick) {
            this.currentUser = session.nick;
            this.showChatScreen();
        } else {
            this.showAuthScreen();
        }

        // Настройка обработчиков
        this._setupEventListeners();

        // Инициализация UI
        window.roomsUI?.init();
        window.messagesUI?.init();

        // Обработка сообщений от сервера
        window.connection.onMessage((data) => this._handleServerMessage(data));

        // Обработка изменения статуса подключения
        window.connection.onStatusChange((status) => this._updateConnectionStatus(status));

        // Автоматическое подключение
        console.log('🔌 Установка подключения...');
        this.connect();
    }

    connect() {
        // Проверяем, есть ли настроенный сервер
        let serverSettings = window.connection.servers?.[0];
        
        // Если сервер не настроен - используем localhost:8080
        if (!serverSettings?.host || !serverSettings?.port) {
            console.log('⚙️ Сервер не настроен, используем localhost:8080');
            serverSettings = { host: 'localhost', port: 8080 };
            window.connection.setServers([serverSettings]);
        }
        
        // Попытка получить список серверов кластера
        const protocol = window.location.protocol === 'https:' ? 'https:' : 'http:';
        fetch(`${protocol}//${serverSettings.host}:${serverSettings.port}/api/servers`)
            .then(res => res.json())
            .then(data => {
                console.log('📋 Получен список серверов:', data.servers);
                window.connection.setServers(data.servers);
                window.connection.connect();
            })
            .catch(err => {
                console.error('⚠️ Ошибка получения списка серверов:', err.message);
                // Используем настроенный сервер напрямую
                window.connection.connect();
            });
    }

    showAuthScreen() {
        const authScreen = document.getElementById('auth-screen');
        const chatScreen = document.getElementById('chat-screen');
        if (authScreen) authScreen.classList.remove('hidden');
        if (chatScreen) chatScreen.classList.add('hidden');
    }

    showChatScreen() {
        const authScreen = document.getElementById('auth-screen');
        const chatScreen = document.getElementById('chat-screen');
        if (authScreen) authScreen.classList.add('hidden');
        if (chatScreen) chatScreen.classList.remove('hidden');
        
        const nickEl = document.getElementById('current-nick');
        if (nickEl) nickEl.textContent = this.currentUser;
    }

    _setupEventListeners() {
        console.log('🔧 Настройка обработчиков событий...');
        
        // Переключение вкладок входа/регистрации
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                console.log('📑 Переключение вкладки:', e.target.dataset.tab);
                document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
                e.target.classList.add('active');

                const tab = e.target.dataset.tab;
                document.getElementById('login-form').classList.toggle('hidden', tab !== 'login');
                document.getElementById('register-form').classList.toggle('hidden', tab !== 'register');
                this._clearAuthError();
            });
        });

        // Настройки сервера на экране авторизации
        const authSettingsBtn = document.getElementById('auth-settings-btn');
        if (authSettingsBtn) {
            authSettingsBtn.addEventListener('click', () => {
                console.log('⚙️ Открытие настроек сервера');
                this._openServerSettings();
            });
        }

        // Форма входа
        const loginForm = document.getElementById('login-form');
        if (loginForm) {
            loginForm.addEventListener('submit', (e) => {
                e.preventDefault();
                const nick = document.getElementById('login-nick').value.trim();
                const password = document.getElementById('login-password').value.trim();
                console.log('🔐 Вход:', nick);
                this._handleLogin(nick, password);
            });
        }

        // Форма регистрации
        const registerForm = document.getElementById('register-form');
        if (registerForm) {
            registerForm.addEventListener('submit', (e) => {
                e.preventDefault();
                const nick = document.getElementById('register-nick').value.trim();
                const password = document.getElementById('register-password').value.trim();
                console.log('📝 Регистрация:', nick);
                this._handleRegister(nick, password);
            });
        }

        // Выход
        document.getElementById('logout-btn').addEventListener('click', () => {
            this._handleLogout();
        });

        // Форма отправки сообщения
        document.getElementById('message-form').addEventListener('submit', (e) => {
            e.preventDefault();
            const input = document.getElementById('message-input');
            const text = input.value.trim();
            if (text) {
                this._sendMessage(text);
                input.value = '';
            }
        });

        // Создание комнаты - кнопка
        document.getElementById('create-room-btn').addEventListener('click', () => {
            document.getElementById('create-room-modal').classList.remove('hidden');
            document.getElementById('new-room-name').value = '';
            document.getElementById('new-room-name').focus();
        });

        // Создание комнаты - отмена
        document.getElementById('cancel-create-room').addEventListener('click', () => {
            document.getElementById('create-room-modal').classList.add('hidden');
        });

        // Создание комнаты - подтверждение
        document.getElementById('confirm-create-room').addEventListener('click', () => {
            const roomName = document.getElementById('new-room-name').value.trim();
            if (roomName) {
                window.commands.createRoom(roomName);
                document.getElementById('create-room-modal').classList.add('hidden');
            }
        });

        // Enter в поле создания комнаты
        document.getElementById('new-room-name').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                document.getElementById('confirm-create-room').click();
            }
        });

        // Настройки сервера - кнопка
        document.getElementById('settings-btn').addEventListener('click', () => {
            this._openServerSettings();
        });

        // Настройки сервера - отмена
        document.getElementById('cancel-settings').addEventListener('click', () => {
            document.getElementById('server-settings-modal').classList.add('hidden');
        });

        // Настройки сервера - сохранение
        document.getElementById('save-settings').addEventListener('click', () => {
            this._saveServerSettings();
        });

        // Enter в поле настроек сервера
        document.getElementById('server-port').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                this._saveServerSettings();
            }
        });
    }

    _openServerSettings() {
        const modal = document.getElementById('server-settings-modal');
        const hostInput = document.getElementById('server-host');
        const portInput = document.getElementById('server-port');
        const autoconnectCheckbox = document.getElementById('server-autoconnect');

        // Загрузка текущих настроек
        window.storage.getServerSettings().then(settings => {
            if (settings) {
                hostInput.value = settings.host || 'localhost';
                portInput.value = settings.port || '8080';
                autoconnectCheckbox.checked = settings.autoconnect || false;
            } else {
                hostInput.value = 'localhost';
                portInput.value = '8080';
                autoconnectCheckbox.checked = false;
            }
            modal.classList.remove('hidden');
        });
    }

    async _saveServerSettings() {
        const host = document.getElementById('server-host').value.trim() || 'localhost';
        const port = document.getElementById('server-port').value.trim() || '8080';
        const autoconnect = document.getElementById('server-autoconnect').checked;

        const settings = {
            host,
            port: parseInt(port) || 8080,
            autoconnect
        };

        await window.storage.setServerSettings(settings);
        console.log('Настройки сервера сохранены:', settings);

        // Обновляем подключение
        window.connection.setServers([{ host, port: settings.port, role: 'master' }]);

        // Закрываем модальное окно
        document.getElementById('server-settings-modal').classList.add('hidden');

        // Если уже подключены - переподключаемся
        if (window.connection.isConnected) {
            window.connection.disconnect();
            window.connection.connect();
        }
    }

    _handleLogin(nick, password) {
        this._clearAuthError();
        
        // Проверка подключения
        if (!window.connection || !window.connection.ws || window.connection.ws.readyState !== WebSocket.OPEN) {
            console.error('❌ Нет подключения к серверу');
            this._showAuthError('Нет подключения к серверу. Проверьте настройки.');
            return;
        }
        
        window.commands.login(nick, password);
        // Ответ обработается в _handleServerMessage
    }

    _handleRegister(nick, password) {
        this._clearAuthError();
        
        // Проверка подключения
        if (!window.connection || !window.connection.ws || window.connection.ws.readyState !== WebSocket.OPEN) {
            console.error('❌ Нет подключения к серверу');
            this._showAuthError('Нет подключения к серверу. Проверьте настройки.');
            return;
        }
        
        window.commands.register(nick, password);
        // Ответ обработается в _handleServerMessage
    }

    _handleLogout() {
        this.currentUser = null;
        this.isAuthenticated = false;
        window.storage.clearSession();
        window.connection.disconnect();
        this.showAuthScreen();
    }

    _sendMessage(text) {
        const clientMsgId = this._generateId();
        
        // Проверка что комната выбрана
        const currentRoom = window.roomsUI?.currentRoom || '#general';
        console.log('📤 Отправка сообщения в', currentRoom, ':', text);

        // Отображение сообщения со статусом "pending"
        window.messagesUI.addPendingMessage(clientMsgId, text);

        // Отправка через connection с поддержкой retry
        const messageData = {
            cmd: 'MSG',
            room: currentRoom,
            text,
            client_msg_id: clientMsgId
        };

        window.connection.sendWithRetry(messageData, true);
    }

    _handleServerMessage(data) {
        console.log('📨 Получено сообщение:', data);

        switch (data.event) {
            case 'OK':
                if (data.cmd === 'LOGIN' || data.cmd === 'REGISTER') {
                    this.currentUser = data.nick;
                    this.isAuthenticated = true;
                    window.storage.setSession({ nick: data.nick });
                    this.showChatScreen();
                    
                    // Запрос списка комнат
                    window.commands.listRooms();
                }
                break;

            case 'ERROR':
                this._showAuthError(data.message);
                break;

            case 'ROOM_LIST':
                window.roomsUI.updateRooms(data.rooms);
                // Автоматическое присоединение к #general при первом входе
                if (data.rooms.includes('#general') && !window.roomsUI.joined) {
                    // Принудительно присоединяемся к #general
                    window.roomsUI.selectRoom('#general', true);
                }
                break;

            case 'USER_LIST':
                window.roomsUI.updateUsers(data.users);
                break;

            case 'MESSAGE':
            case 'JOINED':
            case 'LEFT':
                window.messagesUI.addMessage(data);
                break;

            case 'ACK':
                // Подтверждение получения сообщения сервером
                window.messagesUI.updateMessageStatus(data.client_msg_id, 'sent', data.msg_id);
                break;

            case 'SERVER_LIST':
                window.connection.setServers(data.servers);
                break;
        }
    }

    _updateConnectionStatus(status) {
        const statusEl = document.getElementById('connection-status');
        if (!statusEl) return;
        
        statusEl.classList.remove('status-connected', 'status-connecting', 'status-disconnected');

        switch (status) {
            case 'connected':
                statusEl.textContent = '🟢 Подключено';
                statusEl.classList.add('status-connected');
                break;
            case 'connecting':
                statusEl.textContent = '🟡 Подключение...';
                statusEl.classList.add('status-connecting');
                break;
            case 'disconnected':
                statusEl.textContent = '🔴 Офлайн';
                statusEl.classList.add('status-disconnected');
                break;
        }
    }

    _showAuthError(message) {
        const errorEl = document.getElementById('auth-error');
        if (errorEl) {
            errorEl.textContent = message;
            errorEl.classList.remove('hidden');
        }
    }

    _clearAuthError() {
        const errorEl = document.getElementById('auth-error');
        if (errorEl) {
            errorEl.classList.add('hidden');
        }
    }

    _generateId() {
        return 'msg-' + Math.random().toString(36).substr(2, 9) + '-' + Date.now();
    }

    async _registerServiceWorker() {
        if ('serviceWorker' in navigator) {
            try {
                const registration = await navigator.serviceWorker.register('/sw.js', {
                    scope: '/'
                });
                console.log('✅ Service Worker зарегистрирован:', registration.scope);
                
                // Проверяем обновления
                navigator.serviceWorker.addEventListener('controllerchange', () => {
                    console.log('🔄 Service Worker обновился');
                });
            } catch (error) {
                console.error('❌ Ошибка регистрации Service Worker:', error);
            }
        } else {
            console.warn('⚠️ Service Worker не поддерживается');
        }
    }

    async updateQueueIndicator() {
        const queueEl = document.getElementById('queue-indicator');
        const countEl = document.getElementById('queue-count');
        
        if (!queueEl || !countEl) return;
        
        const count = window.connection.retryQueue.length;
        
        if (count > 0) {
            countEl.textContent = count;
            queueEl.classList.add('visible');
        } else {
            queueEl.classList.remove('visible');
        }
    }
}

// Инициализация приложения
window.app = new ChatApp();
window.addEventListener('DOMContentLoaded', () => window.app.init());
