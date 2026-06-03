/**
 * IRC-команды клиента
 */

class Commands {
    constructor() {
        this.currentNick = null;
    }

    // === Аутентификация ===

    register(nick, password = '') {
        console.log('📤 REGISTER:', nick);
        return this._sendCommand('REGISTER', { nick, password });
    }

    login(nick, password = '') {
        console.log('📤 LOGIN:', nick);
        return this._sendCommand('LOGIN', { nick, password });
    }

    // === Комнаты ===

    listRooms() {
        console.log('📤 LIST_ROOMS');
        return this._sendCommand('LIST_ROOMS', {});
    }

    createRoom(room) {
        console.log('📤 CREATE_ROOM:', room);
        return this._sendCommand('CREATE_ROOM', { room });
    }

    deleteRoom(room) {
        console.log('📤 DELETE_ROOM:', room);
        return this._sendCommand('DELETE_ROOM', { room });
    }

    join(room) {
        console.log('📤 JOIN:', room);
        return this._sendCommand('JOIN', { room });
    }

    leave(room) {
        console.log('📤 LEAVE:', room);
        return this._sendCommand('LEAVE', { room });
    }

    who(room) {
        console.log('📤 WHO:', room);
        return this._sendCommand('WHO', { room });
    }

    // === Сообщения ===

    msg(room, text, clientMsgId = null) {
        console.log('📤 MSG:', room, text);
        return this._sendCommand('MSG', { room, text, client_msg_id: clientMsgId });
    }

    // === Вспомогательные ===

    _sendCommand(cmd, params) {
        const data = { cmd, ...params };
        console.log('📤 Отправка команды:', data);
        
        if (!window.connection) {
            console.error('❌ window.connection не инициализирован');
            return false;
        }
        
        const result = window.connection.send(data);
        console.log('📤 Результат отправки:', result);
        return result;
    }
}

// Экспорт
window.commands = new Commands();
