/**
 * Управление комнатами и UI
 */

class RoomsUI {
    constructor() {
        this.roomList = null;
        this.userList = null;
        this.currentRoom = '#general';
        this.initialized = false;
        this.joined = false;  // Флаг что присоединились к комнате
    }

    init() {
        if (this.initialized) return;
        
        this.roomList = document.getElementById('room-list');
        this.userList = document.getElementById('user-list');
        this.initialized = true;
        console.log('🏠 RoomsUI инициализирован');
    }

    updateRooms(rooms) {
        this.init();
        if (!this.roomList) {
            console.error('❌ room-list не найден');
            return;
        }
        
        this.roomList.innerHTML = '';

        rooms.forEach(room => {
            const li = document.createElement('li');
            li.className = room === this.currentRoom ? 'active' : '';
            li.innerHTML = `<span class="room-name">${this._escapeHtml(room)}</span>`;
            li.addEventListener('click', () => this._selectRoom(room));
            this.roomList.appendChild(li);
        });

        // Сохранение в IndexedDB
        rooms.forEach(room => window.storage.addRoom(room));
    }

    updateUsers(users) {
        this.userList.innerHTML = '';

        users.forEach(user => {
            const li = document.createElement('li');
            li.textContent = user;
            this.userList.appendChild(li);
        });
    }

    selectRoom(room, force = false) {
        this._selectRoom(room, force);
    }

    _selectRoom(room, force = false) {
        // Если комната уже выбрана и не force - выходим
        if (room === this.currentRoom && !force) return;

        // Покидаем текущую комнату (если не первый вход)
        if (this.currentRoom && this.joined) {
            window.commands.leave(this.currentRoom);
        }

        this.currentRoom = room;
        this.joined = true;

        // Обновляем UI
        const items = this.roomList.querySelectorAll('li');
        items.forEach(item => {
            item.classList.toggle('active', item.textContent.trim() === room);
        });

        // Присоединяемся к новой комнате
        window.commands.join(room);

        // Загружаем сообщения
        window.messagesUI.setCurrentRoom(room);
    }

    _escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}

// Экспорт
window.roomsUI = new RoomsUI();
