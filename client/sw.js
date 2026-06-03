/**
 * Service Worker для офлайн-режима
 * Фаза 2: Кэширование статики и офлайн-поддержка
 */

const CACHE_NAME = 'sample-chat-v3';
const STATIC_ASSETS = [
    '/',
    'index.html',
    'css/style.css',
    'js/storage.js',
    'js/connection.js',
    'js/commands.js',
    'js/rooms.js',
    'js/messages.js',
    'js/app.js',
    'manifest.json',
    'icons/icon-192.png',
    'icons/icon-512.png'
];

// === Install ===

self.addEventListener('install', (event) => {
    console.log('[SW] Install');
    
    // Кэширование статики
    event.waitUntil(
        caches.open(CACHE_NAME)
            .then(cache => {
                console.log('[SW] Кэширование статики');
                return cache.addAll(STATIC_ASSETS);
            })
            .then(() => {
                console.log('[SW] Статика закэширована');
                return self.skipWaiting();
            })
            .catch(err => {
                console.error('[SW] Ошибка кэширования:', err);
            })
    );
});

// === Activate ===

self.addEventListener('activate', (event) => {
    console.log('[SW] Activate');
    
    // Очистка старых кэшей
    event.waitUntil(
        caches.keys()
            .then(cacheNames => {
                return Promise.all(
                    cacheNames
                        .filter(name => name !== CACHE_NAME)
                        .map(name => {
                            console.log('[SW] Удаление старого кэша:', name);
                            return caches.delete(name);
                        })
                );
            })
            .then(() => {
                console.log('[SW] Claiming clients');
                return self.clients.claim();
            })
    );
});

// === Fetch ===

self.addEventListener('fetch', (event) => {
    const { request } = event;
    const url = new URL(request.url);

    // Только для same-origin запросов
    if (url.origin !== location.origin) {
        return;
    }

    // API запросы - только сеть
    if (request.url.includes('/api/') || 
        request.url.includes('/ws') || 
        request.url.includes('/metrics')) {
        return;
    }

    // Статика - cache first, затем сеть
    event.respondWith(
        caches.match(request)
            .then(cachedResponse => {
                if (cachedResponse) {
                    console.log('[SW] Кэш:', request.url);
                    
                    // Асинхронное обновление кэша
                    fetch(request)
                        .then(response => {
                            if (response && response.status === 200) {
                                const clone = response.clone();
                                caches.open(CACHE_NAME)
                                    .then(cache => cache.put(request, clone));
                            }
                        })
                        .catch(() => {
                            // Офлайн - игнорируем
                        });
                    
                    return cachedResponse;
                }

                // Нет в кэше - запрос к сети
                console.log('[SW] Сеть:', request.url);
                return fetch(request)
                    .then(response => {
                        if (!response || response.status !== 200 || response.type !== 'basic') {
                            return response;
                        }

                        // Кэшируем успешный ответ
                        const clone = response.clone();
                        caches.open(CACHE_NAME)
                            .then(cache => cache.put(request, clone));

                        return response;
                    })
                    .catch(err => {
                        console.error('[SW] Ошибка сети:', err);
                        
                        // Для навигации - возвращаем index.html (SPA)
                        if (request.mode === 'navigate') {
                            return caches.match('/index.html');
                        }
                        
                        // Для остальных - ошибка
                        return new Response('Offline', {
                            status: 503,
                            statusText: 'Service Unavailable'
                        });
                    });
            })
    );
});

// === Message ===

self.addEventListener('message', (event) => {
    console.log('[SW] Message:', event.data);
    
    if (event.data && event.data.type === 'SKIP_WAITING') {
        self.skipWaiting();
    }
    
    if (event.data && event.data.type === 'CLIENTS_CLAIM') {
        self.clients.claim();
    }
});

// === Push notifications (задел на будущее) ===

self.addEventListener('push', (event) => {
    console.log('[SW] Push received');
    
    const data = event.data ? event.data.json() : {};
    const title = data.title || 'Новое сообщение';
    const options = {
        body: data.body || 'У вас новое сообщение в чате',
        icon: 'icons/icon-192.png',
        badge: 'icons/icon-192.png',
        tag: 'chat-message',
        requireInteraction: false
    };

    event.waitUntil(
        self.registration.showNotification(title, options)
    );
});

// === Notification click ===

self.addEventListener('notificationclick', (event) => {
    console.log('[SW] Notification click');
    
    event.notification.close();
    
    event.waitUntil(
        self.clients.matchAll({ type: 'window', includeUncontrolled: true })
            .then(clientList => {
                // Если есть открытое окно - фокусируем его
                for (const client of clientList) {
                    if (client.url.endsWith('/index.html') || client.url.endsWith('/') && 'focus' in client) {
                        return client.focus();
                    }
                }
                // Иначе открываем новое
                if (self.clients.openWindow) {
                    return self.clients.openWindow('index.html');
                }
            })
    );
});
