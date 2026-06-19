const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const compression = require('compression');
const helmet = require('helmet');

const PORT = process.env.PORT || 7432;
const NODE_ENV = process.env.NODE_ENV || 'development';
const app = express();
const server = http.createServer(app);

// ──── БЕЗОПАСНОСТЬ И ОПТИМИЗАЦИЯ ────
app.use(helmet({
    contentSecurityPolicy: false,
    crossOriginResourcePolicy: { policy: 'cross-origin' }
}));
app.use(compression());
app.use(express.json({ limit: '50mb' }));
app.use(express.static(path.join(__dirname), {
    etag: false,
    maxAge: '1d'
}));

const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST'],
        credentials: false
    },
    pingTimeout: 60000,
    pingInterval: 25000,
    maxHttpBufferSize: 50 * 1024 * 1024,
    transports: ['websocket', 'polling']
});

app.get('/', (_req, res) => {
    res.setHeader('Cache-Control', 'public, max-age=3600');
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/health', (_req, res) => {
    res.json({ status: 'ok', uptime: process.uptime(), rooms: rooms.size });
});

// ──── ХРАНИЛИЩЕ КОМНАТ ────
const rooms = new Map();
const MAX_MESSAGES_PER_CHANNEL = 300;
const MAX_ROOMS_LIFETIME = 60 * 60 * 1000; // 1 час
const CLEANUP_INTERVAL = 5 * 60 * 1000; // 5 минут
const MAX_MESSAGE_LENGTH = 2000;
const MAX_ROOM_NAME_LENGTH = 20;
const MAX_USER_NAME_LENGTH = 32;

function getRoom(roomId) {
    if (!rooms.has(roomId)) {
        rooms.set(roomId, {
            channels: new Map([['general', { messages: [], createdAt: Date.now() }]]),
            users: new Map(),
            created: Date.now(),
            stats: { totalMessages: 0, peakUsers: 1 }
        });
    }
    return rooms.get(roomId);
}

// Автоочистка неиспользуемых комнат
setInterval(() => {
    const now = Date.now();
    let cleaned = 0;
    for (const [roomId, room] of rooms.entries()) {
        if (room.users.size === 0 && now - room.created > MAX_ROOMS_LIFETIME) {
            rooms.delete(roomId);
            cleaned++;
        }
    }
    if (cleaned > 0) {
        console.log(`🧹 Удалено ${cleaned} неиспользуемых комнат`);
    }
}, CLEANUP_INTERVAL);

// Логирование активности
setInterval(() => {
    let totalUsers = 0;
    for (const room of rooms.values()) {
        totalUsers += room.users.size;
    }
    if (totalUsers > 0 || rooms.size > 0) {
        console.log(`📊 А��тивность: ${totalUsers} пользователей в ${rooms.size} комнатах`);
    }
}, 60000);

io.on('connection', (socket) => {
    let currentRoom = null;
    let currentChannel = 'general';
    let myName = 'Аноним';
    let myColor = '#e8a87c';
    let joinedAt = Date.now();

    console.log(`🔌 [${socket.id}] Новое подключение от ${socket.handshake.address}`);

    // Присоединение к комнате
    socket.on('join', ({ roomId, name, color }) => {
        try {
            if (!roomId || typeof roomId !== 'string') {
                socket.emit('error:msg', { text: 'Некорректный ID комнаты' });
                return;
            }

            currentRoom = roomId.toUpperCase().trim().slice(0, MAX_ROOM_NAME_LENGTH);
            myName = (name || 'Аноним').slice(0, MAX_USER_NAME_LENGTH).trim();
            myColor = color && /^#[0-9A-Fa-f]{6}$/.test(color) ? color : '#d4820a';

            const room = getRoom(currentRoom);

            // Защита от дублей
            if (room.users.has(socket.id)) {
                room.users.delete(socket.id);
            }

            room.users.set(socket.id, {
                name: myName,
                color: myColor,
                channel: currentChannel,
                joinedAt: Date.now(),
                audioEnabled: true,
                videoEnabled: false
            });

            // Обновить статистику
            if (room.users.size > room.stats.peakUsers) {
                room.stats.peakUsers = room.users.size;
            }

            socket.join(currentRoom);

            const channelsList = Array.from(room.channels.keys());
            const channelHistory = (room.channels.get(currentChannel)?.messages || []).slice(-MAX_MESSAGES_PER_CHANNEL);
            const usersList = Array.from(room.users.entries()).map(([sid, u]) => ({
                sid,
                name: u.name,
                color: u.color,
                channel: u.channel,
                audioEnabled: u.audioEnabled,
                videoEnabled: u.videoEnabled
            }));

            socket.emit('room:state', {
                channels: channelsList,
                currentChannel,
                messages: channelHistory,
                users: usersList,
                roomStats: room.stats
            });

            socket.to(currentRoom).emit('peer:join', {
                sid: socket.id,
                name: myName,
                color: myColor,
                channel: currentChannel,
                timestamp: Date.now()
            });

            console.log(`📥 [${currentRoom}] ${myName} присоединился (всего: ${room.users.size})`);
        } catch (e) {
            console.error('❌ Ошибка при присоединении:', e.message);
            socket.emit('error:msg', { text: 'Ошибка при присоединении к комнате' });
        }
    });

    // Создание нового канала
    socket.on('channel:create', ({ channelName }) => {
        try {
            if (!currentRoom) return;
            const room = rooms.get(currentRoom);
            if (!room) return;

            const normalizedName = channelName.trim().toLowerCase().slice(0, MAX_ROOM_NAME_LENGTH);
            if (!room.channels.has(normalizedName) && /^[\wа-яА-ЯёЁ0-9_-]{2,20}$/.test(normalizedName)) {
                room.channels.set(normalizedName, { messages: [], createdAt: Date.now() });
                io.to(currentRoom).emit('channel:added', normalizedName);
                console.log(`📢 [${currentRoom}] Создан канал #${normalizedName}`);
            }
        } catch (e) {
            console.error('❌ Ошибка при создании канала:', e.message);
        }
    });

    // Переключение канала
    socket.on('channel:switch', ({ channelName }) => {
        try {
            if (!currentRoom) return;
            const room = rooms.get(currentRoom);
            if (!room || !room.channels.has(channelName)) return;

            const user = room.users.get(socket.id);
            if (user) user.channel = channelName;
            currentChannel = channelName;

            const messages = room.channels.get(channelName).messages.slice(-MAX_MESSAGES_PER_CHANNEL);
            socket.emit('channel:switched', { channelName, messages });
            socket.to(currentRoom).emit('peer:channel', { sid: socket.id, channel: channelName });
            console.log(`🔄 [${currentRoom}] ${myName} → #${channelName}`);
        } catch (e) {
            console.error('❌ Ошибка при переключении канала:', e.message);
        }
    });

    // Отправка сообщения с модерацией
    socket.on('chat:send', ({ text }) => {
        try {
            if (!currentRoom || !text?.trim()) return;
            const room = rooms.get(currentRoom);
            if (!room) return;

            const channel = room.channels.get(currentChannel);
            if (!channel) return;

            const sanitizedText = text
                .slice(0, MAX_MESSAGE_LENGTH)
                .trim()
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;');

            if (!sanitizedText) return;

            const msg = {
                id: `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
                sid: socket.id,
                author: myName,
                color: myColor,
                text: sanitizedText,
                ts: Date.now(),
                channel: currentChannel
            };

            channel.messages.push(msg);
            room.stats.totalMessages++;

            if (channel.messages.length > MAX_MESSAGES_PER_CHANNEL) {
                channel.messages.shift();
            }

            io.to(currentRoom).emit('chat:msg', msg);
            console.log(`💬 [${currentRoom}/#${currentChannel}] ${myName}: ${sanitizedText.substring(0, 40)}`);
        } catch (e) {
            console.error('❌ Ошибка при отправке сообщения:', e.message);
        }
    });

    // Реакция на сообщение
    socket.on('chat:react', ({ msgId, emoji }) => {
        try {
            if (!currentRoom || !msgId || !emoji) return;
            if (!(/^[\p{Emoji}]/u.test(emoji)) && emoji.length > 2) return;

            io.to(currentRoom).emit('chat:react', {
                msgId,
                emoji: emoji.slice(0, 2),
                from: myName,
                sid: socket.id,
                channel: currentChannel,
                timestamp: Date.now()
            });
        } catch (e) {
            console.error('❌ Ошибка при реакции:', e.message);
        }
    });

    // WebRTC сигналинг
    socket.on('rtc:offer', ({ to, offer }) => {
        try {
            if (!to || !offer) return;
            io.to(to).emit('rtc:offer', {
                from: socket.id,
                name: myName,
                color: myColor,
                offer,
                timestamp: Date.now()
            });
        } catch (e) {
            console.error('❌ Ошибка при RTC offer:', e.message);
        }
    });

    socket.on('rtc:answer', ({ to, answer }) => {
        try {
            if (!to || !answer) return;
            io.to(to).emit('rtc:answer', {
                from: socket.id,
                answer,
                timestamp: Date.now()
            });
        } catch (e) {
            console.error('❌ Ошибка при RTC answer:', e.message);
        }
    });

    socket.on('rtc:ice', ({ to, candidate }) => {
        try {
            if (!to || !candidate) return;
            io.to(to).emit('rtc:ice', {
                from: socket.id,
                candidate,
                timestamp: Date.now()
            });
        } catch (e) {
            console.error('❌ Ошибка при RTC ice:', e.message);
        }
    });

    // Состояние медиа
    socket.on('media:state', (state) => {
        try {
            if (currentRoom && typeof state === 'object') {
                const user = rooms.get(currentRoom)?.users.get(socket.id);
                if (user) {
                    if (typeof state.mic === 'boolean') user.audioEnabled = state.mic;
                    if (typeof state.cam === 'boolean') user.videoEnabled = state.cam;
                }
                socket.to(currentRoom).emit('media:state', {
                    sid: socket.id,
                    ...state,
                    timestamp: Date.now()
                });
            }
        } catch (e) {
            console.error('❌ Ошибка при media:state:', e.message);
        }
    });

    // Печатает
    socket.on('typing', (isTyping) => {
        try {
            if (currentRoom) {
                socket.to(currentRoom).emit('typing', {
                    sid: socket.id,
                    name: myName,
                    on: !!isTyping,
                    channel: currentChannel,
                    timestamp: Date.now()
                });
            }
        } catch (e) {
            console.error('❌ Ошибка при typing:', e.message);
        }
    });

    // Диагностика аудио
    socket.on('audio:test', (callback) => {
        try {
            if (typeof callback === 'function') {
                callback({
                    status: 'ok',
                    timestamp: Date.now(),
                    serverId: socket.id
                });
            }
        } catch (e) {
            console.error('❌ Ошибка при audio:test:', e.message);
        }
    });

    // Ping
    socket.on('ping', (callback) => {
        try {
            if (typeof callback === 'function') {
                callback({ pong: Date.now(), latency: 0 });
            }
        } catch (e) {
            console.error('❌ Ошибка при ping:', e.message);
        }
    });

    // Отключение
    socket.on('disconnect', () => {
        try {
            if (currentRoom) {
                const room = rooms.get(currentRoom);
                if (room) {
                    const sessionDuration = Date.now() - joinedAt;
                    room.users.delete(socket.id);
                    io.to(currentRoom).emit('peer:leave', {
                        sid: socket.id,
                        name: myName,
                        timestamp: Date.now()
                    });
                    console.log(`🚪 [${currentRoom}] ${myName} покинул сессию (${Math.round(sessionDuration / 1000)}s, осталось: ${room.users.size})`);

                    if (room.users.size === 0) {
                        console.log(`⏳ [${currentRoom}] Комната опустела`);
                    }
                }
            }
        } catch (e) {
            console.error('❌ Ошибка при disconnect:', e.message);
        }
    });

    // Обработка ошибок
    socket.on('error', (err) => {
        console.error(`❌ Socket error [${socket.id}]:`, err);
    });
});

// Обработка ошибок сервера
process.on('uncaughtException', (err) => {
    console.error('❌ Uncaught Exception:', err);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});

server.listen(PORT, '0.0.0.0', () => {
    console.log(`
╔════════════════════════════════════════════════╗
║   ⚡ NEXUS v2.0 — Advanced Voice Chat        ║
║   🚀 Сервер запущен успешно!                  ║
║   Порт: ${PORT}                              
║   Окружение: ${NODE_ENV}                              
║   Адрес: http://localhost:${PORT}        
║   WebSocket: готов к работе ✓                 ║
╚════════════════════════════════════════════════╝
`);
});
