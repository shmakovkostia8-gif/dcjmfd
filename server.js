const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const compression = require('compression');
const helmet = require('helmet');
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 7432;
const NODE_ENV = process.env.NODE_ENV || 'production';
const app = express();
const server = http.createServer(app);

// ╔═══════════════════════════════════════════════════════════╗
// ║          БЕЗОПАСНОСТЬ И ОПТИМИЗАЦИЯ                      ║
// ╚═══════════════════════════════════════════════════════════╝
app.use(helmet({
    contentSecurityPolicy: false,
    crossOriginResourcePolicy: { policy: 'cross-origin' }
}));
app.use(compression({ level: 6 }));
app.use(express.json({ limit: '100mb' }));
app.use(express.urlencoded({ limit: '100mb', extended: true }));
app.use(express.static(path.join(__dirname), {
    etag: false,
    maxAge: '24h',
    setHeaders: (res) => {
        res.setHeader('Cache-Control', 'public, max-age=86400');
    }
}));

// ╔═══════════════════════════════════════════════════════════╗
// ║          SOCKET.IO НАСТРОЙКИ                             ║
// ╚═══════════════════════════════════════════════════════════╝
const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST'],
        credentials: false
    },
    pingTimeout: 120000,
    pingInterval: 30000,
    maxHttpBufferSize: 100 * 1024 * 1024,
    transports: ['websocket', 'polling'],
    serveClient: false
});

// ╔═══════════════════════════════════════════════════════════╗
// ║          МАРШРУТЫ И УТИЛИТЫ                              ║
// ╚═══════════════════════════════════════════════════════════╝
app.get('/', (req, res) => {
    res.setHeader('Cache-Control', 'public, max-age=3600');
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/api/health', (req, res) => {
    res.json({
        status: 'healthy',
        uptime: process.uptime(),
        rooms: rooms.size,
        totalConnections: io.engine.clientsCount,
        timestamp: new Date().toISOString()
    });
});

app.get('/api/invite/:inviteId', (req, res) => {
    const invite = invites.get(req.params.inviteId);
    if (!invite) {
        return res.status(404).json({ error: 'Invite not found' });
    }
    res.json({
        roomId: invite.roomId,
        createdBy: invite.createdBy,
        expiresAt: invite.expiresAt,
        usageCount: invite.usageCount
    });
});

// ╔═══════════════════════════════════════════════════════════╗
// ║          ХРАНИЛИЩЕ                                        ║
// ╚═══════════════════════════════════════════════════════════╝
const rooms = new Map();
const invites = new Map();
const userSessions = new Map();

const CONFIG = {
    MAX_MESSAGES_PER_CHANNEL: 500,
    MAX_ROOMS_LIFETIME: 90 * 60 * 1000,
    CLEANUP_INTERVAL: 10 * 60 * 1000,
    MAX_MESSAGE_LENGTH: 5000,
    MAX_ROOM_NAME_LENGTH: 30,
    MAX_USER_NAME_LENGTH: 50,
    INVITE_EXPIRY: 7 * 24 * 60 * 60 * 1000, // 7 дней
    MAX_PEERS_PER_ROOM: 100
};

function getRoom(roomId) {
    if (!rooms.has(roomId)) {
        rooms.set(roomId, {
            id: roomId,
            channels: new Map([['general', {
                id: 'general',
                messages: [],
                createdAt: Date.now()
            }]]),
            users: new Map(),
            created: Date.now(),
            stats: {
                totalMessages: 0,
                peakUsers: 0,
                sessionDurations: []
            }
        });
    }
    return rooms.get(roomId);
}

function createInvite(roomId, createdBy) {
    const inviteId = uuidv4();
    invites.set(inviteId, {
        id: inviteId,
        roomId,
        createdBy,
        createdAt: Date.now(),
        expiresAt: Date.now() + CONFIG.INVITE_EXPIRY,
        usageCount: 0
    });
    return inviteId;
}

// ╔═══════════════════════════════════════════════════════════╗
// ║          АВТООЧИСТКА                                      ║
// ╚═══════════════════════════════════════════════════════════╝
setInterval(() => {
    const now = Date.now();
    let cleanedRooms = 0;
    let cleanedInvites = 0;

    // Очистка комнат
    for (const [roomId, room] of rooms.entries()) {
        if (room.users.size === 0 && now - room.created > CONFIG.MAX_ROOMS_LIFETIME) {
            rooms.delete(roomId);
            cleanedRooms++;
        }
    }

    // Очистка истекших инвайтов
    for (const [inviteId, invite] of invites.entries()) {
        if (now > invite.expiresAt) {
            invites.delete(inviteId);
            cleanedInvites++;
        }
    }

    if (cleanedRooms > 0 || cleanedInvites > 0) {
        console.log(`🧹 Cleanup: ${cleanedRooms} rooms, ${cleanedInvites} invites`);
    }
}, CONFIG.CLEANUP_INTERVAL);

// ╔═══════════════════════════════════════════════════════════╗
// ║          СТАТИСТИКА                                       ║
// ╚═══════════════════════════════════════════════════════════╝
setInterval(() => {
    let totalUsers = 0;
    let totalMessages = 0;
    let maxRoomSize = 0;

    for (const room of rooms.values()) {
        totalUsers += room.users.size;
        totalMessages += room.stats.totalMessages;
        if (room.users.size > maxRoomSize) maxRoomSize = room.users.size;
    }

    if (totalUsers > 0 || rooms.size > 0) {
        console.log(`📊 Status: ${totalUsers} users | ${rooms.size} rooms | ${totalMessages} messages | peak: ${maxRoomSize}`);
    }
}, 60000);

// ╔═══════════════════════════════════════════════════════════╗
// ║          SOCKET.IO СОБЫТИЯ                               ║
// ╚═══════════════════════════════════════════════════════════╝
io.on('connection', (socket) => {
    let currentRoom = null;
    let currentChannel = 'general';
    let myName = 'Аноним';
    let myColor = '#e8a87c';
    let joinedAt = Date.now();
    let inviteId = null;

    console.log(`🔌 [${socket.id}] Connected from ${socket.handshake.address}`);

    // ─────────────────────────────────────────────────────────
    // ПРИСОЕДИНЕНИЕ К КОМНАТЕ
    // ─────────────────────────────────────────────────────────
    socket.on('join', ({ roomId, name, color, inviteId: invId }) => {
        try {
            if (!roomId || typeof roomId !== 'string') {
                socket.emit('error:join', { text: 'Invalid room ID' });
                return;
            }

            // Обработка инвайта
            if (invId) {
                const invite = invites.get(invId);
                if (!invite || Date.now() > invite.expiresAt) {
                    socket.emit('error:join', { text: 'Invite expired or invalid' });
                    return;
                }
                invite.usageCount++;
                inviteId = invId;
            }

            currentRoom = roomId.toUpperCase().trim().slice(0, CONFIG.MAX_ROOM_NAME_LENGTH);
            myName = (name || 'Аноним').slice(0, CONFIG.MAX_USER_NAME_LENGTH).trim();
            myColor = /^#[0-9A-Fa-f]{6}$/.test(color) ? color : '#d4820a';

            const room = getRoom(currentRoom);

            // Проверка лимита
            if (room.users.size >= CONFIG.MAX_PEERS_PER_ROOM) {
                socket.emit('error:join', { text: 'Room is full' });
                return;
            }

            // Удаление дубликатов
            if (room.users.has(socket.id)) {
                room.users.delete(socket.id);
            }

            const userData = {
                id: socket.id,
                name: myName,
                color: myColor,
                channel: currentChannel,
                joinedAt: Date.now(),
                audioEnabled: true,
                videoEnabled: false,
                screenEnabled: false,
                speakingLevel: 0
            };

            room.users.set(socket.id, userData);
            userSessions.set(socket.id, { roomId: currentRoom, userData });

            if (room.users.size > room.stats.peakUsers) {
                room.stats.peakUsers = room.users.size;
            }

            socket.join(currentRoom);

            const channelsList = Array.from(room.channels.keys());
            const channelHistory = (room.channels.get(currentChannel)?.messages || [])
                .slice(-CONFIG.MAX_MESSAGES_PER_CHANNEL);
            const usersList = Array.from(room.users.values());

            socket.emit('room:state', {
                roomId: currentRoom,
                channels: channelsList,
                currentChannel,
                messages: channelHistory,
                users: usersList,
                roomStats: room.stats,
                mySocketId: socket.id
            });

            io.to(currentRoom).emit('peer:joined', {
                sid: socket.id,
                name: myName,
                color: myColor,
                userData,
                timestamp: Date.now()
            });

            console.log(`✅ [${currentRoom}] ${myName} joined (total: ${room.users.size})`);
        } catch (e) {
            console.error('❌ Join error:', e.message);
            socket.emit('error:join', { text: 'Connection error' });
        }
    });

    // ─────────────────────────────────────────────────────────
    // СОЗДАНИЕ ИНВАЙТА
    // ─────────────────────────────────────────────────────────
    socket.on('invite:create', (callback) => {
        try {
            if (!currentRoom || typeof callback !== 'function') return;
            const newInviteId = createInvite(currentRoom, myName);
            callback({
                inviteId: newInviteId,
                roomId: currentRoom,
                inviteUrl: `${process.env.INVITE_BASE_URL || 'http://localhost:7432'}?invite=${newInviteId}&room=${currentRoom}`
            });
            console.log(`🎟️ [${currentRoom}] Invite created by ${myName}`);
        } catch (e) {
            console.error('❌ Invite creation error:', e.message);
        }
    });

    // ─────────────────────────────────────────────────────────
    // АУДИО ДИАГНОСТИКА
    // ─────────────────────────────────────────────────────────
    socket.on('audio:check', (callback) => {
        try {
            if (typeof callback === 'function') {
                callback({
                    status: 'ok',
                    timestamp: Date.now(),
                    socketId: socket.id,
                    serverTime: new Date().toISOString(),
                    message: 'Audio channel verified - server hearing you clearly'
                });
            }
        } catch (e) {
            console.error('❌ Audio check error:', e.message);
        }
    });

    // ─────────────────────────────────────────────────────────
    // КАНАЛЫ
    // ─────────────────────────────────────────────────────────
    socket.on('channel:create', ({ channelName }) => {
        try {
            if (!currentRoom) return;
            const room = rooms.get(currentRoom);
            if (!room) return;

            const normalized = channelName.trim().toLowerCase()
                .slice(0, CONFIG.MAX_ROOM_NAME_LENGTH)
                .replace(/[^a-z0-9_-]/g, '');

            if (!room.channels.has(normalized) && /^[a-z0-9_-]{2,}$/.test(normalized)) {
                room.channels.set(normalized, {
                    id: normalized,
                    messages: [],
                    createdAt: Date.now()
                });
                io.to(currentRoom).emit('channel:added', {
                    channelName: normalized,
                    createdBy: myName,
                    timestamp: Date.now()
                });
                console.log(`📢 [${currentRoom}] Channel #${normalized} created`);
            }
        } catch (e) {
            console.error('❌ Channel creation error:', e.message);
        }
    });

    socket.on('channel:switch', ({ channelName }) => {
        try {
            if (!currentRoom) return;
            const room = rooms.get(currentRoom);
            if (!room || !room.channels.has(channelName)) return;

            const user = room.users.get(socket.id);
            if (user) user.channel = channelName;
            currentChannel = channelName;

            const messages = room.channels.get(channelName).messages
                .slice(-CONFIG.MAX_MESSAGES_PER_CHANNEL);
            socket.emit('channel:switched', { channelName, messages });
            io.to(currentRoom).emit('peer:channelChanged', {
                sid: socket.id,
                channel: channelName,
                timestamp: Date.now()
            });
            console.log(`🔄 [${currentRoom}] ${myName} → #${channelName}`);
        } catch (e) {
            console.error('❌ Channel switch error:', e.message);
        }
    });

    // ─────────────────────────────────────────────────────────
    // ЧАТИНГ С ПОЛНОЙ МОДЕРАЦИЕЙ
    // ─────────────────────────────────────────────────────────
    socket.on('chat:send', ({ text }) => {
        try {
            if (!currentRoom || !text?.trim()) return;
            const room = rooms.get(currentRoom);
            if (!room) return;

            const channel = room.channels.get(currentChannel);
            if (!channel) return;

            const sanitized = text
                .slice(0, CONFIG.MAX_MESSAGE_LENGTH)
                .trim()
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#39;');

            if (!sanitized) return;

            const msg = {
                id: `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
                sid: socket.id,
                author: myName,
                color: myColor,
                text: sanitized,
                ts: Date.now(),
                channel: currentChannel
            };

            channel.messages.push(msg);
            room.stats.totalMessages++;

            if (channel.messages.length > CONFIG.MAX_MESSAGES_PER_CHANNEL) {
                channel.messages.shift();
            }

            io.to(currentRoom).emit('chat:message', msg);
            console.log(`💬 [${currentRoom}/#${currentChannel}] ${myName}: ${sanitized.substring(0, 50)}`);
        } catch (e) {
            console.error('❌ Message send error:', e.message);
        }
    });

    socket.on('chat:react', ({ msgId, emoji }) => {
        try {
            if (!currentRoom || !msgId || !emoji) return;
            if (emoji.length > 5) return;

            io.to(currentRoom).emit('chat:reaction', {
                msgId,
                emoji: emoji.slice(0, 2),
                from: myName,
                sid: socket.id,
                channel: currentChannel,
                timestamp: Date.now()
            });
        } catch (e) {
            console.error('❌ Reaction error:', e.message);
        }
    });

    // ──────────────────────────────────────────────────────���──
    // WEBRTC СИГНАЛИНГ (УЛУЧШЕННЫЙ)
    // ─────────────────────────────────────────────────────────
    socket.on('rtc:offer', ({ to, offer }) => {
        try {
            if (!to || !offer) return;
            io.to(to).emit('rtc:offer', {
                from: socket.id,
                fromName: myName,
                fromColor: myColor,
                offer,
                timestamp: Date.now()
            });
            console.log(`📞 RTC offer: ${socket.id} → ${to}`);
        } catch (e) {
            console.error('❌ RTC offer error:', e.message);
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
            console.log(`✅ RTC answer: ${socket.id} → ${to}`);
        } catch (e) {
            console.error('❌ RTC answer error:', e.message);
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
            console.error('❌ ICE candidate error:', e.message);
        }
    });

    socket.on('rtc:error', ({ to, error }) => {
        try {
            io.to(to).emit('rtc:error', {
                from: socket.id,
                error,
                timestamp: Date.now()
            });
            console.log(`⚠️ RTC error from ${socket.id}: ${error}`);
        } catch (e) {
            console.error('❌ RTC error handling:', e.message);
        }
    });

    // ─────────────────────────────────────────────────────────
    // СОСТОЯНИЕ МЕДИА
    // ─────────────────────────────────────────────────────────
    socket.on('media:state', (state) => {
        try {
            if (currentRoom && typeof state === 'object') {
                const user = rooms.get(currentRoom)?.users.get(socket.id);
                if (user) {
                    if (typeof state.audio === 'boolean') user.audioEnabled = state.audio;
                    if (typeof state.video === 'boolean') user.videoEnabled = state.video;
                    if (typeof state.screen === 'boolean') user.screenEnabled = state.screen;
                    if (typeof state.speakingLevel === 'number') user.speakingLevel = state.speakingLevel;
                }
                io.to(currentRoom).emit('media:changed', {
                    sid: socket.id,
                    ...state,
                    timestamp: Date.now()
                });
            }
        } catch (e) {
            console.error('❌ Media state error:', e.message);
        }
    });

    // ─────────────────────────────────────────────────────────
    // ИНДИКАТОР ПЕЧАТИ
    // ─────────────────────────────────────────────────────────
    socket.on('typing', (isTyping) => {
        try {
            if (currentRoom) {
                io.to(currentRoom).emit('user:typing', {
                    sid: socket.id,
                    name: myName,
                    isTyping: !!isTyping,
                    channel: currentChannel,
                    timestamp: Date.now()
                });
            }
        } catch (e) {
            console.error('❌ Typing error:', e.message);
        }
    });

    // ─────────────────────────────────────────────────────────
    // ПИНГ (ЗАМЕР ЗАДЕРЖКИ)
    // ─────────────────────────────────────────────────────────
    socket.on('ping', (callback) => {
        try {
            if (typeof callback === 'function') {
                callback({ pong: Date.now() });
            }
        } catch (e) {
            console.error('❌ Ping error:', e.message);
        }
    });

    // ─────────────────────────────────────────────────────────
    // ОТКЛЮЧЕНИЕ
    // ─────────────────────────────────────────────────────────
    socket.on('disconnect', (reason) => {
        try {
            if (currentRoom) {
                const room = rooms.get(currentRoom);
                if (room) {
                    const sessionDuration = Date.now() - joinedAt;
                    room.stats.sessionDurations.push(sessionDuration);
                    room.users.delete(socket.id);

                    io.to(currentRoom).emit('peer:left', {
                        sid: socket.id,
                        name: myName,
                        reason,
                        timestamp: Date.now()
                    });

                    console.log(`🚪 [${currentRoom}] ${myName} disconnected (${Math.round(sessionDuration / 1000)}s, reason: ${reason})`);
                }
            }
            userSessions.delete(socket.id);
        } catch (e) {
            console.error('❌ Disconnect error:', e.message);
        }
    });

    socket.on('error', (err) => {
        console.error(`⚠️ Socket error [${socket.id}]:`, err);
    });
});

// ╔═══════════════════════════════════════════════════════════╗
// ║          ГЛОБАЛЬНАЯ ОБРАБОТКА ОШИБОК                      ║
// ╚═══════════════════════════════════════════════════════════╝
process.on('uncaughtException', (err) => {
    console.error('❌ Uncaught Exception:', err);
    process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('❌ Unhandled Rejection:', reason);
});

// ╔═══════════════════════════════════════════════════════════╗
// ║          ЗАПУСК СЕРВЕРА                                   ║
// ╚═══════════════════════════════════════════════════════════╝
server.listen(PORT, '0.0.0.0', () => {
    const banner = `
╔════════════════════════════════════════════════════════╗
║   ⚡ NEXUS v3.0 - Advanced Voice Chat                 ║
║   🚀 Server Online & Ready                            ║
╟────────────────────────────────────────────────────────╢
║   Port: ${PORT}${' '.repeat(45 - PORT.toString().length)}║
║   Mode: ${NODE_ENV}${' '.repeat(49 - NODE_ENV.length)}║
║   URL: http://localhost:${PORT}${' '.repeat(33 - PORT.toString().length)}║
║   WebSocket: ✅ Ready                                 ║
║   Audio: 🎙️ Optimized P2P                            ║
╚════════════════════════════════════════════════════════╝
    `;
    console.log(banner);
});

module.exports = { app, server, io };
