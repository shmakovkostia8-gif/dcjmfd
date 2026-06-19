const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const PORT = process.env.PORT || 7432;   // на Railway используется предоставленный порт
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST']
    },
    pingTimeout: 60000,
    pingInterval: 25000
});

app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname)));

app.get('/', (_req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// ── Хранилище комнат ──
const rooms = new Map();

function getRoom(roomId) {
    if (!rooms.has(roomId)) {
        rooms.set(roomId, {
            channels: new Map([['general', { messages: [], createdAt: Date.now() }]]),
            users: new Map(),
            created: Date.now()
        });
    }
    return rooms.get(roomId);
}

// Максимальное число сообщений на канал
const MAX_MESSAGES_PER_CHANNEL = 200;

// Автоочистка комнат, в которых никого нет 10 минут
setInterval(() => {
    const now = Date.now();
    for (const [roomId, room] of rooms.entries()) {
        if (room.users.size === 0 && now - room.created > 600000) {
            rooms.delete(roomId);
            console.log(`🧹 Комната ${roomId} удалена (пуста)`);
        }
    }
}, 300000);

io.on('connection', (socket) => {
    let currentRoom = null;
    let currentChannel = 'general';
    let myName = 'Аноним';
    let myColor = '#e8a87c';

    console.log(`🔌 Новое подключение: ${socket.id}`);

    // Присоединение к комнате
    socket.on('join', ({ roomId, name, color }) => {
        if (!roomId || typeof roomId !== 'string') return;

        currentRoom = roomId.toUpperCase().trim();
        myName = (name || 'Аноним').slice(0, 32);
        myColor = color && /^#[0-9A-Fa-f]{6}$/.test(color) ? color : '#d4820a';

        const room = getRoom(currentRoom);

        // Если такой сокет уже есть, удаляем старую запись
        if (room.users.has(socket.id)) {
            room.users.delete(socket.id);
        }

        // Добавляем пользователя
        room.users.set(socket.id, { name: myName, color: myColor, channel: currentChannel });
        socket.join(currentRoom);

        // Отправляем новичку состояние комнаты
        const channelsList = Array.from(room.channels.keys());
        const channelHistory = (room.channels.get(currentChannel)?.messages || []).slice(-MAX_MESSAGES_PER_CHANNEL);
        const usersList = Array.from(room.users.entries()).map(([sid, u]) => ({
            sid, name: u.name, color: u.color, channel: u.channel
        }));

        socket.emit('room:state', {
            channels: channelsList,
            currentChannel,
            messages: channelHistory,
            users: usersList
        });

        // Оповещаем остальных о новом участнике
        socket.to(currentRoom).emit('peer:join', {
            sid: socket.id,
            name: myName,
            color: myColor,
            channel: currentChannel
        });

        console.log(`📥 ${myName} (${socket.id}) вошёл в комнату ${currentRoom}, канал ${currentChannel}`);
    });

    // Создание нового канала
    socket.on('channel:create', ({ channelName }) => {
        if (!currentRoom) return;
        const room = rooms.get(currentRoom);
        if (!room) return;

        const normalizedName = channelName.trim().toLowerCase();
        if (!room.channels.has(normalizedName) && /^[\wа-яА-ЯёЁ0-9_-]{2,20}$/.test(normalizedName)) {
            room.channels.set(normalizedName, { messages: [], createdAt: Date.now() });
            io.to(currentRoom).emit('channel:added', normalizedName);
            console.log(`📢 В комнате ${currentRoom} создан канал ${normalizedName}`);
        } else if (room.channels.has(normalizedName)) {
            socket.emit('system:msg', { text: `Канал "${normalizedName}" уже существует` });
        }
    });

    // Переключение канала
    socket.on('channel:switch', ({ channelName }) => {
        if (!currentRoom) return;
        const room = rooms.get(currentRoom);
        if (!room || !room.channels.has(channelName)) return;

        const user = room.users.get(socket.id);
        if (user) user.channel = channelName;
        currentChannel = channelName;

        const messages = room.channels.get(channelName).messages.slice(-MAX_MESSAGES_PER_CHANNEL);
        socket.emit('channel:switched', { channelName, messages });
        socket.to(currentRoom).emit('peer:channel', { sid: socket.id, channel: channelName });
        console.log(`🔄 ${myName} переключился на канал ${channelName}`);
    });

    // Отправка сообщения
    socket.on('chat:send', ({ text }) => {
        if (!currentRoom || !text?.trim()) return;
        const room = rooms.get(currentRoom);
        if (!room) return;

        const channel = room.channels.get(currentChannel);
        if (!channel) return;

        const msg = {
            id: `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
            sid: socket.id,
            author: myName,
            color: myColor,
            text: text.slice(0, 2000),
            ts: Date.now()
        };

        channel.messages.push(msg);
        // Ограничиваем историю
        if (channel.messages.length > MAX_MESSAGES_PER_CHANNEL) {
            channel.messages.shift();
        }

        io.to(currentRoom).emit('chat:msg', { ...msg, channel: currentChannel });
        console.log(`💬 ${myName} в #${currentChannel}: ${msg.text.substring(0, 50)}`);
    });

    // Реакция на сообщение
    socket.on('chat:react', ({ msgId, emoji }) => {
        if (!currentRoom || !msgId || !emoji) return;
        io.to(currentRoom).emit('chat:react', {
            msgId,
            emoji,
            from: myName,
            sid: socket.id,
            channel: currentChannel
        });
    });

    // WebRTC сигналинг
    socket.on('rtc:offer', ({ to, offer }) => {
        if (!to || !offer) return;
        io.to(to).emit('rtc:offer', { from: socket.id, name: myName, color: myColor, offer });
    });

    socket.on('rtc:answer', ({ to, answer }) => {
        if (!to || !answer) return;
        io.to(to).emit('rtc:answer', { from: socket.id, answer });
    });

    socket.on('rtc:ice', ({ to, candidate }) => {
        if (!to || !candidate) return;
        io.to(to).emit('rtc:ice', { from: socket.id, candidate });
    });

    // Состояние медиа (микрофон, камера)
    socket.on('media:state', (state) => {
        if (currentRoom && typeof state === 'object') {
            socket.to(currentRoom).emit('media:state', { sid: socket.id, ...state });
        }
    });

    // Печатает...
    socket.on('typing', (isTyping) => {
        if (currentRoom) {
            socket.to(currentRoom).emit('typing', {
                sid: socket.id,
                name: myName,
                on: !!isTyping,
                channel: currentChannel
            });
        }
    });

    // Системное сообщение
    socket.on('system:msg', (text) => {
        if (currentRoom && typeof text === 'string') {
            io.to(currentRoom).emit('system:msg', {
                name: myName,
                text: text.slice(0, 200),
                channel: currentChannel
            });
        }
    });

    // Пинг (для замера задержки, если нужно)
    socket.on('ping', (callback) => {
        if (typeof callback === 'function') callback({ pong: Date.now() });
    });

    // Отключение
    socket.on('disconnect', () => {
        if (currentRoom) {
            const room = rooms.get(currentRoom);
            if (room) {
                room.users.delete(socket.id);
                io.to(currentRoom).emit('peer:leave', { sid: socket.id, name: myName });
                console.log(`🚪 ${myName} (${socket.id}) покинул комнату ${currentRoom}, осталось ${room.users.size} участников`);

                // Если комната опустела, она будет удалена по таймеру
                if (room.users.size === 0) {
                    console.log(`⏳ Комната ${currentRoom} опустела, будет удалена через 10 минут`);
                }
            }
        }
    });
});

server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n╔══════════════════════════════════════════╗
║   ⚡ NEXUS  сервер запущен!              ║
║   Порт: ${PORT}                              ║
║   Локально: http://localhost:${PORT}        ║
║   WebSocket готов к работе                 ║
╚══════════════════════════════════════════╝\n`);
});
