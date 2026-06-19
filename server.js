const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');

const PORT = process.env.PORT || 7432;
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

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname)));

app.get('/', (_req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

const rooms = new Map();
const MAX_MESSAGES_PER_CHANNEL = 200;

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

// Очистка пустых комнат
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

    socket.on('join', ({ roomId, name, color }) => {
        if (!roomId || typeof roomId !== 'string') return;
        
        currentRoom = roomId.toUpperCase().trim();
        myName = (name || 'Аноним').slice(0, 32);
        myColor = color && /^#[0-9A-Fa-f]{6}$/.test(color) ? color : '#d4820a';
        
        const room = getRoom(currentRoom);
        
        if (room.users.has(socket.id)) {
            room.users.delete(socket.id);
        }
        
        room.users.set(socket.id, { name: myName, color: myColor, channel: currentChannel });
        socket.join(currentRoom);
        
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
        
        socket.to(currentRoom).emit('peer:join', {
            sid: socket.id,
            name: myName,
            color: myColor,
            channel: currentChannel
        });
        
        console.log(`📥 ${myName} (${socket.id}) вошёл в комнату ${currentRoom}, канал ${currentChannel}`);
    });
    
    socket.on('channel:create', ({ channelName }) => {
        if (!currentRoom) return;
        const room = rooms.get(currentRoom);
        if (!room) return;
        
        const normalizedName = channelName.trim().toLowerCase();
        if (!room.channels.has(normalizedName) && /^[\wа-яА-ЯёЁ0-9_-]{2,20}$/.test(normalizedName)) {
            room.channels.set(normalizedName, { messages: [], createdAt: Date.now() });
            io.to(currentRoom).emit('channel:added', normalizedName);
            console.log(`📢 В комнате ${currentRoom} создан канал ${normalizedName}`);
        }
    });
    
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
    });
    
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
        if (channel.messages.length > MAX_MESSAGES_PER_CHANNEL) {
            channel.messages.shift();
        }
        
        io.to(currentRoom).emit('chat:msg', { ...msg, channel: currentChannel });
    });
    
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
    
    socket.on('media:state', (state) => {
        if (currentRoom && typeof state === 'object') {
            socket.to(currentRoom).emit('media:state', { sid: socket.id, ...state });
        }
    });
    
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
    
    socket.on('system:msg', (text) => {
        if (currentRoom && typeof text === 'string') {
            io.to(currentRoom).emit('system:msg', {
                name: myName,
                text: text.slice(0, 200),
                channel: currentChannel
            });
        }
    });
    
    socket.on('ping', (callback) => {
        if (typeof callback === 'function') callback({ pong: Date.now() });
    });
    
    socket.on('disconnect', () => {
        if (currentRoom) {
            const room = rooms.get(currentRoom);
            if (room) {
                room.users.delete(socket.id);
                io.to(currentRoom).emit('peer:leave', { sid: socket.id, name: myName });
                console.log(`🚪 ${myName} (${socket.id}) покинул комнату ${currentRoom}`);
            }
        }
    });
});

server.on('error', (err) => {
    console.error('❌ Ошибка сервера:', err);
});

server.listen(PORT, '0.0.0.0', () => {
    console.log(`\n╔══════════════════════════════════════════╗
║   ⚡ NEXUS  сервер запущен!              ║
║   Порт: ${PORT}                              ║
║   Локально: http://localhost:${PORT}        ║
║   WebSocket готов к работе                 ║
╚══════════════════════════════════════════╝\n`);
});
