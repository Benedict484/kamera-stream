const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript',
  '.css': 'text/css',
};

const server = http.createServer((req, res) => {
  let filePath = req.url === '/' ? '/index.html' : req.url;
  filePath = path.join(__dirname, 'public', filePath);
  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Nicht gefunden');
      return;
    }
    const ext = path.extname(filePath);
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
});

const wss = new WebSocket.Server({ server });

// code -> { sender: ws|null, receiver: ws|null }
const rooms = new Map();

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

wss.on('connection', (ws) => {
  ws.room = null;
  ws.role = null;

  ws.on('message', (raw, isBinary) => {
    // Binärdaten = ein Bild vom Sender. Direkt und ohne Umwege an den
    // Empfänger weiterreichen — das spart Rechenzeit und reduziert Verzögerung.
    if (isBinary) {
      if (ws.role === 'sender' && ws.room) {
        const room = rooms.get(ws.room);
        if (room && room.receiver && room.receiver.readyState === WebSocket.OPEN) {
          room.receiver.send(raw, { binary: true });
        }
      }
      return;
    }

    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    if (msg.type === 'register') {
      const code = String(msg.room || '').toUpperCase().trim();
      const role = msg.role === 'sender' ? 'sender' : 'receiver';
      if (!code) { send(ws, { type: 'error', message: 'Kein Code angegeben' }); return; }

      let room = rooms.get(code);
      if (!room) { room = { sender: null, receiver: null }; rooms.set(code, room); }
      room[role] = ws;
      ws.room = code;
      ws.role = role;

      send(ws, { type: 'registered', role, code });

      const other = role === 'sender' ? room.receiver : room.sender;
      if (other) { send(other, { type: 'peer-joined' }); send(ws, { type: 'peer-joined' }); }
      return;
    }

    if (msg.type === 'leave') {
      ws.close();
      return;
    }
  });

  ws.on('close', () => {
    if (!ws.room) return;
    const room = rooms.get(ws.room);
    if (!room) return;
    if (room.sender === ws) room.sender = null;
    if (room.receiver === ws) room.receiver = null;
    const other = ws.role === 'sender' ? room.receiver : room.sender;
    if (other) send(other, { type: 'peer-left' });
    if (!room.sender && !room.receiver) rooms.delete(ws.room);
  });
});

server.listen(PORT, () => {
  console.log('');
  console.log('  ✓ Server läuft auf http://localhost:' + PORT);
  console.log('  Jetzt in einem zweiten Terminal-Fenster: ngrok http ' + PORT);
  console.log('');
});
