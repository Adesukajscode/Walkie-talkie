const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(express.static('public'));

// nomor -> socket.id
const users = new Map();

// Endpoint debug: buka https://xxx.trycloudflare.com/users di browser
app.get('/users', (req, res) => res.json([...users.keys()]));

const clean = (n) => String(n || '').replace(/\D/g, ''); // buang semua non-digit

io.on('connection', (socket) => {
  console.log('[+] socket', socket.id);

  socket.on('register', (raw) => {
    const noHp = clean(raw);
    if (noHp.length < 8) {
      socket.emit('register-error', 'Nomor tidak valid');
      return;
    }
    // Kalau nomor sama sudah dipakai socket lain, tendang yang lama
    const old = users.get(noHp);
    if (old && old !== socket.id) io.to(old).emit('force-logout');

    users.set(noHp, socket.id);
    socket.noHp = noHp;
    console.log(`[REG] ${noHp} => ${socket.id}`);
    console.log(`      ONLINE: [${[...users.keys()].join(', ')}]`);
    socket.emit('registered', noHp);
  });

  socket.on('call-user', ({ to }) => {
    const target = clean(to);
    console.log(`[CALL] ${socket.noHp} -> ${target}`);
    const targetId = users.get(target);
    if (!targetId) {
      const online = [...users.keys()].join(', ') || '(belum ada)';
      console.log(`      GAGAL: ${target} tidak online`);
      return socket.emit('call-error', `Nomor ${target} tidak online. Online: ${online}`);
    }
    io.to(targetId).emit('incoming-call', { from: socket.noHp });
  });

  socket.on('accept-call', ({ to }) => {
    const tid = users.get(clean(to));
    if (tid) io.to(tid).emit('call-accepted');
  });

  socket.on('reject-call', ({ to }) => {
    const tid = users.get(clean(to));
    if (tid) io.to(tid).emit('call-rejected');
  });

  socket.on('offer', ({ to, sdp }) => {
    const tid = users.get(clean(to));
    if (tid) io.to(tid).emit('offer', { from: socket.noHp, sdp });
  });

  socket.on('answer', ({ to, sdp }) => {
    const tid = users.get(clean(to));
    if (tid) io.to(tid).emit('answer', { sdp });
  });

  socket.on('ice-candidate', ({ to, candidate }) => {
    const tid = users.get(clean(to));
    if (tid) io.to(tid).emit('ice-candidate', { candidate });
  });

  socket.on('end-call', ({ to }) => {
    const tid = users.get(clean(to));
    if (tid) io.to(tid).emit('call-ended');
    // ⚠️ JANGAN hapus user di sini — biar tetap bisa dipanggil lagi
  });

  socket.on('disconnect', () => {
    if (socket.noHp && users.get(socket.noHp) === socket.id) {
      users.delete(socket.noHp);
      console.log(`[BYE] ${socket.noHp} | sisa: [${[...users.keys()].join(', ')}]`);
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => console.log(`Server: http://localhost:${PORT}`));
