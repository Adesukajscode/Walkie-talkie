export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    const up = (req.headers.get('Upgrade') || '').toLowerCase();
    if (up === 'websocket') {
      return env.ROOM.get(env.ROOM.idFromName('global')).fetch(req);
    }
    if (u.pathname === '/health') return Response.json({ ok: 1 });
    if (u.pathname === '/users') {
      const r = await env.ROOM.get(env.ROOM.idFromName('global')).fetch('https://do/users');
      return new Response(await r.text(), { headers: { 'content-type': 'application/json' } });
    }
    return env.ASSETS.fetch(req);
  }
};

export class Room {
  constructor() {
    this.socks = new Map();
    this.wsNo = new Map();
  }

  async fetch(req) {
    const u = new URL(req.url);
    if (u.pathname === '/users') return Response.json([...this.socks.keys()]);
    if ((req.headers.get('Upgrade') || '').toLowerCase() !== 'websocket') {
      return new Response('WS only', { status: 426 });
    }
    const pair = new WebSocketPair();
    const c = pair[0], s = pair[1];
    s.accept();
    s.addEventListener('message', (e) => {
      try { this.msg(s, e.data); } catch (err) { console.log('err', err.message); }
    });
    s.addEventListener('close', () => this.bye(s));
    s.addEventListener('error', () => this.bye(s));
    this.send(s, { type: 'welcome' });
    return new Response(null, { status: 101, webSocket: c });
  }

  msg(ws, raw) {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    const t = m.type;
    const clean = (n) => String(n || '').replace(/\D/g, '');
    console.log('MSG', t);

    if (t === 'ping') return this.send(ws, { type: 'pong' });

    if (t === 'register') {
      const no = clean(m.no);
      if (no.length < 8) return this.send(ws, { type: 'error', msg: 'Nomor invalid' });
      const old = this.socks.get(no);
      if (old && old !== ws) { try { old.close(); } catch {} }
      this.socks.set(no, ws);
      this.wsNo.set(ws, no);
      console.log('REG', no, 'total', this.socks.size);
      this.send(ws, { type: 'registered', no });
      this.online();
      return;
    }

    if (t === 'match') {
      const me = this.wsNo.get(ws);
      if (!me) return this.send(ws, { type: 'error', msg: 'Daftar dulu' });
      const others = [...this.socks.keys()].filter(x => x !== me);
      if (!others.length) return this.send(ws, { type: 'error', msg: 'Tidak ada user lain' });
      const target = others[Math.floor(Math.random() * others.length)];
      console.log('MATCH', me, '->', target);
      this.send(ws, { type: 'match-found', no: target });
      return;
    }

    if (t === 'call') {
      const from = this.wsNo.get(ws);
      const to = clean(m.to);
      const target = this.socks.get(to);
      if (!from) return this.send(ws, { type: 'error', msg: 'Daftar dulu' });
      if (!target) return this.send(ws, { type: 'error', msg: 'Nomor ' + to + ' tidak online' });
      console.log('CALL', from, '->', to);
      this.send(target, { type: 'incoming', from, sdp: m.sdp });
      return;
    }

    if (t === 'accept') {
      const target = this.socks.get(clean(m.to));
      if (target) this.send(target, { type: 'accepted' });
      return;
    }
    if (t === 'reject') {
      const target = this.socks.get(clean(m.to));
      if (target) this.send(target, { type: 'rejected' });
      return;
    }
    if (t === 'end') {
      const target = this.socks.get(clean(m.to));
      if (target) this.send(target, { type: 'ended' });
      return;
    }
    if (t === 'sdp' || t === 'ice') {
      const from = this.wsNo.get(ws);
      const target = this.socks.get(clean(m.to));
      if (target) this.send(target, { type: t, from, sdp: m.sdp, ice: m.ice });
      return;
    }
  }

  bye(ws) {
    const no = this.wsNo.get(ws);
    if (no && this.socks.get(no) === ws) this.socks.delete(no);
    this.wsNo.delete(ws);
    this.online();
  }

  send(ws, o) { try { ws.send(JSON.stringify(o)); } catch {} }
  online() {
    const list = [...this.socks.keys()];
    const msg = JSON.stringify({ type: 'online', list });
    for (const ws of this.socks.values()) { try { ws.send(msg); } catch {} }
  }
}
