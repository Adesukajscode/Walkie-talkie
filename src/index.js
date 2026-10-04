const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': '*',
};

export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    try {
      if (req.method === 'OPTIONS') return new Response(null, { headers: CORS });

      if (u.pathname === '/health') {
        return new Response(JSON.stringify({ ok: 1, t: Date.now() }), {
          headers: { 'content-type': 'application/json', ...CORS },
        });
      }

      if (u.pathname === '/debug') {
        try {
          const r = await env.ROOM.get(env.ROOM.idFromName('global')).fetch('https://do/debug');
          const body = await r.text();
          return new Response(JSON.stringify({ do: 'ok', body }), {
            headers: { 'content-type': 'application/json', ...CORS },
          });
        } catch (e) {
          return new Response(JSON.stringify({ do: 'fail', error: e.message }), {
            status: 500,
            headers: { 'content-type': 'application/json', ...CORS },
          });
        }
      }

      if (u.pathname === '/users') {
        const r = await env.ROOM.get(env.ROOM.idFromName('global')).fetch('https://do/users');
        return new Response(await r.text(), {
          headers: { 'content-type': 'application/json', ...CORS },
        });
      }

      if (u.pathname === '/ws') {
        const id = env.ROOM.idFromName('global');
        const stub = env.ROOM.get(id);
        return stub.fetch(req);
      }

      if (env.ASSETS) return env.ASSETS.fetch(req);
      return new Response('Not found', { status: 404 });
    } catch (e) {
      return new Response(JSON.stringify({ error: e.message, stack: e.stack }), {
        status: 500,
        headers: { 'content-type': 'application/json', ...CORS },
      });
    }
  },
};

export class Room {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.socks = new Map();
    this.wsNo = new Map();
  }

  async fetch(req) {
    const u = new URL(req.url);

    if (u.pathname === '/debug') {
      return new Response(JSON.stringify({
        users: [...this.socks.keys()],
        socks: this.socks.size,
        wsNo: this.wsNo.size,
      }), { headers: { 'content-type': 'application/json' } });
    }

    if (u.pathname === '/users') {
      return new Response(JSON.stringify([...this.socks.keys()]), {
        headers: { 'content-type': 'application/json' },
      });
    }

    const upgrade = (req.headers.get('Upgrade') || '').toLowerCase();
    if (upgrade !== 'websocket') {
      return new Response('Expected WebSocket, got Upgrade=' + upgrade, { status: 426 });
    }

    let pair;
    try {
      pair = new WebSocketPair();
    } catch (e) {
      return new Response('WebSocketPair fail: ' + e.message, { status: 500 });
    }

    const [client, server] = Object.values(pair);
    server.accept();

    server.addEventListener('message', (e) => {
      try { this.onMsg(server, e.data); }
      catch (err) { console.log('onMsg err', err.message); }
    });
    server.addEventListener('close', () => this.cleanup(server));
    server.addEventListener('error', () => this.cleanup(server));

    this.send(server, { type: 'welcome', t: Date.now() });

    return new Response(null, { status: 101, webSocket: client });
  }

  onMsg(ws, raw) {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    const t = m.type;
    const cl = (n) => String(n || '').replace(/\D/g, '');
    console.log('MSG', t, 'from', this.wsNo.get(ws) || '?');

    if (t === 'ping') { this.send(ws, { type: 'pong', t: Date.now() }); return; }

    if (t === 'register') {
      const no = cl(m.no);
      if (!/^(0|62)?8\d{7,13}$/.test(no)) {
        return this.send(ws, { type: 'error', msg: 'Nomor tidak valid' });
      }
      const old = this.socks.get(no);
      if (old && old !== ws) {
        try { old.send(JSON.stringify({ type: 'force-logout' })); old.close(); } catch {}
      }
      this.socks.set(no, ws);
      this.wsNo.set(ws, no);
      this.send(ws, { type: 'registered', no });
      this.broadcastOnline();
      return;
    }

    if (t === 'call-user') {
      const from = this.wsNo.get(ws);
      const to = cl(m.to);
      if (!from) return this.send(ws, { type: 'error', msg: 'Daftar dulu' });
      const tg = this.socks.get(to);
      if (!tg) return this.send(ws, { type: 'call-error', msg: `Nomor ${to} tidak online` });
      this.send(tg, { type: 'incoming-call', from });
      return;
    }

    if (t === 'accept-call' || t === 'reject-call' || t === 'end-call') {
      const tg = this.socks.get(cl(m.to));
      const map = { 'accept-call': 'call-accepted', 'reject-call': 'call-rejected', 'end-call': 'call-ended' };
      if (tg) this.send(tg, { type: map[t] });
      return;
    }

    if (t === 'offer' || t === 'answer' || t === 'ice-candidate') {
      const from = this.wsNo.get(ws);
      const tg = this.socks.get(cl(m.to));
      if (tg) this.send(tg, { type: t, from, sdp: m.sdp, candidate: m.candidate });
      return;
    }

    if (t === 'unregister') {
      this.cleanup(ws);
    }
  }

  cleanup(ws) {
    const no = this.wsNo.get(ws);
    if (no && this.socks.get(no) === ws) this.socks.delete(no);
    this.wsNo.delete(ws);
    this.broadcastOnline();
  }

  send(ws, o) {
    try { ws.send(JSON.stringify(o)); } catch {}
  }

  broadcastOnline() {
    const list = [...this.socks.keys()];
    const msg = JSON.stringify({ type: 'online', list });
    for (const ws of this.socks.values()) {
      try { ws.send(msg); } catch {}
    }
  }
}
