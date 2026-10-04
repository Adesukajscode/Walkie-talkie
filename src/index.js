export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    const upgrade = (req.headers.get('Upgrade') || '').toLowerCase();

    // PRIORITAS 1: WebSocket — langsung ke DO, tidak lewat assets
    if (upgrade === 'websocket') {
      const id = env.ROOM.idFromName('global');
      return env.ROOM.get(id).fetch(req);
    }

    // Health & debug
    if (u.pathname === '/health') {
      return Response.json({ ok: 1, t: Date.now() });
    }
    if (u.pathname === '/users') {
      const r = await env.ROOM.get(env.ROOM.idFromName('global')).fetch('https://do/users');
      return new Response(await r.text(), { headers: { 'content-type': 'application/json' } });
    }
    if (u.pathname === '/prewarm') {
      try { await env.ROOM.get(env.ROOM.idFromName('global')).fetch('https://do/prewarm'); } catch (e) {}
      return new Response('warmed');
    }

    // Fallback ke assets
    return env.ASSETS.fetch(req);
  }
};

export class Room {
  constructor(state, env) {
    this.socks = new Map();
    this.wsNo = new Map();
  }

  async fetch(req) {
    const u = new URL(req.url);

    if (u.pathname === '/users') {
      return Response.json([...this.socks.keys()]);
    }
    if (u.pathname === '/prewarm') {
      return new Response('ok');
    }

    const upgrade = (req.headers.get('Upgrade') || '').toLowerCase();
    if (upgrade !== 'websocket') {
      return new Response('Need WS, got: ' + upgrade, { status: 426 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();

    server.addEventListener('message', (e) => {
      try { this.onMsg(server, e.data); } catch (err) { console.log('msg err', err.message); }
    });
    server.addEventListener('close', () => this.cleanup(server));
    server.addEventListener('error', () => this.cleanup(server));

    this.send(server, { type: 'welcome' });

    return new Response(null, { status: 101, webSocket: client });
  }

  onMsg(ws, raw) {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    const t = m.type;
    const cl = n => String(n || '').replace(/\D/g, '');

    if (t === 'ping') return this.send(ws, { type: 'pong' });

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
      console.log('REG', no, 'total:', this.socks.size);
      this.send(ws, { type: 'registered', no });
      this.broadcastOnline();
      return;
    }

    if(t==='match'){
      const me=this.wsNo.get(ws);
      if(!me)return this.send(ws,{type:'error',msg:'Daftar dulu'});
      const others=[...this.socks.keys()].filter(x=>x!==me);
      if(!others.length)return this.send(ws,{type:'error',msg:'Tidak ada user lain online'});
      const target=others[Math.floor(Math.random()*others.length)];
      this.send(ws,{type:'match-target',no:target});
      console.log('MATCH',me,'->',target);
      return;
    }
    if (t === 'call-user') {
      const from = this.wsNo.get(ws);
      const to = cl(m.to);
      const target = this.socks.get(to);
      if (!from) return this.send(ws, { type: 'error', msg: 'Daftar dulu' });
      if (!target) return this.send(ws, { type: 'call-error', msg: 'Nomor ' + to + ' tidak online' });
      this.send(target, { type: 'incoming-call', from, offer: m.sdp });
      return;
    }

    if (t === 'accept-call') {
      const target = this.socks.get(cl(m.to));
      if (target) this.send(target, { type: 'call-accepted' });
      return;
    }
    if (t === 'reject-call') {
      const target = this.socks.get(cl(m.to));
      if (target) this.send(target, { type: 'call-rejected' });
      return;
    }
    if (t === 'end-call') {
      const target = this.socks.get(cl(m.to));
      if (target) this.send(target, { type: 'call-ended' });
      return;
    }

    if (t === 'offer' || t === 'answer' || t === 'ice-candidate') {
      const from = this.wsNo.get(ws);
      const target = this.socks.get(cl(m.to));
      if (target) this.send(target, { type: t, from, sdp: m.sdp, candidate: m.candidate });
      return;
    }

    if (t === 'unregister') this.cleanup(ws);
  }

  cleanup(ws) {
    const no = this.wsNo.get(ws);
    if (no && this.socks.get(no) === ws) this.socks.delete(no);
    this.wsNo.delete(ws);
    this.broadcastOnline();
  }

  send(ws, obj) {
    try { ws.send(JSON.stringify(obj)); } catch {}
  }

  broadcastOnline() {
    const list = [...this.socks.keys()];
    const msg = JSON.stringify({ type: 'online', list });
    for (const ws of this.socks.values()) {
      try { ws.send(msg); } catch {}
    }
  }
}
