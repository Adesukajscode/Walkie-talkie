export default {
  async fetch(req, env) {
    const u = new URL(req.url);
    if (u.pathname === '/ws') {
      return env.ROOM.get(env.ROOM.idFromName('global')).fetch(req);
    }
    if (u.pathname === '/health') return Response.json({ ok: 1 });
    if (u.pathname === '/users') {
      const r = await env.ROOM.get(env.ROOM.idFromName('global')).fetch('https://x/users');
      return r;
    }
    return env.ASSETS.fetch(req);
  }
};

export class Room {
  constructor() {
    this.socks = new Map();   // no -> ws
    this.wsNo = new Map();    // ws -> no
  }
  async fetch(req) {
    const u = new URL(req.url);
    if (u.pathname === '/users') return Response.json([...this.socks.keys()]);
    if (req.headers.get('Upgrade') !== 'websocket')
      return new Response('Expected WS', { status: 426 });
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    server.addEventListener('message', e => this.onMsg(server, e.data));
    server.addEventListener('close', () => this.cleanup(server));
    server.addEventListener('error', () => this.cleanup(server));
    return new Response(null, { status: 101, webSocket: client });
  }
  onMsg(ws, raw) {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    const t = m.type, cl = n => String(n || '').replace(/\D/g, '');
    if (t === 'register') {
      const no = cl(m.no);
      if (!/^(0|62)?8\d{7,13}$/.test(no))
        return this.send(ws, { type: 'error', msg: 'Nomor tidak valid' });
      const old = this.socks.get(no);
      if (old && old !== ws) { try { old.send(JSON.stringify({ type: 'force-logout' })); old.close(); } catch {} }
      this.socks.set(no, ws);
      this.wsNo.set(ws, no);
      this.send(ws, { type: 'registered', no });
      this.broadcast();
    } else if (t === 'call-user') {
      const from = this.wsNo.get(ws), to = cl(m.to), tg = this.socks.get(to);
      if (!tg) return this.send(ws, { type: 'call-error', msg: `Nomor ${to} tidak online` });
      this.send(tg, { type: 'incoming-call', from });
    } else if (t === 'accept-call' || t === 'reject-call' || t === 'end-call') {
      const tg = this.socks.get(cl(m.to));
      const map = { 'accept-call': 'call-accepted', 'reject-call': 'call-rejected', 'end-call': 'call-ended' };
      if (tg) this.send(tg, { type: map[t] });
    } else if (t === 'offer' || t === 'answer' || t === 'ice-candidate') {
      const from = this.wsNo.get(ws), tg = this.socks.get(cl(m.to));
      if (tg) this.send(tg, { type: t, from, sdp: m.sdp, candidate: m.candidate });
    } else if (t === 'unregister') {
      this.cleanup(ws);
      this.broadcast();
    }
  }
  cleanup(ws) {
    const no = this.wsNo.get(ws);
    if (no && this.socks.get(no) === ws) this.socks.delete(no);
    this.wsNo.delete(ws);
    this.broadcast();
  }
  send(ws, o) { try { ws.send(JSON.stringify(o)); } catch {} }
  broadcast() {
    const list = [...this.socks.keys()];
    const msg = JSON.stringify({ type: 'online', list });
    for (const ws of this.socks.values()) try { ws.send(msg); } catch {}
  }
}
