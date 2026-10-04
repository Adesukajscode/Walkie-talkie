const CORS={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST,OPTIONS','Access-Control-Allow-Headers':'*'};
export default{
  async fetch(req,env){
    const u=new URL(req.url);
    try{
      if(req.method==='OPTIONS')return new Response(null,{headers:CORS});
      const doFetch=(p)=>env.ROOM.get(env.ROOM.idFromName('global')).fetch('https://do'+p);
      if(u.pathname==='/health')return Response.json({ok:1,t:Date.now()},{headers:CORS});
      if(u.pathname==='/prewarm'){try{await doFetch('/prewarm')}catch(e){}return new Response('warmed',{headers:CORS})}
      if(u.pathname==='/debug'){try{const r=await doFetch('/debug');return new Response(JSON.stringify({do:'ok',body:await r.text()}),{headers:{'content-type':'application/json',...CORS}})}catch(e){return new Response(JSON.stringify({do:'fail',error:e.message}),{status:500,headers:{'content-type':'application/json',...CORS}})}}
      if(u.pathname==='/users'){const r=await doFetch('/users');return new Response(await r.text(),{headers:{'content-type':'application/json',...CORS}})}
      if(u.pathname==='/ws')return env.ROOM.get(env.ROOM.idFromName('global')).fetch(req);
      if(env.ASSETS)return env.ASSETS.fetch(req);
      return new Response('Not found',{status:404});
    }catch(e){return new Response(JSON.stringify({error:e.message}),{status:500,headers:{'content-type':'application/json',...CORS}})}
  }
};
export class Room{
  constructor(s,e){
    this.state=s;this.env=e;
    this.socks=new Map();
    this.wsNo=new Map();
    this.locs=new Map();
    this.calls=new Map();
  }
  async fetch(req){
    const u=new URL(req.url);
    if(u.pathname==='/prewarm')return new Response('ok');
    if(u.pathname==='/debug')return Response.json({users:[...this.socks.keys()],socks:this.socks.size});
    if(u.pathname==='/users')return Response.json([...this.socks.keys()]);
    const up=(req.headers.get('Upgrade')||'').toLowerCase();
    if(up!=='websocket')return new Response('Expected WS',{status:426});
    let pair;try{pair=new WebSocketPair()}catch(e){return new Response('WS fail: '+e.message,{status:500})}
    const[c,s]=Object.values(pair);s.accept();
    this.send(s,{type:'welcome',t:Date.now()});
    s.addEventListener('message',ev=>{try{this.onMsg(s,ev.data)}catch(e){console.log('err',e.message)}});
    s.addEventListener('close',()=>this.cleanup(s));
    s.addEventListener('error',()=>this.cleanup(s));
    return new Response(null,{status:101,webSocket:c});
  }
  onMsg(ws,raw){
    let m;try{m=JSON.parse(raw)}catch{return}
    const t=m.type,cl=n=>String(n||'').replace(/\D/g,'');
    if(t==='ping')return this.send(ws,{type:'pong'});
    if(t==='register'){
      const no=cl(m.no);
      if(!/^(0|62)?8\d{7,13}$/.test(no))return this.send(ws,{type:'error',msg:'Nomor tidak valid'});
      const old=this.socks.get(no);
      if(old&&old!==ws){try{old.send(JSON.stringify({type:'force-logout'}));old.close()}catch{}}
      this.socks.set(no,ws);this.wsNo.set(ws,no);
      this.send(ws,{type:'registered',no});this.broadcastOnline();
      console.log('REG',no,'total',this.socks.size);
      return;
    }
    if(t==='location'){
      const no=this.wsNo.get(ws);
      if(no){
        const loc={lat:+m.lat,lng:+m.lng,acc:+m.acc||0};
        this.locs.set(no,loc);
        const peerNo=this.calls.get(no);
        if(peerNo){const peer=this.socks.get(peerNo);if(peer)this.send(peer,{type:'peer-location',lat:loc.lat,lng:loc.lng,acc:loc.acc})}
      }
      return;
    }
    if(t==='call-user'){
      const from=this.wsNo.get(ws),to=cl(m.to),tg=this.socks.get(to);
      if(!from)return this.send(ws,{type:'error',msg:'Daftar dulu'});
      if(!tg)return this.send(ws,{type:'call-error',msg:'Nomor '+to+' tidak online'});
      this.calls.set(from,to);this.calls.set(to,from);
      const fromLoc=this.locs.get(from),toLoc=this.locs.get(to);
      this.send(tg,{type:'incoming-call',from,offer:m.sdp,peerLoc:fromLoc||null});
      if(toLoc)this.send(ws,{type:'peer-location',lat:toLoc.lat,lng:toLoc.lng,acc:toLoc.acc});
      return;
    }
    if(t==='reject-call'||t==='end-call'){
      const me=this.wsNo.get(ws),to=cl(m.to),tg=this.socks.get(to);
      const map={'reject-call':'call-rejected','end-call':'call-ended'};
      if(tg)this.send(tg,{type:map[t]});
      if(me)this.calls.delete(me);
      if(to)this.calls.delete(to);
      return;
    }
    if(t==='offer'||t==='answer'||t==='ice-candidate'){
      const from=this.wsNo.get(ws),tg=this.socks.get(cl(m.to));
      if(tg)this.send(tg,{type:t,from,sdp:m.sdp,candidate:m.candidate});
      return;
    }
    if(t==='unregister')this.cleanup(ws);
  }
  cleanup(ws){
    const no=this.wsNo.get(ws);
    if(no&&this.socks.get(no)===ws){
      this.socks.delete(no);this.locs.delete(no);
      const p=this.calls.get(no);
      if(p)this.calls.delete(p);
      this.calls.delete(no);
    }
    this.wsNo.delete(ws);this.broadcastOnline();
  }
  send(ws,o){try{ws.send(JSON.stringify(o))}catch{}}
  broadcastOnline(){const list=[...this.socks.keys()],msg=JSON.stringify({type:'online',list});for(const ws of this.socks.values())try{ws.send(msg)}catch{}}
}
