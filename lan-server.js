const https=require('https'),fs=require('fs'),express=require('express'),{WebSocketServer}=require('ws');
const app=express();
app.use(express.static('public'));
const server=https.createServer({key:fs.readFileSync('key.pem'),cert:fs.readFileSync('cert.pem')},app);
const wss=new WebSocketServer({server,path:'/ws'});
const socks=new Map(),wsNo=new Map(),clean=n=>String(n||'').replace(/\D/g,'');
const send=(ws,o)=>{try{ws.send(JSON.stringify(o))}catch{}};
function broadcast(){
  const list=[...socks.keys()],msg=JSON.stringify({type:'online',list});
  for(const ws of socks.values())try{ws.send(msg)}catch{}
}
function cleanup(ws){
  const no=wsNo.get(ws);
  if(no&&socks.get(no)===ws)socks.delete(no);
  wsNo.delete(ws);broadcast();
}
wss.on('connection',ws=>{
  console.log('[+] client');
  send(ws,{type:'welcome',t:Date.now()});
  ws.on('message',raw=>{
    let m;try{m=JSON.parse(raw)}catch{return}
    const t=m.type;
    if(t==='ping')return send(ws,{type:'pong'});
    if(t==='register'){
      const no=clean(m.no);
      if(!/^(0|62)?8\d{7,13}$/.test(no))return send(ws,{type:'error',msg:'Nomor tidak valid'});
      const old=socks.get(no);
      if(old&&old!==ws){try{old.send(JSON.stringify({type:'force-logout'}));old.close()}catch{}}
      socks.set(no,ws);wsNo.set(ws,no);
      console.log('[REG]',no);
      send(ws,{type:'registered',no});broadcast();
      return;
    }
    if(t==='call-user'){
      const from=wsNo.get(ws),to=clean(m.to),tg=socks.get(to);
      if(!from)return send(ws,{type:'error',msg:'Daftar dulu'});
      if(!tg)return send(ws,{type:'call-error',msg:`Nomor ${to} tidak online`});
      send(tg,{type:'incoming-call',from,offer:m.sdp});
      return;
    }
    if(t==='reject-call'||t==='end-call'){
      const tg=socks.get(clean(m.to));
      const map={'reject-call':'call-rejected','end-call':'call-ended'};
      if(tg)send(tg,{type:map[t]});
      return;
    }
    if(t==='offer'||t==='answer'||t==='ice-candidate'){
      const from=wsNo.get(ws),tg=socks.get(clean(m.to));
      if(tg)send(tg,{type:t,from,sdp:m.sdp,candidate:m.candidate});
      return;
    }
    if(t==='unregister')cleanup(ws);
  });
  ws.on('close',()=>cleanup(ws));
  ws.on('error',()=>cleanup(ws));
});
const PORT=3000;
server.listen(PORT,'0.0.0.0',()=>{
  console.log(`\n📡 LAN Walkie siap di:`);
  console.log(`   https://localhost:${PORT}       (HP ini)`);
  console.log(`   https://192.168.43.1:${PORT}    (HP lain via hotspot)\n`);
});
