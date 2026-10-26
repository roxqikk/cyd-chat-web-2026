import asyncio, hmac, json, os, re, time
from collections import defaultdict, deque
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
app=FastAPI()
peers=defaultdict(set)
tries=defaultdict(deque)
try: rooms=json.loads(os.environ.get("ROOM_KEYS","{}"))
except ValueError: rooms={}
if not isinstance(rooms,dict): rooms={}
origins=set(os.environ.get("ALLOWED_ORIGINS","https://roxqikk.github.io").split(","))
@app.get("/")
def root(): return {"status":"ok","app":"CYD Chat 2.0"}
@app.get("/health")
def health(): return {"status":"ok"}
@app.websocket("/ws")
async def chat(ws:WebSocket):
    origin=ws.headers.get("origin")
    if origin and origin not in origins:
        await ws.close(code=1008);return
    await ws.accept()
    room=None
    try:
        raw=await asyncio.wait_for(ws.receive_text(),15)
        if len(raw)>2048: return
        data=json.loads(raw)
        ip=ws.client.host if ws.client else "unknown"
        q=tries[ip];now=time.monotonic()
        while q and now-q[0]>300:q.popleft()
        if len(q)>=10:
            await ws.send_json({"type":"error","message":"Слишком много попыток"});return
        q.append(now)
        candidate=data.get("room","")
        password=data.get("password","")
        nick=data.get("nick","")
        valid=isinstance(candidate,str) and re.fullmatch(r"[A-Za-z0-9_-]{4,48}",candidate) and isinstance(password,str) and isinstance(nick,str) and 1<=len(nick.strip())<=24
        secret=rooms.get(candidate,"") if valid else ""
        if not valid or not isinstance(secret,str) or len(secret)<8 or not hmac.compare_digest(secret,password):
            await ws.send_json({"type":"error","message":"Неверная комната или пароль"});return
        room=candidate;nick=nick.strip()
        if len(peers[room])>=30:
            await ws.send_json({"type":"error","message":"Комната заполнена"});return
        peers[room].add(ws)
        await ws.send_json({"type":"joined"})
        sent=deque()
        while True:
            raw=await ws.receive_text()
            if len(raw.encode())>4096:break
            try:data=json.loads(raw)
            except ValueError:continue
            if not isinstance(data,dict) or data.get("type")!="message" or not isinstance(data.get("text"),str):continue
            msg=data["text"].strip()
            if not 1<=len(msg)<=1000:continue
            now=time.monotonic()
            while sent and now-sent[0]>10:sent.popleft()
            if len(sent)>=10:
                await ws.send_json({"type":"error","message":"Слишком много сообщений"});continue
            sent.append(now)
            for peer in list(peers[room]):
                if peer is not ws:
                    try:await peer.send_json({"type":"message","nick":nick,"text":msg})
                    except Exception:pass
    except (WebSocketDisconnect,asyncio.TimeoutError,ValueError,TypeError,AttributeError):
        pass
    finally:
        if room:
            peers[room].discard(ws)
            if not peers[room]:peers.pop(room,None)
