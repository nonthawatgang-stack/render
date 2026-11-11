// Monster Crafter — เซิร์ฟเวอร์ต่อห้องเล่นกับเพื่อน (relay)
// ไม่ได้รันเกม: แค่สร้างห้อง (รหัส 5 ตัว) แล้วส่งข้อความระหว่างเครื่องในห้องเดียวกัน
// ข้อความจากเกม: {op:"create",name} · {op:"join",code,name} · {op:"send",to:"all"|"host"|<id>,data} · {op:"leave"}
// ข้อความถึงเกม: {op:"created"|"joined",code,id,host,members} · {op:"members",members,host} · {op:"msg",from,data} · {op:"closed",why} · {op:"error",why}
const http = require("http");
const crypto = require("crypto");

const PORT = process.env.PORT || 8080;
const MAX = 4;
const rooms = new Map();   // code -> {host, members: Map(id -> {ws, name})}
let nextId = 1;

const server = http.createServer((req, res) => {
  res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
  res.end("Monster Crafter relay OK · rooms " + rooms.size + "\n");
});
function code5() {
  const A = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";   // ไม่มี O/0/I/1 อ่านสับสน
  let c;
  do { c = Array.from({ length: 5 }, () => A[Math.floor(Math.random() * A.length)]).join(""); } while (rooms.has(c));
  return c;
}
function send(ws, o) { if (ws.open) ws.sendText(JSON.stringify(o)); }
function memberList(room) { return [...room.members.entries()].map(([id, m]) => ({ id, name: m.name })); }
function broadcastMembers(room) {
  const o = { op: "members", members: memberList(room), host: room.host };
  for (const m of room.members.values()) send(m.ws, o);
}
function leave(ws) {
  const code = ws.room; if (!code) return;
  const room = rooms.get(code); ws.room = null;
  if (!room) return;
  room.members.delete(ws.id);
  if (ws.id === room.host || room.members.size === 0) {
    for (const m of room.members.values()) { send(m.ws, { op: "closed", why: "หัวห้องออกจากห้องแล้ว" }); m.ws.room = null; }
    rooms.delete(code);
  } else broadcastMembers(room);
}

// ---------------------------------------------------------------- WebSocket แบบเขียนเอง (ไม่ต้องติดตั้งแพ็กเกจเพิ่ม)
const clients = new Set();
const MAXLEN = 4 * 1024 * 1024;
class Sock {
  constructor(sock) { this.sock = sock; this.open = true; this.buf = Buffer.alloc(0); this.frag = []; this.handlers = {}; this.isAlive = true; }
  on(ev, fn) { this.handlers[ev] = fn; }
  emit(ev, x) { if (this.handlers[ev]) this.handlers[ev](x); }
  frame(op, payload) {
    const n = payload.length;
    let head;
    if (n < 126) head = Buffer.from([0x80 | op, n]);
    else if (n < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(n, 2); }
    else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(n), 2); }
    try { this.sock.write(Buffer.concat([head, payload])); } catch (e) { this.close(); }
  }
  sendText(t) { if (this.open) this.frame(0x1, Buffer.from(t, "utf8")); }
  ping() { if (this.open) this.frame(0x9, Buffer.alloc(0)); }
  close() { if (!this.open) return; this.open = false; try { this.frame(0x8, Buffer.alloc(0)); this.sock.end(); } catch (e) {} clients.delete(this); this.emit("close"); }
  feed(chunk) {
    this.buf = Buffer.concat([this.buf, chunk]);
    while (this.buf.length >= 2) {
      const b0 = this.buf[0], b1 = this.buf[1];
      const fin = (b0 & 0x80) !== 0, op = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.buf.length < 10) return; len = Number(this.buf.readBigUInt64BE(2)); off = 10; }
      if (len > MAXLEN) return this.close();
      const need = off + (masked ? 4 : 0) + len;
      if (this.buf.length < need) return;
      let data = this.buf.subarray(off + (masked ? 4 : 0), need);
      if (masked) { const m = this.buf.subarray(off, off + 4); data = Buffer.from(data); for (let i = 0; i < data.length; i++) data[i] ^= m[i & 3]; }
      this.buf = this.buf.subarray(need);
      if (op === 0x8) return this.close();
      if (op === 0x9) { this.frame(0xA, data); continue; }
      if (op === 0xA) { this.isAlive = true; continue; }
      if (op === 0x0 || op === 0x1 || op === 0x2) {
        this.frag.push(Buffer.from(data));
        if (fin) { const all = Buffer.concat(this.frag); this.frag = []; this.emit("message", all.toString("utf8")); }
      }
    }
  }
}
server.on("upgrade", (req, sock) => {
  const key = req.headers["sec-websocket-key"];
  if (!key) return sock.destroy();
  const acc = crypto.createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
  sock.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + acc + "\r\n\r\n");
  sock.setNoDelay(true);
  const ws = new Sock(sock);
  clients.add(ws);
  sock.on("data", (c) => ws.feed(c));
  sock.on("close", () => ws.close());
  sock.on("error", () => ws.close());
  onConnection(ws);
});

function onConnection(ws) {
  ws.id = nextId++;
  ws.isAlive = true;
    ws.on("message", (raw) => {
    let o; try { o = JSON.parse(raw); } catch { return; }
    const name = String(o.name || "นักล่า").slice(0, 24);
    if (o.op === "create") {
      leave(ws);
      const code = code5();
      const room = { host: ws.id, members: new Map([[ws.id, { ws, name }]]) };
      rooms.set(code, room); ws.room = code;
      send(ws, { op: "created", code, id: ws.id, host: ws.id, members: memberList(room) });
    } else if (o.op === "join") {
      const code = String(o.code || "").toUpperCase().trim();
      const room = rooms.get(code);
      if (!room) return send(ws, { op: "error", why: "ไม่พบห้องรหัสนี้" });
      if (room.members.size >= MAX) return send(ws, { op: "error", why: "ห้องเต็มแล้ว (4 คน)" });
      if (room.started) return send(ws, { op: "error", why: "ห้องนี้กำลังล่าอยู่" });
      leave(ws);
      room.members.set(ws.id, { ws, name }); ws.room = code;
      send(ws, { op: "joined", code, id: ws.id, host: room.host, members: memberList(room) });
      broadcastMembers(room);
    } else if (o.op === "state") {   // หัวห้องแจ้งว่าเริ่มล่า/กลับห้อง (กันคนเข้ากลางเควส)
      const room = rooms.get(ws.room);
      if (room && room.host === ws.id) room.started = !!o.started;
    } else if (o.op === "send") {
      const room = rooms.get(ws.room); if (!room) return;
      const out = { op: "msg", from: ws.id, data: o.data };
      if (o.to === "all") { for (const [id, m] of room.members) if (id !== ws.id) send(m.ws, out); }
      else if (o.to === "host") { const h = room.members.get(room.host); if (h) send(h.ws, out); }
      else { const m = room.members.get(Number(o.to)); if (m) send(m.ws, out); }
    } else if (o.op === "leave") leave(ws);
    else if (o.op === "ping") send(ws, { op: "pong" });
  });
  ws.on("close", () => leave(ws));
}

// ตัดการเชื่อมต่อที่ค้าง (เน็ตหลุดไม่บอก)
setInterval(() => {
  for (const ws of clients) {
    if (!ws.isAlive) { ws.close(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 15000);

server.listen(PORT, () => console.log("Monster Crafter relay on :" + PORT));
