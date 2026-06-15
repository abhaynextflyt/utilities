// ============================================================
// SCRIPT 3 of 5  ·  BROKER / MAIN SERVER  (server side)
// ------------------------------------------------------------
// Tails the append-only log and pushes every NEW line to all
// connected WebSocket clients in realtime. This is the live
// delivery path: clients see telemetry the instant it lands in
// the log, with no database in the way at all.
//
// Producers (write) and consumers (drain to DB) are completely
// decoupled from this realtime fan-out — that is the whole point.
// ============================================================
const fs = require('fs');
const { WebSocketServer } = require('ws');
const cfg = require('./config');

const clients = new Set();
let bytePos = 0;   // how far we've tailed
let buffer = '';   // partial line carried between reads
let busy = false;

function broadcastLine(line) {
  if (!line) return;
  const tab = line.indexOf('\t');
  const offset = Number(line.slice(0, tab));
  const eventJson = line.slice(tab + 1); // already JSON
  const frame = `{"offset":${offset},"event":${eventJson}}`;
  for (const ws of clients) if (ws.readyState === 1) ws.send(frame);
}

// Poll the log for newly appended bytes, split into lines, broadcast.
function tail() {
  if (busy) return;
  busy = true;
  fs.stat(cfg.LOG_FILE, (err, st) => {
    if (err || st.size <= bytePos) {
      if (st && st.size < bytePos) bytePos = 0; // file truncated/rotated
      busy = false;
      return;
    }
    const stream = fs.createReadStream(cfg.LOG_FILE, { start: bytePos, end: st.size - 1, encoding: 'utf8' });
    stream.on('data', (d) => (buffer += d));
    stream.on('end', () => {
      bytePos = st.size;
      let nl;
      while ((nl = buffer.indexOf('\n')) >= 0) {
        broadcastLine(buffer.slice(0, nl));
        buffer = buffer.slice(nl + 1);
      }
      busy = false;
    });
    stream.on('error', () => (busy = false));
  });
}

const wss = new WebSocketServer({ port: cfg.BROKER_WS_PORT });
wss.on('connection', (ws) => {
  clients.add(ws);
  console.log(`[broker] client connected (${clients.size} live)`);
  ws.on('close', () => clients.delete(ws));
  ws.on('error', () => clients.delete(ws));
});

// Start at the current end of the log: clients receive messages from now on.
try { bytePos = fs.statSync(cfg.LOG_FILE).size; } catch { bytePos = 0; }
setInterval(tail, 150);

console.log(`[broker] main server up  WS :${cfg.BROKER_WS_PORT}  tailing ${cfg.LOG_FILE}`);