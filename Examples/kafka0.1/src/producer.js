// ============================================================
// SCRIPT 2 of 5  ·  PRODUCER  (server side, write path)
// ------------------------------------------------------------
// Receives events over HTTP and APPENDS them to one log file,
// assigning each a monotonically increasing offset. It is the
// ONLY writer of the log (single-writer = no races). Crucially
// it NEVER talks to MongoDB, so a DB crash cannot slow it down
// or lose its data. This is the buffer that saves you.
//
//   log line format:  <offset>\t<json>\n
// ============================================================
const http = require('http');
const fs = require('fs');
const path = require('path');
const cfg = require('./config');

fs.mkdirSync(path.dirname(cfg.LOG_FILE), { recursive: true });

// On startup, recover the next offset by reading the last line.
function recover() {
  if (!fs.existsSync(cfg.LOG_FILE)) return 0;
  const lines = fs.readFileSync(cfg.LOG_FILE, 'utf8').split('\n').filter(Boolean);
  if (!lines.length) return 0;
  return Number(lines[lines.length - 1].split('\t')[0]) + 1;
}

let nextOffset = recover();
const fd = fs.openSync(cfg.LOG_FILE, 'a'); // append mode

const server = http.createServer((req, res) => {
  if (req.method === 'POST' && req.url === '/publish') {
    let body = '';
    req.on('data', (c) => (body += c));
    req.on('end', () => {
      try {
        const event = JSON.parse(body);
        const offset = nextOffset++;
        // append-only write. Reaches the OS immediately; real durability
        // would add periodic fsync + replication (see the guide, ch.09).
        fs.writeSync(fd, `${offset}\t${JSON.stringify(event)}\n`);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ offset }));
        if (offset % 20 === 0) console.log(`[producer] appended up to offset ${offset}`);
      } catch (e) {
        res.writeHead(400);
        res.end('bad json');
      }
    });
  } else {
    res.writeHead(404);
    res.end();
  }
});

server.listen(cfg.PRODUCER_PORT, () =>
  console.log(`[producer] ingesting on :${cfg.PRODUCER_PORT}  log=${cfg.LOG_FILE}  nextOffset=${nextOffset}`)
);