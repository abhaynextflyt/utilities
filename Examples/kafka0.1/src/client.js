// ============================================================
// SCRIPT 5 of 5  ·  CLIENT  (client side)
// ------------------------------------------------------------
// Connects to the broker and prints live telemetry as it lands.
// Notice it keeps receiving messages even while MongoDB is dead,
// because realtime delivery does not depend on the database.
// Auto-reconnects if the broker restarts.
// ============================================================
const WebSocket = require('ws');
const cfg = require('./config');

const url = `ws://localhost:${cfg.BROKER_WS_PORT}`;

function connect() {
  const ws = new WebSocket(url);

  ws.on('open', () => console.log(`[client] connected to broker ${url} — live telemetry:`));

  ws.on('message', (raw) => {
    try {
      const { offset, event } = JSON.parse(raw);
      console.log(`[client] #${offset}  ${event.droneId}  ${event.type.padEnd(16)}  batt=${event.battery}%`);
    } catch {
      console.log('[client]', raw.toString());
    }
  });

  ws.on('close', () => {
    console.log('[client] disconnected — retrying in 2s');
    setTimeout(connect, 2000);
  });
  ws.on('error', () => {}); // handled by close
}

connect();