// ============================================================
// SCRIPT 1 of 5  ·  GENERATOR  (generation side)
// ------------------------------------------------------------
// Pretends to be a fleet of drones. Emits telemetry FAST and
// POSTs each event to the producer. It never touches the DB and
// never blocks on anything downstream — just like a real drone
// radio that keeps transmitting no matter what the ground does.
// ============================================================
const http = require('http');
const cfg = require('./config');

const drones = ['drone_101', 'drone_102', 'drone_103', 'drone_104'];
const types = [
  'MISSION_STARTED', 'REACHED_WAYPOINT', 'BATTERY_LOW', 'GPS_LOCK',
  'ALTITUDE_HOLD', 'GEOFENCE_BREACH', 'RTL_ENGAGED', 'MISSION_COMPLETE',
];

let count = 0;

function send(payload) {
  const body = JSON.stringify(payload);
  const req = http.request(
    {
      hostname: 'localhost',
      port: cfg.PRODUCER_PORT,
      path: '/publish',
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
    },
    (res) => res.resume() // drain the response, we only care that it was accepted
  );
  req.on('error', (e) => console.error(`[gen] producer unreachable (${e.code}) — is 2-producer.js running?`));
  req.write(body);
  req.end();
}

setInterval(() => {
  const event = {
    droneId: drones[(Math.random() * drones.length) | 0],
    type: types[(Math.random() * types.length) | 0],
    lat: +(28.6 + Math.random() * 0.1).toFixed(6),
    lon: +(77.2 + Math.random() * 0.1).toFixed(6),
    battery: (Math.random() * 100) | 0,
    ts: Date.now(),
  };
  send(event);
  if (++count % 20 === 0) console.log(`[gen] emitted ${count} telemetry events`);
}, cfg.GEN_INTERVAL_MS);

console.log(`[gen] streaming telemetry -> producer :${cfg.PRODUCER_PORT} every ${cfg.GEN_INTERVAL_MS}ms`);