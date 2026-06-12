// ============================================================
// NO-KAFKA  ·  NAIVE  (the thing you are comparing against)
// ------------------------------------------------------------
// Generates the SAME telemetry but writes it DIRECTLY to MongoDB
// with no log buffer in between. There is nowhere to hold an event
// while the DB is unavailable.
//
// Kill mongod while this runs and watch LOST climb. Those events
// are gone forever — there was no log to replay them from. Restart
// mongod and only new events get stored; the gap is permanent.
//
// This is the failure the whole Kafka design exists to prevent.
// ============================================================
const { MongoClient } = require('mongodb');
const cfg = require('./config');

const drones = ['drone_101', 'drone_102', 'drone_103', 'drone_104'];
const types = [
  'MISSION_STARTED', 'REACHED_WAYPOINT', 'BATTERY_LOW', 'GPS_LOCK',
  'ALTITUDE_HOLD', 'GEOFENCE_BREACH', 'RTL_ENGAGED', 'MISSION_COMPLETE',
];

async function main() {
  const client = new MongoClient(cfg.MONGO_URL, { serverSelectionTimeoutMS: 1500 });
  await client.connect().catch(() => console.error('[naive] mongo not up yet — start mongod'));
  const col = client.db(cfg.DB_NAME).collection('telemetry_naive');

  let produced = 0, stored = 0, lost = 0;
  console.log('[naive] writing telemetry DIRECTLY to mongo — no buffer, no safety net');

  setInterval(async () => {
    const event = {
      droneId: drones[(Math.random() * drones.length) | 0],
      type: types[(Math.random() * types.length) | 0],
      battery: (Math.random() * 100) | 0,
      ts: Date.now(),
    };
    produced++;
    try {
      await col.insertOne(event); // if mongo is down this throws and the event vanishes
      stored++;
    } catch {
      lost++; // nothing buffered it — unrecoverable
    }
    if (produced % 20 === 0) {
      const tag = lost > 0 ? '   <-- DATA LOSS' : '';
      console.log(`[naive] produced=${produced}  stored=${stored}  LOST=${lost}${tag}`);
    }
  }, cfg.GEN_INTERVAL_MS);
}

main();