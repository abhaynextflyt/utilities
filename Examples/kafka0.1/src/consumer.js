// ============================================================
// SCRIPT 4 of 5  ·  CONSUMER  (server side, drain to DB)
// ------------------------------------------------------------
// Reads the log from its last committed offset and writes each
// event into MongoDB SLOWLY (simulating a slow downstream). It
// commits its offset after every write.
//
// THE KEY BEHAVIOUR: if MongoDB crashes, the consumer does NOT
// advance its offset. It backs off and retries. When Mongo comes
// back it resumes exactly where it left off — zero data loss,
// because the log already safely holds everything the producer
// wrote while Mongo was down.
//
// Writes are idempotent (_id = offset) so a retried write can
// never create a duplicate.
// ============================================================
const fs = require('fs');
const { MongoClient } = require('mongodb');
const cfg = require('./config');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const OID = 'drone-consumer';

async function main() {
  const client = new MongoClient(cfg.MONGO_URL, { serverSelectionTimeoutMS: 2000 });

  // wait for Mongo to be available before starting
  for (;;) {
    try { await client.connect(); break; }
    catch { console.error('[consumer] waiting for MongoDB...'); await sleep(2000); }
  }

  const db = client.db(cfg.DB_NAME);
  const telemetry = db.collection('telemetry');
  const offsets = db.collection('consumer_offsets');

  const saved = await offsets.findOne({ _id: OID });
  let lastOffset = saved ? saved.offset : -1;
  console.log(`[consumer] resuming from offset ${lastOffset + 1}`);

  for (;;) {
    // read the log (the buffer) and process anything past our offset
    let lines = [];
    try {
      if (fs.existsSync(cfg.LOG_FILE)) {
        lines = fs.readFileSync(cfg.LOG_FILE, 'utf8').split('\n').filter(Boolean);
      }
    } catch { /* transient read error, retry next loop */ }

    let progressed = false;
    for (const line of lines) {
      const tab = line.indexOf('\t');
      const offset = Number(line.slice(0, tab));
      if (offset <= lastOffset) continue;

      const event = JSON.parse(line.slice(tab + 1));
      try {
        // idempotent upsert keyed by offset, then commit the offset
        await telemetry.updateOne({ _id: offset }, { $set: { ...event, offset } }, { upsert: true });
        await offsets.updateOne({ _id: OID }, { $set: { offset } }, { upsert: true });
        lastOffset = offset;
        progressed = true;
        console.log(`[consumer] -> mongo  offset ${offset}  ${event.droneId} ${event.type}`);
        await sleep(cfg.CONSUMER_DELAY_MS); // deliberately slow
      } catch (e) {
        // Mongo is down/unreachable. DO NOT advance the offset.
        console.error(`[consumer] mongo write failed at offset ${offset} — backing off, NO DATA LOST (${e.message})`);
        await sleep(2000);
        break; // restart the loop and retry the SAME offset
      }
    }

    if (!progressed) {
      const behind = lines.length ? Number(lines[lines.length - 1].split('\t')[0]) - lastOffset : 0;
      if (behind > 0) console.log(`[consumer] caught up to head, lag=${behind}`);
      await sleep(500); // nothing new (or still backing off) — wait
    }
  }
}

main().catch((e) => { console.error(e); process.exit(1); });