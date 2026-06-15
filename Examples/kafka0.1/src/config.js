// Shared configuration for every script in the simulation.
// Override any value with an env var, e.g. CONSUMER_DELAY_MS=500 node with-kafka/4-consumer.js
const path = require('path');

module.exports = {
  // producer ingests telemetry over HTTP here
  PRODUCER_PORT: Number(process.env.PRODUCER_PORT || 4001),

  // broker (main server) pushes live telemetry to clients over WebSocket here
  BROKER_WS_PORT: Number(process.env.BROKER_WS_PORT || 4002),

  // the append-only log — the buffer that decouples everything
  LOG_FILE: path.join(__dirname, 'data', 'drone_events.log'),

  // MongoDB — the "slow" downstream store that we will crash on purpose
  MONGO_URL: process.env.MONGO_URL || 'mongodb://localhost:27017',
  DB_NAME: process.env.DB_NAME || 'drone_sim',

  // generator runs FAST, consumer runs SLOW — on purpose, so the log fills up
  GEN_INTERVAL_MS: Number(process.env.GEN_INTERVAL_MS || 50),    // ~20 events/sec
  CONSUMER_DELAY_MS: Number(process.env.CONSUMER_DELAY_MS || 200), // ~5 writes/sec
};