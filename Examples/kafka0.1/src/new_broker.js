const fs = require('fs');
const { fork } = require('child_process');

const LOG_FILE = '.\\data\\drone_events.log';
const WINDOW_SIZE = 5;
const NUM_QUEUES = 7;
const queues = Array.from({ length: NUM_QUEUES }, () => []);
const workers = [];
const ackQueue = [];
let nextQueue = 0;
let bytePos = 0;
let buffer = '';
let busy = false;
for (let i = 0; i < NUM_QUEUES; i++) {
    const worker = fork('./worker.js');
    worker.busy = false;
    worker.on('message', msg => {
        if (msg.type !== 'ACK') return;
        worker.busy = false;
        ackQueue.push(msg);
        console.log(
            `ACK ${msg.startOffset}-${msg.endOffset} from Worker ${msg.workerPid}`
        );
    });
    workers.push(worker);
}
function tail() {
    if (busy) return;
    busy = true;
    fs.stat(LOG_FILE, (err, st) => {
        if (err) {
            busy = false;
            return;
        }
        if (st.size <= bytePos) {
            if (st.size < bytePos)
                bytePos = 0;
            busy = false;
            return;
        }
        const stream = fs.createReadStream(LOG_FILE, {
            start: bytePos,
            end: st.size - 1,
            encoding: 'utf8'
        });
        stream.on('data', chunk => {
            buffer += chunk;
        });
        stream.on('end', () => {
            bytePos = st.size;
            const newLines = [];
            let nl;
            while ((nl = buffer.indexOf('\n')) >= 0) {
                newLines.push(buffer.slice(0, nl));
                buffer = buffer.slice(nl + 1);
            }
            for (let i = 0; i < newLines.length; i += WINDOW_SIZE) {
                const batch = newLines.slice(
                    i,
                    i + WINDOW_SIZE
                );
                const startOffset =
                    Number(batch[0].split('\t')[0]);
                const endOffset =
                    Number(
                        batch[batch.length - 1]
                            .split('\t')[0]
                    );
                const queueId = nextQueue;
                queues[queueId].push({
                    startOffset,
                    endOffset,
                    data: batch
                });
                console.log(`Queue ${queueId} <- ${startOffset}-${endOffset}`);
                nextQueue =
                    (nextQueue + 1) % NUM_QUEUES;
            }
            busy = false;
        });
        stream.on('error', () => {
            busy = false;
        });
    });
}
function dispatch() {
    for (let i = 0; i < NUM_QUEUES; i++) {
        const worker = workers[i];
        if (worker.busy)
            continue;
        const batch = queues[i].shift();
        if (!batch)
            continue;
        worker.busy = true;
        worker.send(batch);
        console.log(
            `Worker ${worker.pid} <- ${batch.startOffset}-${batch.endOffset}`
        );
    }
}
function processAcks() {
    while (ackQueue.length) {
        const ack = ackQueue.shift();
        console.log(
            `Processed ACK ${ack.startOffset}-${ack.endOffset} | Worker ${ack.workerPid}`
        );
    }
}
try {
    bytePos = fs.statSync(LOG_FILE).size;
}
catch {
    bytePos = 0;
}
setInterval(tail, 150);
setInterval(dispatch, 100);
setInterval(processAcks, 500);
console.log('Broker Started');
console.log(`Workers: ${NUM_QUEUES}`);
console.log(`Window Size: ${WINDOW_SIZE}`);
console.log(`Tailing: ${LOG_FILE}`);