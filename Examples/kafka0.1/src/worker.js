process.on('message', batch => {
    console.log(
        `Worker ${process.pid} processing ${batch.startOffset}-${batch.endOffset}`
    );
    console.log(
        `Entries: ${batch.data.length}`
    );
    setTimeout(() => {
        process.send({
            type: 'ACK',
            startOffset: batch.startOffset,
            endOffset: batch.endOffset,
            workerPid: process.pid
        });
    }, 1000);
});