/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

var Writable = require('stream').Writable;

var Kafka = require('../');
var count = 0;
var total = 0;
var store = [];
var host = process.argv[2] || 'localhost:9092';
var topic = process.argv[3] || 'test';

var consumer = new Kafka.KafkaConsumer({
  'metadata.broker.list': host,
  'group.id': 'node-rdkafka-bench-consumer',
  'fetch.wait.max.ms': 100,
  'fetch.message.max.bytes': 1024 * 1024,
  'enable.auto.commit': false
}, {
  'auto.offset.reset': 'earliest'
});

// Track how many messages we see per second
var interval;

consumer.on('rebalance', function() {
  interval = setInterval(function() {
    if (count > 0) {
      // Don't store ones when we didn't get data i guess?
      store.push(count);
      count = 0;
      console.log('%d messages per second', count);
    } else {
      // We are done.
      shutdown();
      clearInterval(interval);
    }
  }, 1000).unref();
});

console.log('Running consumer stream API benchmarks');

var stream = consumer.getReadStream(topic, {
  fetchSize: 16
});
var isShuttingDown = false;

stream
  .on('error', function(err) {
    console.log('Shutting down due to error');
    console.log(err.stack);
    shutdown();
  })
  .on('end', function() {
    shutdown();
  })
  .pipe(new Writable({
    objectMode: true,
    write: function(batchMessages, encoding, cb) {
      if (Array.isArray(batchMessages)) {
        count += batchMessages.length;
        total += batchMessages.length;
      } else {
        count += 1;
        total += 1;
      }
      setImmediate(cb);
    }
  }));

process.once('SIGTERM', shutdown);
process.once('SIGINT', shutdown);
process.once('SIGHUP', shutdown);

function shutdown() {
  if (isShuttingDown) {
    return;
  }
  isShuttingDown = true;

  if (store.length > 0) {
    var calc = 0;
    for (var x in store) {
      calc += store[x];
    }

    var mps = parseFloat(calc * 1.0/store.length);

    console.log('%d messages per second on average', Math.round(mps));
  }

  consumer.disconnect();
  process.exit(0);

}
