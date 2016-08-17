/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

var Kafka = require('../');
var t = require('assert');
var crypto = require('crypto');

var kafkaBrokerList = process.env.KAFKA_HOST || 'localhost:9092';

var serviceStopped = false;

var topic = 'test'; // + crypto.randomBytes(20).toString('hex');
var producer;

module.exports = {
  'Producer e2e tests': {
    'can connect to Kafka': function(cb) {

      producer = new Kafka.Producer({
        'client.id': 'kafka-test',
        'metadata.broker.list': kafkaBrokerList,
        'dr_cb': true
      });
      producer.connect({}, function(err) {
        if (err) {
          t.ifError(err);
        }

        producer.getMetadata({}, function(err, metadata) {
          t.ifError(err);
          t.ok(metadata);

          // Ensure it is in the correct format
          t.ok(metadata.orig_broker_name, 'Broker name is not set');
          t.ok(metadata.orig_broker_id, 'Broker id is not set');
          t.equal(Array.isArray(metadata.brokers), true);
          t.equal(Array.isArray(metadata.topics), true);

          producer.disconnect(function() {
            producer = null;
            cb();
          });
        });
      });
    },

    'gets 100% deliverability': function(cb) {
      this.timeout(10000);
      producer = new Kafka.Producer({
        'client.id': 'kafka-mocha',
        'metadata.broker.list': kafkaBrokerList,
        'dr_cb': true
      });
      producer.connect();

      var total = 0;
      var totalSent = 0;
      var max = 10000;
      var errors = 0;
      var started = Date.now();
      var topicObj;

      var sendMessage = function() {
        console.log('Sending message');
        if (!topicObj) {
          topicObj = producer.Topic(topic, {});
        }

        var ret = producer.produce({
          topic: topicObj,
          message: new Buffer('message ' + total)
        }, function(err) {
          total++;
          totalSent++;
          t.ifError(err);

          if (total >= max) {
          } else {
            sendMessage();
          }
        });

      };

      var verified_received = 0;
      var exitNextTick = false;
      var errorsArr = [];

      var tt = setInterval(function() {
        console.log('polling');
        producer.poll();

        if (exitNextTick) {
          clearInterval(tt);
          if (errors > 0) {
            return cb(errorsArr[0]);
          }

          producer.disconnect(function() {
            cb();
          });

          return;
        }

        if (verified_received + errors === max) {
          exitNextTick = true;
        }

      }, 1000).unref();

      producer
        .on('delivery-report', function(report) {
          t.ok(report !== undefined);
          t.ok(typeof report.topic_name === 'string');
          t.ok(typeof report.partition === 'number');
          t.ok(typeof report.offset === 'number');
          verified_received++;
        }).on('ready', sendMessage);
    }

  }
};
