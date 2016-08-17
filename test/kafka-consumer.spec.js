/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

var t = require('assert');
var util = require('util');

// This replaces some stuff so the next module should be aware
var mock = require('./librdkafka-mock');
var KafkaConsumer = require('../lib/kafka-consumer');
KafkaConsumer.useMock(mock);

var mockMetadata = {
  orig_broker_id: 1,
  orig_broker_name: "broker_name",
  brokers: [
    {
      id: 1,
      host: 'localhost',
      port: 40
    }
  ],
  topics: [
    {
      name: 'awesome-topic',
      partitions: [
        {
          id: 1,
          leader: 20,
          replicas: [1, 2],
          isrs: [1, 2]
        }
      ]
    }
  ]
};

module.exports = {
  'KafkaConsumer': {
    'exports a function': function() {
      t.equal(typeof KafkaConsumer, 'function', 'Should export a function');
    },
    'should instantiate to an object': function() {
      var client = new KafkaConsumer();
      t.equal(typeof client, 'object', 'Should export an object');
    },
    'should instantiate an object with default parameters': function() {
      var client = new KafkaConsumer();
    },
    'should set an onEvent callback on the native bindings by default': function(done) {
      var client = new KafkaConsumer();
      var onEvent = client.getClient().onEvent;

      t.equal(typeof onEvent, 'function');

      client.once('event.log', function(data) {
        done();
      });

      // Let's call onEvent and make sure it routes properly
      onEvent('log', {});
    },
    'error events should propagate to the proper emitted event': function(done) {
      var client = new KafkaConsumer();
      var onEvent = client.getClient().onEvent;

      t.equal(typeof onEvent, 'function');

      client.once('error', function(error) {
        t.equal(util.isError(error), true, 'Did not convert the object to an error');
        t.equal(error.code, -1, 'Error codes are not equal');
        done();
      });

      // Let's call onEvent and make sure it routes properly
      onEvent('error', { code: -1, message: 'Uh oh' });
    },
    'should set a rebalance event by default': function(done) {
      var client = new KafkaConsumer();
      var onRebalance = client.getClient().rebalanceCb;

      t.equal(typeof onRebalance, 'function', 'Did not set a rebalance event');

      client.on('rebalance', function() {
        done();
      });

      onRebalance();
    },
    'should properly connect': function(done) {
      var client = new KafkaConsumer();
      var native = client.getClient();

      native.on('connect', function(cb) {
        setImmediate(function() {
          cb(null, { name: 'kafka#test' });
        });
      });

      native.on('getMetadata', function(opts, cb) {
        cb(null, mockMetadata);
      });

      client.connect(null, function(err, metadata) {
        t.ifError(err);
        t.deepEqual(metadata, mockMetadata, 'Metadata is not equal');
        done();
      });
    },
    'should properly disconnect': function(done) {
      var client = new KafkaConsumer();
      var native = client.getClient();

      native.on('connect', function(cb) {
        setImmediate(function() {
          cb(null, { name: 'kafka#test' });
        });
      });

      native.on('getMetadata', function(opts, cb) {
        cb(null, mockMetadata);
      });

      native.on('disconnect', function(cb) {
        cb();
      });

      client.connect(null, function(err, metadata) {
        t.ifError(err);
        t.deepEqual(metadata, mockMetadata, 'Metadata is not equal');
        client.disconnect(function(err, metrics) {
          t.ifError(err);
          t.ok(metrics);
          done();
        });
      });
    },
    'connection should emit the ready event': function(done) {
      var client = new KafkaConsumer();
      var native = client.getClient();

      native.on('connect', function(cb) {
        setImmediate(function() {
          cb(null, { name: 'kafka#test' });
        });
      });

      native.on('getMetadata', function(opts, cb) {
        cb(null, mockMetadata);
      });

      native.on('disconnect', function(cb) {
        cb();
      });

      client.connect()
        .on('ready', function() {
          done();
        })
        .on('error', t.ifError);
    }

  }
};
