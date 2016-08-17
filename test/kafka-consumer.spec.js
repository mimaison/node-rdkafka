/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

var t = require('assert');

// This replaces some stuff so the next module should be aware
var mock = require('./librdkafka-mock');
var KafkaConsumer = require('../lib/kafka-consumer');
KafkaConsumer.useMock(mock);

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
    }
  }
};
