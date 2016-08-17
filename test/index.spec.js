/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

var t = require('assert');
var lib = require('../lib');

module.exports = {
  'Main export': {
    'is an object': function() {
      t.equal(typeof lib, 'object');
    },

    'has required export objects': function() {
      t.equal(typeof lib.KafkaConsumer, 'function', 'KafkaConsumer is not a function');
      t.equal(typeof lib.Consumer, 'function', 'Consumer is not a function');
      t.equal(typeof lib.Producer, 'function', 'Producer is not a function');
      t.equal(typeof lib.CODES, 'object', 'Codes is not set');
      t.ok(Object.keys(lib.CODES).length);
      t.ok(Object.keys(lib.CODES.ERRORS).length);
    }
  }
};
