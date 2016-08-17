/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

 var t = require('assert');
 var LibrdKafkaError = require('../lib/error');

module.exports = {
  'LibrdKafkaError': {
    'exports an function': function() {
      t.equal(typeof LibrdKafkaError, 'function');
    },

    'can be instantiated': function() {
      var err = new LibrdKafkaError();
      t.equal(typeof err, 'object');
    },

    'extends Error': function() {
      t.equal(new LibrdKafkaError() instanceof Error, true);
    },

    'exports codes': function() {
      t.equal(typeof LibrdKafkaError.codes, 'object');

      var numCodes = Object.keys(LibrdKafkaError.codes).length;
      t.equal(numCodes> 0, true);
    },

    'can extend a specific error': function() {
      var e = new Error('Baseline error');

      var rdError = new LibrdKafkaError(e);

      t.ok(rdError instanceof Error);
      t.equal(rdError.message, e.message.toLowerCase());
      // Should have equal stacks
      t.equal(rdError.stack, e.stack);
      t.ok(rdError.origin);
      t.equal(typeof rdError.code, 'number');
    },

    'can use a factory method to extend an error': function() {
      var e = new Error('Baseline error');

      var rdError = LibrdKafkaError.create(e);

      t.ok(rdError instanceof Error);
      t.equal(rdError.message, e.message.toLowerCase());
      // Should have equal stacks
      t.equal(rdError.stack, e.stack);
      t.ok(rdError.origin);
      t.equal(typeof rdError.code, 'number');
    },

    'can take an object like the ones passed from the addon': function() {
      var e = {
        message: 'Test error',
        code: -1,
      };

      var rdError = LibrdKafkaError.create(e);
      t.ok(rdError instanceof Error);

      t.equal(typeof rdError.code, 'number');
      t.equal(rdError.origin, 'unknown');
      t.ok(rdError.message);
    }
  }
};
