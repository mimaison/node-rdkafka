/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

var addon = require('bindings')('node-librdkafka');
var t = require('assert');

var consumerConfig = {
  'group.id': 'awesome'
};
var client;
var defaultConfig = {
  'client.id': 'kafka-mocha',
  'group.id': 'kafka-mocha-grp',
  'metadata.broker.list': 'localhost:9092'
};

module.exports = {
  'native addon': {
    'exports something': function() {
      t.equal(typeof(addon), 'object');
    },
    'exports valid producer': function() {
      t.equal(typeof(addon.Producer), 'function');
      t.throws(addon.Producer); // Requires constructor
      t.equal(typeof(new addon.Producer({}, {})), 'object');
    },
    'exports valid consumer': function() {
      t.equal(typeof(addon.KafkaConsumer), 'function');
      t.throws(addon.KafkaConsumer); // Requires constructor
      t.equal(typeof(new addon.KafkaConsumer(consumerConfig, {})), 'object');
    },
    'exports version': function() {
      t.ok(addon.librdkafkaVersion);
    }
  },

  /**
   * Native consumer tests
   *
   * Generally for testing proper throws, error messages, type definitions, etc.
   */
  'Native Consumer': {
    'afterEach': function() {
      client = null;
    },
    'requires configuration': function() {
      t.throws(function() {
        return new addon.KafkaConsumer();
      });
    },
    'cannot be set without a topic config': function() {
      t.throws(function() {
        client = new addon.KafkaConsumer(defaultConfig);
      });
    },
    'can be given a topic config': function() {
      client = new addon.KafkaConsumer(defaultConfig, {});
    },
    'throws us an error if we provide an invalid configuration value': function() {
      t.throws(function() {
        client = new addon.KafkaConsumer({
          'foo': 'bar'
        });
      }, 'should throw because the key is invalid1');
    },
    'throws us an error if topic config is given something invalid': function() {
      t.throws(function() {
        client = new addon.KafkaConsumer(defaultConfig, { 'foo': 'bar' });
      });
    },
    'ignores function arguments for global configuration': function() {
      client = new addon.KafkaConsumer({
        'event_cb': function() {},
        'group.id': 'mocha-test'
      }, {});
      t.ok(client);
    },
    'can be garbage collected': function() {
      client = new addon.KafkaConsumer({}, {});
      client = null;
      global.gc();
    }
  },

  /**
   * Native initialized client tests
   *
   * Just saves us from some typing because we can just access `client` in
   * each of the tests
   */
  'Native KafkaConsumer client': {
    'beforeEach': function() {
      client = new addon.KafkaConsumer(defaultConfig, {});
    },
    'afterEach': function() {
      client = null;
      global.gc();
    },
    'is an object': function() {
      t.equal(typeof(client), 'object');
    },
    'has necessary methods from superclass': function() {
      var methods = ['connect', 'disconnect', 'onEvent', 'getMetadata'];
      methods.forEach(function(m) {
        t.equal(typeof(client[m]), 'function', 'Client is missing ' + m + ' method');
      });
    },
    'has necessary bindings for librdkafka 1:1 binding': function() {
      var methods = ['assign', 'unassign', 'subscribe', 'consume', 'consumeLoop',
        'onRebalance', 'subscribeSync', 'getAssignments', 'unsubscribe', 'unsubscribeSync',
        'commit', 'commitSync'];
      methods.forEach(function(m) {
        t.equal(typeof(client[m]), 'function', 'Client is missing ' + m + ' method');
      });
    },
    'methods that require being connected (and throw,) throw': function() {
      var methods = ['assign', 'unassign', 'subscribe', 'onRebalance',
      'subscribeSync', 'getAssignments', 'unsubscribe', 'commitSync'];
      methods.forEach(function(m) {
        t.throws(function() {
          client[m]();
        });
      });
    },
  },

  /**
   * Native producer tests
   *
   * Generally for testing proper throws, error messages, type definitions, etc.
   */
  'Native Producer': {
    'afterEach': function() {
      client = null;
    },
    'requires configuration': function() {
      t.throws(function() {
        return new addon.Producer();
      });
    },
    'cannot be set without a topic config': function() {
      t.throws(function() {
        client = new addon.Producer(defaultConfig);
      });
    },
    'can be given a topic config': function() {
      client = new addon.Producer(defaultConfig, {});
    },
    'throws us an error if we provide an invalid configuration value': function() {
      t.throws(function() {
        client = new addon.Producer({
          'foo': 'bar'
        });
      }, 'should throw because the key is invalid1');
    },
    'throws us an error if topic config is given something invalid': function() {
      t.throws(function() {
        client = new addon.Producer(defaultConfig, { 'foo': 'bar' });
      });
    },
    'ignores function arguments for global configuration': function() {
      client = new addon.Producer({
        'event_cb': function() {},
        'group.id': 'mocha-test'
      }, {});
      t.ok(client);
    },
    'ignores function arguments for topic configuration': function() {
      client = new addon.Producer(defaultConfig, {
        'partitioner_cb': function() {}
      });
    },
    'can be garbage collected': function() {
      client = new addon.Producer({}, {});
      client = null;
      global.gc();
    }
  },

  /**
   * Native initialized producer client tests
   *
   * Just saves us from some typing because we can just access `client` in
   * each of the tests
   */
  'Native Producer client': {
    'beforeEach': function() {
      client = new addon.Producer(defaultConfig, {});
    },
    'afterEach': function() {
      client = null;
    },
    'is an object': function() {
      t.equal(typeof(client), 'object');
    },
    'requires configuration': function() {
      t.throws(function() {
        return new addon.Producer();
      });
    },
    'has necessary methods from superclass': function() {
      var methods = ['connect', 'disconnect', 'onEvent', 'getMetadata'];
      methods.forEach(function(m) {
        t.equal(typeof(client[m]), 'function', 'Client is missing ' + m + ' method');
      });
    },
  }
};
