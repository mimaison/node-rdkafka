/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

'use strict';

var t = require('assert');
var util = require('util');
var Emitter = require('events');

// This file intends to mock the librdkafka native bindings so it can
// be injected into the tests to ensure that the node part works properly

var librdkafka = module.exports = {
  KafkaConsumer: KafkaConsumer,
  Producer: Producer,
  Topic: Topic
};

util.inherits(Connection, Emitter);

function Connection(globalConfig, topicConfig) {
  Emitter.call(this);
  t.equal(typeof globalConfig, 'object', 'Global config must be an object');
  t.equal(typeof topicConfig, 'object', 'Topic config must be an object');

  this.globalConfig = globalConfig;
  this.topicConfig = topicConfig;
}

Connection.prototype.onEvent = function(cb) {
  t.equal(typeof cb, 'function', 'Callback must be a function');

  this.onEvent = cb;
};

Connection.prototype.connect = function(cb) {
  t.equal(typeof cb, 'function', 'Callback must be a function');

  this.emit('connect', cb);
};

Connection.prototype.disconnect = function(cb) {
  t.equal(typeof cb, 'function', 'Callback must be a function');

  this.emit('disconnect', cb);
};

Connection.prototype.getMetadata = function(opts, cb) {
  if (opts) {
    t.equal(typeof opts, 'object', 'Opts must be an object');
  }
  t.equal(typeof cb, 'function', 'Callback must be a function');

  this.emit('getMetadata', opts, cb);
};


// Consumer

util.inherits(KafkaConsumer, Connection);

function KafkaConsumer(globalConfig, topicConfig) {
  Connection.call(this, globalConfig, topicConfig);
}

KafkaConsumer.prototype.onRebalance = function(cb) {
  t.equal(typeof cb, 'function', 'Callback must be a function');

  this.rebalanceCb = cb;
};

// Producer

util.inherits(Producer, Connection);

function Producer(globalConfig, topicConfig) {
  Connection.call(this, globalConfig, topicConfig);
}

Producer.prototype.onDeliveryReport = function(cb) {
  t.equal(typeof cb, 'function', 'Callback must be a function');

  this.deliveryReportCb = cb;
};

function Topic(topicName, config, client) {
  if (!(client instanceof KafkaConsumer) && !(client instanceof Producer)) {
    t.fail(client, 'KafkaConsumer or Producer', 'client must be an instance of handle');
  }

  t.equal(typeof topicName, 'string', 'Topic name must be a string');
  t.equal(typeof config, 'object', 'Config must be an object');

  this.topicName = topicName;
  this.config = config;
  this.client = client;
}
