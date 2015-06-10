// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var runInBatch = require('./batch');
var rowrange = require('./rowrange');

/**
 * @summary
 * Defines the client API for the NoSQL part of Syncbase.
 * @namespace
 * @name nosql
 * @memberof module:syncbase
 */
module.exports = {
  rowrange: rowrange,
  runInBatch: runInBatch
};