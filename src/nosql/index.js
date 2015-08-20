// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var rowrange = require('./rowrange');
var runInBatch = require('./batch');
var Schema = require('./schema');
var vdl = require('../gen-vdl/v.io/syncbase/v23/services/syncbase/nosql');
var watch = require('./watch');

/**
 * @summary
 * Defines the client API for the NoSQL part of Syncbase.
 * @namespace
 * @name nosql
 * @memberof module:syncbase
 */
module.exports = {
  BatchOptions: vdl.BatchOptions,
  ReadOnlyBatchError: vdl.ReadOnlyBatchError,
  ResumeMarker: watch.ResumeMarker,
  rowrange: rowrange,
  runInBatch: runInBatch,
  Schema: Schema,
  SchemaMetadata: vdl.SchemaMetadata,
  SyncGroupMemberInfo: vdl.SyncGroupMemberInfo,
  SyncGroupSpec: vdl.SyncGroupSpec,
  WatchChange: watch.WatchChange
};
