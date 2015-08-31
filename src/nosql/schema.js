// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

module.exports = Schema;

/**
 * Each database has a Schema associated with it which defines the current
 * version of the database. When a new version of app wishes to change its data
 * in a way that it is not compatible with the old app's data, the app must
 * change the schema version and provide relevant upgrade logic in the
 * Upgrader. The conflict resolution rules are also associated with the schema
 * version. Hence if the conflict resolution rules change then the schema
 * version also must be bumped.
 *
 * Schema provides metadata and an upgrader for a given database.
 *
 * @constructor
 * @param {module:syncbase.nosql.SchemaMetadata} metadata Schema metadata.
 * @param {module:syncbase.nosql.Schema~upgrader} upgrader Upgrader function.
 */
function Schema(metadata, upgrader) {
  Object.defineProperty(this, 'metadata', {
    value: metadata,
    writable: false,
    enumerable: false
  });

  Object.defineProperty(this, 'upgrader', {
    value: upgrader,
    writable: false,
    enumerable: false
  });
}

/**
 * Schema upgrader function.
 * @callback module:syncbase.Schema~upgrader
 * @param {module:syncbase.nosql.Database} db Database.
 * @param {num} oldVersion Old version.
 * @param {num} newVersion New version.
 * @param {function} cb Callback to call when done.
 */
