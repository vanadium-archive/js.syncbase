// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

module.exports = BatchDatabase;

/*
 * A handle to a set of reads and writes to the database that should be
 * considered an atomic unit. See beginBatch() for concurrency semantics.
 *
 * This constructor is private.  Use [database.beginBatch]{@link
 * module:syncbase.nosql.Database.beginBatch} or [nosql.runInBatch]{@link
 * module:syncbase.nosql~runInBatch} instead.
 * @constructor
 * @inner
 * @param {module:syncbase.database.Database} db Database.
 */
function BatchDatabase(db) {
  if (!(this instanceof BatchDatabase)) {
    return new BatchDatabase(db);
  }

  Object.defineProperty(this, '_db', {
    enumerable: false,
    value: db,
    writeable: false
  });
}

/**
 * Returns the Table with the given name.
 * @param {string} relativeName Table name.  Must not contain slashes.
 * @return {module:syncbase.table.Table} Table object.
 */
BatchDatabase.prototype.table = function(relativeName) {
  return this._db.table(relativeName);
};

/**
 * Returns a list of all Table names.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
BatchDatabase.prototype.listTables = function(ctx, cb) {
  this._db.listTables(ctx, cb);
};

/**
 * Persists the pending changes to the database.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
BatchDatabase.prototype.commit = function(ctx, cb) {
  this._db._wire(ctx).commit(ctx, cb);
};

/**
 * Notifies the server that any pending changes can be discarded.  It is not
 * strictly required, but it may allow the server to release locks or other
 * resources sooner than if it was not called.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
BatchDatabase.prototype.abort = function(ctx, cb) {
  this._db._wire(ctx).abort(ctx, cb);
};
