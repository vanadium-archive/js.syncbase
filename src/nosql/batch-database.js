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
 * @param {number} schemaVersion Database schema version expected by client.
 */
function BatchDatabase(db, schemaVersion) {
  if (!(this instanceof BatchDatabase)) {
    return new BatchDatabase(db, schemaVersion);
  }

  this.schemaVersion = schemaVersion;
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
  this._db._wire(ctx).commit(ctx, this.schemaVersion, cb);
};

/**
 * Notifies the server that any pending changes can be discarded.  It is not
 * strictly required, but it may allow the server to release locks or other
 * resources sooner than if it was not called.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
BatchDatabase.prototype.abort = function(ctx, cb) {
  this._db._wire(ctx).abort(ctx, this.schemaVersion, cb);
};

/**
 * Executes a syncQL query.
 *
 * Returns a stream of rows.  The first row contains an array of headers (i.e.
 * column names).  Subsequent rows contain an array of values for each row that
 * matches the query.  The number of values returned in each row will match the
 * size of the headers array.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {string} query Query string.
 * @param {function} cb Callback.
 * @returns {stream} Stream of rows.
 */
BatchDatabase.prototype.exec = function(ctx, query, cb) {
  return this._db.exec(ctx, query, cb);
};

/**
 * Gets the ResumeMarker that points to the current end of the event log.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
BatchDatabase.prototype.getResumeMarker = function(ctx, cb) {
  this._db.getResumeMarker(ctx, cb);
};
