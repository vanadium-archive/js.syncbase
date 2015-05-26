// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

module.exports = Database;

// Database represents a collection of Tables. Batches, queries, sync, watch,
// etc. all operate at the Database level.
function Database(fullName, name) {
  if (!(this instanceof Database)) {
    return new Database(fullName, name);
  }

  /**
   * @property name
   * @type {string}
   */
  Object.defineProperty(this, 'name', {
    value: name,
    writable: false
  });

  /**
   * @property name
   * @type {string}
   */
  Object.defineProperty(this, 'fullName', {
    value: fullName,
    writable: false
  });
}

// Table returns the Table with the given name.
// relativeName must not contain slashes.
Database.prototype.table = function(relativeName) {};

// ListTables returns a list of all Table names.
Database.prototype.listTables = function(ctx) {};

// Create creates this Database.
// If perms is nil, we inherit (copy) the App perms.
Database.prototype.create = function(ctx, perms) {};

// Delete deletes this Database.
Database.prototype.delete = function(ctx) {};

// Create creates the specified Table.
// If perms is nil, we inherit (copy) the Database perms.
// relativeName must not contain slashes.
Database.prototype.createTable = function(ctx, relativeName, perms) {};

// DeleteTable deletes the specified Table.
Database.prototype.deleteTable = function(ctx, relativeName) {};

// SetPermissions replaces the current Permissions for an object.
Database.prototype.setPermissions = function(ctx, perms, version) {};

// GetPermissions returns the current Permissions for an object.
Database.prototype.getPermissions = function(ctx) {};

// BeginBatch creates a new batch. Instead of calling this function directly,
// clients are recommended to use the RunInBatch() helper function, which
// detects "concurrent batch" errors and handles retries internally.
//
// Default concurrency semantics:
// - Reads inside a batch see a consistent snapshot, taken during
//   BeginBatch(), and will not see the effect of writes inside the batch.
// - Commit() may fail with ErrConcurrentBatch, indicating that after
//   BeginBatch() but before Commit(), some concurrent routine wrote to a key
//   that matches a key or row-range read inside this batch. (Writes inside a
//   batch cannot cause that batch's Commit() to fail.)
// - Other methods (e.g. Get) will never fail with error ErrConcurrentBatch,
//   even if it is known that Commit() will fail with this error.
//
// Concurrency semantics can be configured using BatchOptions.
Database.prototype.beginBatch = function(ctx, opts) {};

// BatchDatabase is a handle to a set of reads and writes to the database that
// should be considered an atomic unit. See BeginBatch() for concurrency
// semantics.
function BatchDatabase(db) {
  if (typeof this !== BatchDatabase) {
    return new BatchDatabase(db);
  }

  this._db = db;
}

// Table returns the Table with the given name.
// relativeName must not contain slashes.
BatchDatabase.prototype.table = function(relativeName) {
  return this._db.table(relativeName);
};

// ListTables returns a list of all Table names.
BatchDatabase.prototype.listTables = function(ctx) {
  return this._db.listTables(ctx);
};

// Commit persists the pending changes to the database.
BatchDatabase.prototype.commit = function(relativeName) {};

// Abort notifies the server that any pending changes can be discarded.
// It is not strictly required, but it may allow the server to release locks
// or other resources sooner than if it was not called.
BatchDatabase.prototype.abort = function(ctx) {};