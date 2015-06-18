// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

module.exports = Database;

var vanadium = require('vanadium');

var BatchDatabase = require('./batch-database');
var nosqlVdl = require('../gen-vdl/v.io/syncbase/v23/services/syncbase/nosql');
var SyncGroup = require('./syncgroup');
var Table = require('./table');
var util = require('../util');

/**
 * Database represents a collection of Tables. Batches, queries, sync, watch,
 * etc. all operate at the Database level.
 * @constructor
 * @param {string} parentFullName Full name of App which contains this
 * Database.
 * @param {string} relativeName Relative name of this Database.  Must not
 * contain slashes.
 */
function Database(parentFullName, relativeName) {
  if (!(this instanceof Database)) {
    return new Database(parentFullName, relativeName);
  }

  util.addNameProperties(this, parentFullName, relativeName);

  /**
   * Caches the database wire object.
   * @private
   */
  Object.defineProperty(this, '_wireObj', {
    enumerable: false,
    value: null,
    writable: true
  });
}

/**
 * @private
 */
Database.prototype._wire = function(ctx) {
  if (this._wireObj) {
    return this._wireObj;
  }
  var client = vanadium.runtimeForContext(ctx).newClient();
  var signature = [nosqlVdl.Database.prototype._serviceDescription];

  this._wireObj = client.bindWithSignature(this.fullName, signature);
  return this._wireObj;
};

/**
 * Creates this Database.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:vanadium.security.access.Permissions} perms Permissions for
 * the new database.  If perms is null, we inherit (copy) the App perms.
 * @param {function} cb Callback.
 */
Database.prototype.create = function(ctx, perms, cb) {
  this._wire(ctx).create(ctx, perms, cb);
};

/**
 * Deletes this Database.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Database.prototype.delete = function(ctx, cb) {
  this._wire(ctx).delete(ctx, cb);
};

/**
 * Returns the Table with the given name.
 * @param {string} relativeName Table name.  Must not contain slashes.
 * @return {module:syncbase.table.Table} Table object.
 */
Database.prototype.table = function(relativeName) {
  return new Table(this.fullName, relativeName);
};

/**
 * Returns a list of all Table names.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Database.prototype.listTables = function(ctx, cb) {
  util.getChildNames(ctx, this.fullName, cb);
};

/**
 * @private
 */
Database.prototype._tableWire = function(ctx, relativeName) {
  if (relativeName.indexOf('/') >= 0) {
    throw new Error('relativeName must not contain slashes.');
  }

  var client = vanadium.runtimeForContext(ctx).newClient();
  var signature = [nosqlVdl.Table.prototype._serviceDescription];

  var fullTableName = vanadium.naming.join(this.fullName, relativeName);
  return client.bindWithSignature(fullTableName, signature);
};

// TODO(nlacasse): It's strange that we create a Database with:
//   var db = new Database();
//   db.create();
// But we create a Table with:
//   db.createTable();
// The .delete method is similarly confusing.  db.delete deletes a database,
// but table.delete deletes a row (or row range).
// Consider puting all 'create' and 'delete' methods on the parent class for
// consistency.
// TODO(aghassemi): If we keep this, it should return "table" in the CB instead
// of being void.
/**
 * Creates the specified Table.
 * If perms is nil, we inherit (copy) the Database perms.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {string} relativeName Table name.  Must not contain slashes.
 * @param {module:vanadium.security.access.Permissions} perms Permissions for
 * the new database.  If perms is null, we inherit (copy) the Database perms.
 * @param {function} cb Callback.
 */
Database.prototype.createTable = function(ctx, relativeName, perms, cb) {
  this._tableWire(ctx, relativeName).create(ctx, perms, cb);
};

/**
 * Deletes the specified Table.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {string} relativeName Relative name of Table to delete.  Must not
 * contain slashes.
 * @param {function} cb Callback.
 */
Database.prototype.deleteTable = function(ctx, relativeName, cb) {
  this._tableWire(ctx, relativeName).delete(ctx, cb);
};

/**
 * Replaces the current Permissions for the Database.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:vanadium.security.access.Permissions} perms Permissions for
 * the database.
 * @param {string} version Version of the current Permissions object which will
 * be over-written.  If empty, SetPermissions will perform an unconditional
 * update.
 * @param {function} cb Callback.
 */
Database.prototype.setPermissions = function(ctx, perms, version, cb) {
  this._wire(ctx).setPermissions(ctx, perms, version, cb);
};

/**
 * Returns the current Permissions for the Database.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Database.prototype.getPermissions = function(ctx, cb) {
  this._wire(ctx).getPermissions(ctx, cb);
};

/**
 * Creates a new batch. Instead of calling this function directly, clients are
 * recommended to use the RunInBatch() helper function, which detects
 * "concurrent batch" errors and handles retries internally.
 *
 * Default concurrency semantics:
 * - Reads inside a batch see a consistent snapshot, taken during
 *   beginBatch(), and will not see the effect of writes inside the batch.
 * - commit() may fail with errConcurrentBatch, indicating that after
 *   beginBatch() but before commit(), some concurrent routine wrote to a key
 *   that matches a key or row-range read inside this batch. (Writes inside a
 *   batch cannot cause that batch's commit() to fail.)
 * - Other methods (e.g. get) will never fail with error errConcurrentBatch,
 *   even if it is known that commit() will fail with this error.
 *
 * Concurrency semantics can be configured using BatchOptions.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:vanadium.syncbase.nosql.BatchOptions} opts BatchOptions.
 * @param {function} cb Callback.
 */
Database.prototype.beginBatch = function(ctx, opts, cb) {
  var self = this;
  this._wire(ctx).beginBatch(ctx, opts, function(err, relativeName) {
    if (err) {
      return cb(err);
    }

    // The relativeName returned from the beginBatch() call above is different
    // than the relativeName of the current database. We must create a new
    // Database with this new relativeName, and then create a BatchDatabase
    // from that new Database.
    var db = new Database(self._parentFullName, relativeName);
    return cb(null, new BatchDatabase(db));
  });
};

/**
 * Gets a handle to the SyncGroup with the given name.
 *
 * @param {string} name SyncGroup name.
 */
Database.prototype.syncGroup = function(name) {
  return new SyncGroup(this, name);
};

/**
 * Gets the global names of all SyncGroups attached to this database.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Database.prototype.getSyncGroupNames = function(ctx, cb) {
  this._wire(ctx).getSyncGroupNames(ctx, cb);
};
