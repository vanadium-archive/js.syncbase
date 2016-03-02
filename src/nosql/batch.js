// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var inherits = require('inherits');

var AbstractDatabase = require('./abstract-database');

inherits(BatchDatabase, AbstractDatabase);
module.exports = {
  BatchDatabase: BatchDatabase,
  runInBatch: runInBatch
};

/**
 * BatchDatabase is a handle to a set of reads and writes to the database that
 * should be considered an atomic unit. See database.beginBatch() for
 * concurrency semantics.
 * Private constructor. Use database.beginBatch() to get a BatchDatabase.
 * @param {string} parentFullName Full name of parent App.
 * @param {string} relativeName Relative name for this Database.
 * @param {string} batchSuffix Suffix for this BatchDatabase.
 * @param {number} schema Database schema expected by client.
 * @constructor
 * @inner
 * @memberof {module:syncbase.nosql}
 */
function BatchDatabase(parentFullName, relativeName, batchSuffix, schema) {
  if (!(this instanceof BatchDatabase)) {
    return new BatchDatabase(parentFullName, relativeName, batchSuffix, schema);
  }
  AbstractDatabase.call(this, parentFullName, relativeName, batchSuffix,
                        schema);
}

/**
 * Persists the pending changes to the database.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
BatchDatabase.prototype.commit = function(ctx, cb) {
  this._wire(ctx).commit(ctx, this.schemaVersion, cb);
};

/**
 * Notifies the server that any pending changes can be discarded.  It is not
 * strictly required, but it may allow the server to release locks or other
 * resources sooner than if it was not called.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
BatchDatabase.prototype.abort = function(ctx, cb) {
  this._wire(ctx).abort(ctx, this.schemaVersion, cb);
};

/**
 * runInBatch runs a function with a newly created batch. If the function
 * errors, the batch is aborted. If the function succeeds, the batch is
 * committed. A readonly batch is aborted either way.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:syncbase.database.Database} db Database.
 * @param {module:vanadium.syncbase.nosql.BatchOptions} opts BatchOptions.
 * @param {module:syncbase.nosql~runInBatchFn} fn Function to run inside a
 * batch.
 * @param {module:vanadium~voidCb} cb Callback that will be called after the
 * batch has been committed or aborted.
 */
function runInBatch(ctx, db, opts, fn, cb) {
  function attempt(cb) {
    db.beginBatch(ctx, opts, function(err, batchDb) {
      if (err) {
        return cb(err);
      }
      fn(batchDb, function(err) {
        if (err || opts.readOnly) {
          return batchDb.abort(ctx, function() {
            return cb(err);  // return fn error, not abort error
          });
        }
        // TODO(sadovsky): commit() can fail for a number of reasons, e.g. RPC
        // failure or ErrConcurrentTransaction. Depending on the cause of
        // failure, it may be desirable to retry the commit() and/or to call
        // abort().
        batchDb.commit(ctx, cb);
      });
    });
  }

  function retryLoop(i) {
    attempt(function(err) {
      // TODO(sadovsky): Only retry if err is ErrConcurrentTransaction.
      if (err && i < 2) {
        retryLoop(i + 1);
      } else {
        cb(err);
      }
    });
  }

  retryLoop(0);
}

/**
 * A function that is run inside a batch by [runInBatch]{@link
 * module:syncbase.nosql~runInBatch}.
 * @callback module:syncbase.nosql~runInBatchFn
 * @param {module:syncbase.batchDatabase.BatchDatabase} batch BatchDatabase.
 * @param {object} service The stub object containing the exported
 * methods of the remote service.
 */
