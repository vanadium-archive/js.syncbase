// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

module.exports = runInBatch;

/**
 * @summary
 * runInBatch runs a function with a newly created batch. If the function
 * errors, the batch is aborted. If the function succeeds, the batch is
 * committed. If an error occurs during commit, then the batch is aborted.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:syncbase.database.Database} db Database.
 * @param {module:vanadium.syncbase.nosql.BatchOptions} opts BatchOptions.
 * @param {module:syncbase.nosql~runInBatchFn} fn Function to run inside a
 * batch.
 * @param {module:vanadium~voidCb} cb Callback that will be called after the
 * batch has been committed or aborted.
 *
 * TODO(nlacasse): Add retry loop.
 */
function runInBatch(ctx, db, opts, fn, cb) {
  db.beginBatch(ctx, opts, function(err, batchDb) {
    if (err) {
      return cb(err);
    }

    function onError(err) {
      batchDb.abort(ctx, function() {
        cb(err);
      });
    }

    function onSuccess() {
      // TODO(nlacasse): Commit() can fail for a number of reasons, e.g. RPC
      // failure or ErrConcurrentTransaction. Depending on the cause of
      // failure, it may be desirable to retry the Commit() and/or to call
      // Abort(). For now, we always abort on a failed commit.
      batchDb.commit(ctx, function(commitErr) {
        if (commitErr) {
          return onError(commitErr);
        }
        return cb(null);
      });
    }

    fn(batchDb, function(err) {
      if (err) {
        return onError(err);
      }
      onSuccess();
    });
  });
}

/**
 * A function that is run inside a batch by [runInBatch]{@link
 * module:syncbase.nosql~runInBatch}.
 * @callback module:syncbase.nosql~runInBatchFn
 * @param {module:syncbase.batchDatabase.BatchDatabase} batch BatchDatabase.
 * @param {object} service The stub object containing the exported
 * methods of the remote service.
 */
