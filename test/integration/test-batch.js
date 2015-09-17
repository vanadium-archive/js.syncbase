// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var async = require('async');
var test = require('prova');

var BatchDatabase = require('../../src/nosql/batch-database');

var nosql = require('../..').nosql;
var BatchOptions = nosql.BatchOptions;
var range = nosql.rowrange;
var ReadOnlyBatchError = nosql.ReadOnlyBatchError;

var testUtil = require('./util');
var assertScanRows = testUtil.assertScanRows;
var assertSelectRows = testUtil.assertSelectRows;
var setupDatabase = testUtil.setupDatabase;
var setupTable = testUtil.setupTable;
var uniqueName = testUtil.uniqueName;

test('db.beginBatch creates a BatchDatabase with name', function(t) {
  setupDatabase(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    o.database.beginBatch(o.ctx, new BatchOptions({}), function(err, batch) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      t.ok(batch instanceof BatchDatabase, 'batch is a BatchDatabase');
      t.notEqual(batch.name, o.database.name,
                 'batch has different name than database');
      t.notEqual(batch.fullName, o.database.fullName,
                 'batch has different fullName than database');

      o.teardown(t.end);
    });
  });
});

test('transactions are not visible until commit', function(t) {
  setupTable(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var ctx = o.ctx;
    var db = o.database;
    var table = o.table;

    var keyName = uniqueName('key');
    var value = uniqueName('val');

    var emptyPrefix = range.prefix('');

    db.beginBatch(ctx, new BatchOptions({}), put);

    var batch;
    function put(err, _batch) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      batch = _batch;
      var batchTable = batch.table(table.name);
      batchTable.put(ctx, keyName, value, assertNoRows);
    }

    function assertNoRows(err) {
      if (err) {
        return end(err);
      }

      assertScanRows(ctx, table, emptyPrefix, [], commit);
    }

    function commit(err) {
      if (err) {
        return end(err);
      }

      batch.commit(ctx, assertRow);
    }

    function assertRow(err) {
      if (err) {
        return end(err);
      }

      var wantRows = [{
        key: keyName,
        value: value
      }];

      assertScanRows(ctx, table, emptyPrefix, wantRows, end);
    }

    function end(err) {
      t.error(err);
      o.teardown(t.end);
    }
  });
});

test('concurrent transactions are isolated', function(t) {
  setupTable(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var ctx = o.ctx;
    var db = o.database;
    var table = o.table;

    var batches;
    var batchTables;
    var rows;

    var emptyPrefix = range.prefix('');

    startBatches();

    // Create two batches.
    function startBatches() {
      async.times(2, function(n, cb) {
        db.beginBatch(ctx, {}, cb);
      }, addRows);
    }

    // Each batch adds a new row.
    function addRows(err, _batches) {
      if (err) {
        return end(err);
      }

      batches = _batches;
      batchTables = batches.map(function(batch) {
        return batch.table(table.name);
      });

      // Put to the same key in each batch.
      var key = uniqueName('key');
      async.mapSeries(batchTables, function(batchTable, cb) {
        // Put different value in each batch.
        var value = uniqueName('value');
        batchTable.put(ctx, key, value, function(err) {
          if (err) {
            return cb(err);
          }
          return cb(null, [{key: key, value: value}]);
        });
      }, assertBatchesSeeCorrectRows);
    }

    // Verify that each batch sees only its own rows.
    function assertBatchesSeeCorrectRows(err, _rows) {
      if (err) {
        return end(err);
      }

      rows = _rows;
      async.forEachOfSeries(batchTables, function(batchTable, idx, cb) {
        // NOTE(nlacasse): Currently, a scan() inside a batch will return only
        // the rows that existed in the snapshot when the batch was started.
        // Thus, we can't use assertScanRows() to check that the batch has the
        // correct rows.  Instead we must call get() on the table directly to
        // ensure that the new rows exist in the snapshot.
        batchTable.get(ctx, rows[idx][0].key, function(err, row) {
          if (err) {
            return cb(err);
          }
          t.equal(rows[idx].key, row.key, 'row has correct key');
          t.equal(rows[idx].value, row.value, 'row has correct value');
          return cb(null);
        });
      }, commitFirstBatch);
    }

    function commitFirstBatch(err) {
      if (err) {
        return end(err);
      }

      batches[0].commit(ctx, commitSecondBatch);
    }

    function commitSecondBatch(err) {
      if (err) {
        return end(err);
      }

      // Second batch should fail on commit.
      batches[1].commit(ctx, function(err) {
        t.ok(err, 'second batch should fail on commit');

        assertFirstBatchesRowsExist();
      });
    }

    function assertFirstBatchesRowsExist() {
      // Check that only first batch's rows exist in table.
      assertScanRows(ctx, table, emptyPrefix, rows[0], end);
    }

    function end(err) {
      t.error(err);
      o.teardown(t.end);
    }
  });
});

test('read-only batches', function(t) {
  setupTable(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var ctx = o.ctx;
    var db = o.database;
    var table = o.table;

    var key = uniqueName('key');
    var value = uniqueName('value');

    var batch;
    var batchTable;

    table.put(ctx, key, value, startReadOnlyBatch);

    function startReadOnlyBatch(err) {
      if (err) {
        return end(err);
      }

      var opts = new BatchOptions(new Map([
        ['ReadOnly', true]
      ]));

      db.beginBatch(ctx, opts, attemptBatchPut);
    }

    function attemptBatchPut(err, _batch) {
      if (err) {
        return end(err);
      }

      batch = _batch;
      batchTable = batch.table(table.name);

      batchTable.put(ctx, uniqueName('key'), uniqueName('val'), function(err) {
        assertReadOnlyBatchError(err);
        attemptBatchDeletePrefix();
      });
    }

    function attemptBatchDeletePrefix() {
      batchTable.deleteRange(ctx, range.prefix(key), function(err) {
        assertReadOnlyBatchError(err);
        attemptBatchDeleteRow();
      });
    }

    function attemptBatchDeleteRow() {
      batchTable.row(key).delete(ctx, function(err) {
        assertReadOnlyBatchError(err);
        batch.getResumeMarker(ctx, assertBatchResumeMarker);
      });
    }

    function assertBatchResumeMarker(err, r) {
      if (err) {
        return end(err);
      }
      t.ok(r, 'should return a resume marker');

      assertBatchScan();
    }

    var wantRows = [{
      key: key,
      value: value
    }];

    function assertBatchScan() {
      assertScanRows(ctx, batchTable, range.prefix(''), wantRows,
        assertBatchSelect);
    }

    function assertBatchSelect(err) {
      if (err) {
        return end(err);
      }

      assertSelectRows(ctx, batch, batchTable, '', wantRows, end);
    }

    function end(err) {
      t.error(err);
      o.teardown(t.end);
    }

    function assertReadOnlyBatchError(err) {
      t.ok(err, 'should error');
      t.ok(err instanceof ReadOnlyBatchError,
           'err should be ReadOnlyBatchError');
    }

  });
});

test('new batch operations fail after successful batch commit', function(t) {
  setupTable(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var ctx = o.ctx;
    var db = o.database;
    var table = o.table;

    db.beginBatch(ctx, {}, put);

    var batch;

    function put(err, _batch) {
      if (err) {
        return end(err);
      }

      batch = _batch;
      var batchTable = batch.table(table.name);

      batchTable.put(ctx, uniqueName('key'), uniqueName('val'), commit);
    }

    function commit(err) {
      if (err) {
        return end(err);
      }

      batch.commit(ctx, function(err) {
        if (err) {
          return end(err);
        }

        assertOpsFail(t, ctx, batch, table.name, end);
      });
    }

    function end(err) {
      t.error(err);
      o.teardown(t.end);
    }
  });
});

test('new batch operations fail after unsuccessful batch commit', function(t) {
  setupTable(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var ctx = o.ctx;
    var db = o.database;
    var table = o.table;

    db.beginBatch(ctx, {}, readBatchTable);

    var key = uniqueName('key');
    var value = uniqueName('value');

    var batch;
    var batchTable;

    function readBatchTable(err, _batch) {
      if (err) {
        return t.end(err);
      }

      batch = _batch;
      batchTable = batch.table(table.name);

      batchTable.get(ctx, key, function(err) {
        // Should error because the key does not exist yet.
        t.ok(err, 'get should error when key does not exist');
        putTable();
      });
    }

    function putTable(err) {
      if (err) {
        return end(err);
      }

      // Put on the table directly, not the batch table.  This will conflict
      // with future batchTable.put() call.
      table.put(ctx, key, value, putBatchTable);
    }

    function putBatchTable(err) {
      if (err) {
        return end(err);
      }

      var newValue = uniqueName('value');

      batchTable.put(ctx, key, newValue, commit);
    }

    function commit(err) {
      if (err) {
        return end(err);
      }

      batch.commit(ctx, function(err) {
        t.ok(err, 'commit() should error');
        assertOpsFail(t, ctx, batch, table.name, end);
      });
    }

    function end(err) {
      t.error(err);
      o.teardown(t.end);
    }
  });
});

test('new batch operations fail after batch is aborted', function(t) {
  setupTable(t, function(err, o) {
    if (err) {
      return end(err);
    }

    var ctx = o.ctx;
    var db = o.database;
    var table = o.table;

    db.beginBatch(ctx, {}, abort);

    function abort(err, batch) {
      if (err) {
        return end(err);
      }

      batch.abort(ctx, function(err) {
        if (err) {
          return end(err);
        }

        assertOpsFail(t, ctx, batch, table.name, end);
      });
    }

    function end(err) {
      t.error(err);
      o.teardown(t.end);
    }
  });
});

function assertOpsFail(t, ctx, batch, tableName, cb) {
  var batchTable = batch.table(tableName);

  async.series([
    assertGetFails,
    assertScanFails,
    assertPutFails,
    assertDeleteFails,
    assertRowDeleteFails,
    assertCommitFails
  ], cb);

  function assertGetFails(cb) {
    batchTable.get(ctx, uniqueName('key'), function(err) {
      t.ok(err, 'get() should error');
      cb(null);
    });
  }

  function assertScanFails(cb) {
    var streamGotError = false;

    var stream = batchTable.scan(ctx, range.prefix(''), function(err) {
      t.ok(err, 'scan() should pass error to callback');
      t.ok(streamGotError, 'scan() should send error to stream');
      cb(null);
    });

    stream.on('error', function(err) {
      streamGotError = true;
    });
  }

  function assertPutFails(cb) {
    batchTable.put(ctx, uniqueName('key'), uniqueName('val'),
                   function(err) {
      t.ok(err, 'put() should error');
      cb(null);
    });
  }

  function assertDeleteFails(cb) {
    batchTable.deleteRange(ctx, range.prefix(uniqueName('key')), function(err) {
      t.ok(err, 'delete() should error');
      cb(null);
    });
  }

  function assertRowDeleteFails(cb) {
    batchTable.row(uniqueName('key')).delete(ctx, function(err) {
      t.ok(err, 'row.delete() should error');
      cb(null);
    });
  }

  function assertCommitFails(cb) {
    batch.commit(ctx, function(err) {
      t.ok(err, 'commit() should error');
      cb(null);
    });
  }
}
