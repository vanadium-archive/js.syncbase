// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var async = require('async');
var test = require('prova');

var syncbase = require('../..');

var testUtil = require('./util');
var assertScanRows = testUtil.assertScanRows;
var setupDatabase = testUtil.setupDatabase;
var setupTable = testUtil.setupTable;
var uniqueName = testUtil.uniqueName;

//TODO(aghassemi): We fail to bind to Unicode names, investigate.
//var ROW_KEY = 'چשKEYઑᜰ';
//var ROW_VAL = '⛓⛸VALϦӪ';
var ROW_KEY = 'row_key';
var ROW_VAL = 'row value';

test('table.create() creates a table', function(t) {
  setupDatabase(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.database;
    var table = db.table(uniqueName('table'));
    var table2 = db.table(uniqueName('table'));

    async.waterfall([
      // Verify table does not exist yet.
      table.exists.bind(table, o.ctx),
      function(exists, cb) {
        t.notok(exists, 'exists: table doesn\'t exist yet');
        cb(null);
      },

      // Verify table list is empty.
      db.listTables.bind(db, o.ctx),
      function(tableList, cb) {
        t.deepEqual(tableList, [],
          'listTables: no tables exist');
        cb(null);
      },

      // Create table.
      table.create.bind(table, o.ctx, {}),

      // Verify table exists.
      table.exists.bind(table, o.ctx),
      function(exists, cb) {
        t.ok(exists, 'exists: table exists');
        cb(null);
      },

      // Verify table list contains the table.
      db.listTables.bind(db, o.ctx),
      function(tableList, cb) {
        t.deepEqual(tableList, [table.name],
          'listTables: table exists');
        cb(null);
      },

      // Create another table.
      table2.create.bind(table2, o.ctx, {}),

      // Verify table list contains both tables.
      db.listTables.bind(db, o.ctx),
      function(tableList, cb) {
        t.deepEqual(tableList.sort(), [table.name, table2.name].sort(),
          'listTables: both tables exist');
        cb(null);
      },
    ], function(err, arg) {
      t.error(err);
      o.teardown(t.end);
    });
  });
});

test('table.destroy() destroys a table', function(t) {
  setupDatabase(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.database;
    var table = db.table(uniqueName('table'));

    async.waterfall([
      // Create table.
      table.create.bind(table, o.ctx, {}),

      // Verify table exists.
      table.exists.bind(table, o.ctx),
      function(exists, cb) {
        t.ok(exists, 'table exists');
        cb(null);
      },

      // Destroy table.
      table.destroy.bind(table, o.ctx),

      // Verify table no longer exists.
      table.exists.bind(table, o.ctx),
      function(exists, cb) {
        t.notok(exists, 'table no longer exists');
        cb(null);
      },
    ], function(err, arg) {
      t.error(err);
      o.teardown(t.end);
    });
  });
});

test('Destroying a table that does not exist should error', function(t) {
  setupDatabase(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.database;
    var tableName = uniqueName('table');

    db.table(tableName).destroy(o.ctx, function(err) {
      t.error(err);
      o.teardown(t.end);
    });
  });
});

test('Putting a string value in a row', function(t) {
  setupTable(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var key = uniqueName(ROW_KEY);
    var value = uniqueName(ROW_VAL);

    var table = o.table;
    table.put(o.ctx, key, value, function(err) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      table.get(o.ctx, key, function(err, val) {
        t.error(err);
        t.equals(val, value, 'put was successful');
        o.teardown(t.end);
      });
    });
  });
});

test('Deleting a row', function(t) {
  setupTable(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var key = uniqueName(ROW_KEY);
    var value = uniqueName(ROW_VAL);

    var table = o.table;
    var row = o.table.row(key);

    async.waterfall([
      // Verify row doesn't exist yet.
      row.exists.bind(row, o.ctx),
      function(exists, cb) {
        t.notok(exists, 'row doesn\'t exist yet');
        cb(null);
      },

      // Put row.
      table.put.bind(table, o.ctx, key, value),

      // Verify row exists.
      row.exists.bind(row, o.ctx),
      function(exists, cb) {
        t.ok(exists, 'row exists');
        cb(null);
      },

      // Delete row.
      table.delete.bind(table, o.ctx, key),

      // Verify row no longer exists.
      row.exists.bind(row, o.ctx),
      function(exists, cb) {
        t.notok(exists, 'row no longer exists');
        cb(null);
      },
    ], function(err, arg) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      table.get(o.ctx, key, function(err, val) {
        t.ok(err, 'get should error after row is deleted');
        o.teardown(t.end);
      });
    });
  });
});

test('Scanning table by single row', function(t) {
  setupTable(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var key = uniqueName(ROW_KEY);
    var value = uniqueName(ROW_VAL);

    var table = o.table;
    table.put(o.ctx, key, value, scan);

    function scan(err) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      var wantRows = [{
        key: key,
        value: value
      }];
      var range = syncbase.nosql.rowrange.singleRow(key);
      assertScanRows(o.ctx, table, range, wantRows, function(err) {
        t.error(err);
        o.teardown(t.end);
      });
    }
  });
});

test('Scanning table by a prefix range passed as string', function(t) {
  testScanningTableByPrefix(t, ROW_KEY);
});

test('Scanning table by a prefix range passed as RowRange', function(t) {
  var range = syncbase.nosql.rowrange.prefix(ROW_KEY);
  testScanningTableByPrefix(t, range);
});

function testScanningTableByPrefix(t, range) {
  setupTable(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var table = o.table;

    // create multiple rows all with ROW_KEY as prefix
    var prefixedRows = [{
      key: uniqueName(ROW_KEY),
      value: uniqueName(ROW_VAL)
    }, {
      key: uniqueName(ROW_KEY),
      value: uniqueName(ROW_VAL)
    }];

    // create multiple rows with a different prefix
    var otherRows = [{
      key: uniqueName('misc_row_keys'),
      value: uniqueName(ROW_VAL)
    }];

    var allRows = prefixedRows.concat(otherRows);
    async.forEach(allRows, function(pair, cb) {
      table.put(o.ctx, pair.key, pair.value, cb);
    }, scan);

    function scan(err) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      assertScanRows(o.ctx, table, range, prefixedRows, function(err) {
        t.error(err);
        o.teardown(t.end);
      });
    }
  });
}

test('Deleting rows by a prefix range passed as string', function(t) {
  testDeletingRowsByPrefix(t, ROW_KEY);
});

test('Deleting rows by a prefix range passed as RowRange', function(t) {
  var range = syncbase.nosql.rowrange.prefix(ROW_KEY);
  testDeletingRowsByPrefix(t, range);
});

function testDeletingRowsByPrefix(t, range) {
  setupTable(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var table = o.table;

    // create multiple rows all with ROW_KEY as prefix
    var rows = [{
      key: uniqueName(ROW_KEY),
      value: uniqueName(ROW_VAL)
    }, {
      key: uniqueName(ROW_KEY),
      value: uniqueName(ROW_VAL)
    }];

    async.forEach(rows, function(pair, cb) {
      table.put(o.ctx, pair.key, pair.value, cb);
    }, deleteRows);

    function deleteRows(err) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      table.deleteRange(o.ctx, range, scan);
    }

    function scan(err) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      var wantRows = [];
      assertScanRows(o.ctx, table, range, wantRows, function(err) {
        t.error(err);
        o.teardown(t.end);
      });
    }
  });
}

//TODO(aghassemi) Skipped test.
//Set permission for prefix != "" is not implemented.
test.skip('Getting/Setting permissions on rows', function(t) {
  setupTable(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var table = o.table;

    // create multiple rows with different suffixes
    var prefix1Rows = [{
      key: uniqueName('prefix1'),
      value: uniqueName(ROW_VAL)
    }];

    var prefix2Rows = [{
      key: uniqueName('prefix2'),
      value: uniqueName(ROW_VAL)
    }];

    var allRows = prefix1Rows.concat(prefix2Rows);
    async.forEach(allRows, function(pair, cb) {
      table.put(o.ctx, pair.key, pair.value, cb);
    }, setPermissions);

    function setPermissions(err) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      // set up different ACLS for different prefixes
      var prefix1Perms = new Map([
        ['Read', {
          'in': ['...', 'canRead1'],
          'notIn': ['cantRead1']
        }],
        ['Write', {
          'in': ['...', 'canWrite1'],
          'notIn': ['cantWrite1']
        }],
        ['Admin', {
          'in': ['...', 'canAdmin1'],
          'notIn': ['cantAdmin1']
        }]
      ]);

      var prefix2Perms = new Map([
        ['Read', {
          'in': ['...', 'canRead2'],
          'notIn': ['cantRead2']
        }],
        ['Write', {
          'in': ['...', 'canWrite2'],
          'notIn': ['cantWrite2']
        }],
        ['Admin', {
          'in': ['...', 'canAdmin2'],
          'notIn': ['cantAdmin2']
        }]
      ]);

      var prefix1 = syncbase.nosql.rowrange.prefix('prefix1');
      var prefix2 = syncbase.nosql.rowrange.prefix('prefix2');

      table.setPermissions(o.ctx, prefix1, prefix1Perms, function(err) {
        if (err) {
          t.error(err);
          return o.teardown(t.end);
        }

        table.setPermissions(o.ctx, prefix2, prefix2Perms, getPermissions);
      });

      function getPermissions(err) {
        if (err) {
          t.error(err);
          return o.teardown(t.end);
        }

        table.getPermissions(o.ctx, prefix1Rows[0].key, function(err, perms) {
          if (err) {
            t.error(err);
            return o.teardown(t.end);
          }

          t.deepEquals(perms.prefix, prefix1);
          t.deepEquals(perms.perms, prefix1Perms);

          table.getPermissions(o.ctx, prefix2Rows[0].key, function(err, perms) {
            if (err) {
              t.error(err);
              return o.teardown(t.end);
            }

            t.deepEquals(perms.prefix, prefix2);
            t.deepEquals(perms.perms, prefix2Perms);
            o.teardown(t.end);
          });
        });
      }
    }
  });
});
