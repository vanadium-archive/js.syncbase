// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var async = require('async');
var streamToArray = require('stream-to-array');
var test = require('prova');

var testUtil = require('./util');
var syncbase = require('../..');

var setupTable = testUtil.setupTable;
var uniqueName = testUtil.uniqueName;

//TODO(aghassemi): We fail to bind to Unicode names, investigate.
//var ROW_KEY = 'چשKEYઑᜰ';
//var ROW_VAL = '⛓⛸VALϦӪ';
var ROW_KEY = 'row_key';
var ROW_VAL = 'row value';

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
    table.put(o.ctx, key, value, deleteRow);

    function deleteRow() {
      table.row(key).delete(o.ctx, function() {
        table.get(o.ctx, key, function(err, val) {
          t.ok(err, 'get should error after row is deleted');
          o.teardown(t.end);
        });
      });
    }
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

      var range = syncbase.nosql.rowrange.singleRow(key);
      var stream = table.scan(o.ctx, range, function(err) {
        if (err) {
          t.error(err);
          return o.teardown(t.end);
        }
      });

      streamToArray(stream, function(err, values) {
        if (err) {
          t.error(err);
          return o.teardown(t.end);
        }

        var row = values[0];
        t.ok(row, 'row exists');
        t.deepEquals(row.key, key, 'got expected key from scan');
        t.deepEquals(row.value, value, 'got expected value from scan');
        o.teardown(t.end);
      });
    }
  });
});

test('Scanning table by a prefix range', function(t) {
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

      var range = syncbase.nosql.rowrange.prefix(ROW_KEY);
      var stream = table.scan(o.ctx, range, function(err) {
        if (err) {
          t.error(err);
          return o.teardown(t.end);
        }
      });

      streamToArray(stream, function(err, rows) {
        if (err) {
          t.error(err);
          return o.teardown(t.end);
        }

        t.ok(rows, 'got some rows');
        t.deepEquals(rows.sort(), prefixedRows.sort(),
          'got expected results from scan');
        o.teardown(t.end);
      });
    }
  });
});

test('Deleting rows by a prefix range', function(t) {
  setupTable(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var table = o.table;
    var range = syncbase.nosql.rowrange.prefix(ROW_KEY);

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

      table.delete(o.ctx, range, scan);
    }

    function scan(err) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      var stream = table.scan(o.ctx, range, function(err) {
        if (err) {
          t.error(err);
          return o.teardown(t.end);
        }
      });

      streamToArray(stream, function(err, rows) {
        if (err) {
          t.error(err);
          return o.teardown(t.end);
        }

        t.deepEquals(rows, [], 'Rows were deleted successfully');
        o.teardown(t.end);
      });
    }
  });
});

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