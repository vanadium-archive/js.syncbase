// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var naming = require('vanadium').naming;
var test = require('prova');
var vanadium = require('vanadium');

var Database = require('../../src/nosql/database');
var Table = require('../../src/nosql/table');

var testUtil = require('./util');
var databaseExists = testUtil.databaseExists;
var tableExists = testUtil.tableExists;
var setupApp = testUtil.setupApp;
var setupDatabase = testUtil.setupDatabase;
var uniqueName = testUtil.uniqueName;

test('app.noSqlDatabase() returns a database', function(t) {
  setupApp(t, function(err, o) {
    if (err) {
      return  t.end(err);
    }

    var dbName = uniqueName('db');
    var db = o.app.noSqlDatabase(dbName);

    t.ok(db, 'Database is constructed.');
    t.ok(db instanceof Database, 'database is a Database object.');
    t.equal(db.name, dbName, 'Database has the correct name.');

    db.name = 'foo';
    t.equal(db.name, dbName, 'Setting the name has no effect.');

    var expectedFullName = naming.join(o.app.fullName, dbName);
    t.equal(db.fullName, expectedFullName, 'Database has correct fullName.');

    db.fullName = 'bar';
    t.equal(db.fullName, expectedFullName, 'Setting fullName has no effect.');

    o.teardown(t.end);
  });
});

test('app.noSqlDatabase with slashes in the name', function (t) {
  setupApp(t, function(err, o) {
    if (err) {
      return  t.end(err);
    }

    var dbName = 'bad/name';
    t.throws(function() {
      o.app.noSqlDatabase(dbName);
    }, 'should throw');

    o.teardown(t.end);
  });
});

test('db.create() creates a database', function(t) {
  setupApp(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.app.noSqlDatabase(uniqueName('db'));

    db.create(o.ctx, {}, function(err) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      databaseExists(o.ctx, o.app, db.name, function(err, exists) {
        t.error(err);
        t.ok(exists, 'database exists');
        o.teardown(t.end);
      });
    });
  });
});

test('creating a database twice should error', function(t) {
  setupApp(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.app.noSqlDatabase(uniqueName('db'));

    // Create once.
    db.create(o.ctx, {}, function(err) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      // Create again.
      db.create(o.ctx, {}, function(err) {
        t.ok(err, 'should error.');
        o.teardown(t.end);
      });
    });
  });
});

test('db.delete() deletes a database', function(t) {
  setupApp(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.app.noSqlDatabase(uniqueName('db'));

    db.create(o.ctx, {}, function(err) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      db.delete(o.ctx, function(err) {
        if (err) {
          t.error(err);
          return o.teardown(t.end);
        }

        databaseExists(o.ctx, o.app, db.name, function(err, exists) {
          t.error(err);
          t.notok(exists, 'database does not exist');
          o.teardown(t.end);
        });
      });
    });
  });
});

test('deleting a db that has not been created should error', function(t) {
  setupApp(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.app.noSqlDatabase(uniqueName('db'));

    db.delete(o.ctx, function(err) {
      t.ok(err, 'should error');
      o.teardown(t.end);
    });
  });
});

test('db.table() returns a table', function(t) {
  setupApp(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.app.noSqlDatabase(uniqueName('db'));
    var tableName = uniqueName('table');
    var table = db.table(tableName);

    t.ok(table, 'table is created.');
    t.ok(table instanceof Table, 'table is a Table object.');
    t.equal(table.name, tableName, 'table has the correct name.');
    t.equal(table.fullName, vanadium.naming.join(db.fullName, tableName),
            'table has the correct fullName.');

    o.teardown(t.end);
  });
});

test('db.createTable() creates a table', function(t) {
  setupDatabase(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.database;

    var tableName = uniqueName('table');
    db.createTable(o.ctx, tableName, {}, function(err) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      tableExists(o.ctx, db, tableName, function(err, exists) {
        t.error(err);
        t.ok(exists, 'table exists');
        o.teardown(t.end);
      });
    });
  });
});

test('db.deleteTable() deletes a table', function(t) {
  setupDatabase(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.database;

    var tableName = uniqueName('table');
    db.createTable(o.ctx, tableName, {}, function(err) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      db.deleteTable(o.ctx, tableName, function(err) {
        if (err) {
          t.error(err);
          return o.teardown(t.end);
        }

        tableExists(o.ctx, db, tableName, function(err, exists) {
          t.error(err);
          t.notok(exists, 'table does not exist');
          o.teardown(t.end);
        });
      });
    });
  });
});

test('deleting a table that does not exist should error', function(t) {
  setupDatabase(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.database;
    var tableName = uniqueName('table');

    db.deleteTable(o.ctx, tableName, function(err) {
      t.ok(err, 'should error.');
      o.teardown(t.end);
    });
  });
});

test('Getting/Setting permissions of a database', function(t) {
  setupDatabase(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    testUtil.testGetSetPermissions(t, o.ctx, o.database, function(err) {
      t.error(err);
      return o.teardown(t.end);
    });
  });
});
