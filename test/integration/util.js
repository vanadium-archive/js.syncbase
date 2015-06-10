// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

module.exports = {
  appExists: appExists,
  databaseExists: databaseExists,
  tableExists: tableExists,

  setupApp: setupApp,
  setupDatabase: setupDatabase,
  setupService: setupService,
  setupTable: setupTable,

  uniqueName: uniqueName,

  testGetSetPermissions: testGetSetPermissions
};

var vanadium = require('vanadium');
var extend = require('xtend');

var syncbase = require('../..');

var SERVICE_NAME = require('./service-name');

// Helper function to generate unique names.
var nameCounter = 0;

function uniqueName(prefix) {
  prefix = prefix || 'name';
  return prefix + '_' + nameCounter++;
}

// Initializes Vanadium runtime.
function setupService(t, cb) {
  vanadium.init(function(err, rt) {
    if (err) {
      return cb(err);
    }

    function teardown(cb) {
      rt.close(function(err) {
        t.error(err, 'rt.close should not error.');
        cb(null);
      });
    }

    var service = syncbase.newService(SERVICE_NAME);

    return cb(null, {
      ctx: rt.getContext(),
      rt: rt,
      service: service,
      teardown: teardown
    });
  });
}

// Initializes Vanadium runtime and creates an App.
function setupApp(t, cb) {
  setupService(t, function(err, o) {
    if (err) {
      return cb(err);
    }

    var app = o.service.app(uniqueName('app'));

    app.create(o.ctx, {}, function(err) {
      if (err) {
        o.rt.close(t.error);
        return cb(err);
      }

      return cb(null, extend(o, {
        app: app
      }));
    });
  });
}

// Initializes Vanadium runtime and creates an App and a Database.
function setupDatabase(t, cb) {
  setupApp(t, function(err, o) {
    if (err) {
      return cb(err);
    }

    var db = o.app.noSqlDatabase(uniqueName('db'));

    db.create(o.ctx, {}, function(err) {
      if (err) {
        o.rt.close(t.error);
        return cb(err);
      }

      return cb(null, extend(o, {
        database: db
      }));
    });
  });
}

// Initializes Vanadium runtime and creates an App, a Database and a Table.
function setupTable(t, cb) {
  setupDatabase(t, function(err, o) {
    if (err) {
      return cb(err);
    }
    var db = o.database;

    var tableName = uniqueName('table');
    db.createTable(o.ctx, tableName, {}, function(err) {
      if (err) {
        o.rt.close(t.error);
        return cb(err);
      }

      return cb(null, extend(o, {
        table: db.table(tableName)
      }));
    });
  });
}

// Assert that two permissions objects are equal.
function assertPermissionsEqual(t, got, want) {
  t.equal(got.size, want.size, 'Permissions size matches');
  want.forEach(function(value, key) {
    t.deepEqual(got.get(key), value, 'Permission value matches');
  });
}

// For any object that implements get/setPermissions, test that getting and
// setting permissions behaves as it should.
function testGetSetPermissions(t, ctx, obj, cb) {
  obj.getPermissions(ctx, function(err, perms, version) {
    if (err) {
      t.error('error getting permissions ' + err);
      return cb(err);
    }

    t.ok(perms, 'Has permissions');
    t.ok(version, 'Has a version');

    var newPerms = new Map([
      ['Read', {
        'in': ['...', 'canRead'],
        'notIn': ['cantRead']
      }],
      ['Write', {
        'in': ['...', 'canWrite'],
        'notIn': ['cantWrite']
      }],
      ['Admin', {
        'in': ['...', 'canAdmin'],
        'notIn': ['cantAdmin']
      }]
    ]);

    obj.setPermissions(ctx, newPerms, version, function(err) {
      if (err) {
        t.error('error setting permissions ' + err);
        return cb(err);
      }

      obj.getPermissions(ctx, function(err, gotPerms, gotVersion) {
        if (err) {
          t.error('error getting permissions ' + err);
          return cb(err);
        }

        t.ok(perms, 'Has permissions');
        t.ok(version, 'Has a version');

        t.notEqual(version, gotVersion, 'should have a new version');
        assertPermissionsEqual(t, gotPerms, newPerms);
        return cb(null);
      });
    });
  });
}

function appExists(ctx, service, name, cb) {
  service.listApps(ctx, function(err, names) {
    if (err) {
      return cb(err);
    }

    cb(null, names.indexOf(name) >= 0);
  });
}

function databaseExists(ctx, app, name, cb) {
  app.listDatabases(ctx, function(err, names) {
    if (err) {
      return cb(err);
    }

    cb(null, names.indexOf(name) >= 0);
  });
}

function tableExists(ctx, db, name, cb) {
  db.listTables(ctx, function(err, names) {
    if (err) {
      return cb(err);
    }

    cb(null, names.indexOf(name) >= 0);
  });
}