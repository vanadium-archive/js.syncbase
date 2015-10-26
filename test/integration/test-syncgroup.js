// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var async = require('async');
var naming = require('vanadium').naming;
var test = require('prova');

var syncbase = require('../..');
var nosql = syncbase.nosql;
var syncbaseSuffix = syncbase.syncbaseSuffix;
var Syncgroup = require('../../src/nosql/syncgroup');
var verror = require('vanadium').verror;

var testUtil = require('./util');
var setupDatabase = testUtil.setupDatabase;
var setupSyncgroup = testUtil.setupSyncgroup;
var uniqueName = testUtil.uniqueName;

// TODO(nlacasse): Where does this magic number 8 come from? It's in
// syncgroup_test.go.
var myInfo = new nosql.SyncgroupMemberInfo({
  syncPriority: 8
});

test('db.syncgroup returns a syncgroup with name', function(t) {
  setupDatabase(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.database;

    var sgName = uniqueName('syncgroup');
    var sg = db.syncgroup(sgName);
    t.ok(sg instanceof Syncgroup, 'syncgroup is instanceof Syncgroup');
    t.equal(sg.name, sgName, 'syncgroup has correct name');
    o.teardown(t.end);
  });
});

test('syncgroup.create with empty spec', function(t) {
  setupDatabase(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.database;
    var ctx = o.ctx;

    var spec = new nosql.SyncgroupSpec();
    var name = uniqueName('syncgroup');

    db.syncgroup(name).create(ctx, spec, myInfo, function(err) {
      t.ok(err, 'should error');
      t.ok(err instanceof verror.BadArgError, 'err is BadArgError');
      o.teardown(t.end);
    });
  });
});

test('syncgroup.create with valid spec', function(t) {
  var perms = {};
  var prefixes = [
    new nosql.TableRow({tableName: 't1', row: 'foo'})
  ];

  setupDatabase(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.database;
    var ctx = o.ctx;

    // TODO(nlacasse): It's not obvious that the syncgroup name needs to be
    // appended to a syncbase service name.
    var name = naming.join(o.service.fullName,
                           syncbaseSuffix,
                           uniqueName('syncgroup'));

    var spec = new nosql.SyncgroupSpec({
      description: 'test syncgroup ' + name,
      perms: perms,
      prefixes: prefixes
    });

    db.syncgroup(name).create(ctx, spec, myInfo, function(err) {
      t.error(err, 'should not error');
      o.teardown(t.end);
    });
  });
});

test('creating a nested syncgroup', function(t) {
  var perms = {};
  var prefixes = [
    new nosql.TableRow({tableName: 't1', row: 'foo'})
  ];
  var prefixes2 = [
    new nosql.TableRow({tableName: 't1', row: 'foobar'})
  ];

  setupSyncgroup(t, perms, prefixes, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.database;
    var ctx = o.ctx;

    // TODO(nlacasse): It's not obvious that the syncgroup name needs to be
    // appended to a syncbase service name.
    var name = naming.join(o.service.fullName,
                           syncbaseSuffix,
                           uniqueName('syncgroup'));

    var spec = new nosql.SyncgroupSpec({
      description: 'another syncgroup named ' + name,
      perms: perms,
      prefixes: prefixes2
    });

    var sg2 = db.syncgroup(name);
    sg2.create(ctx, spec, myInfo, function(err) {
      t.error(err, 'should not error');
      o.teardown(t.end);
    });
  });
});

test('creating a syncgroup that already exists', function(t) {
  var perms = {};
  var prefixes = [
    new nosql.TableRow({tableName: 't1', row: 'foo'})
  ];
  var prefixes2 = [
    new nosql.TableRow({tableName: 'another', row: 'prefix'})
  ];

  setupSyncgroup(t, perms, prefixes, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.database;
    var ctx = o.ctx;

    var name = o.syncgroup.name;

    var spec = new nosql.SyncgroupSpec({
      description: 'another syncgroup named ' + name,
      perms: perms,
      prefixes: prefixes2
    });

    var sg2 = db.syncgroup(name);
    sg2.create(ctx, spec, myInfo, function(err) {
      t.ok(err, 'should error');
      t.ok(err instanceof verror.ExistError, 'err is ExistError');
      o.teardown(t.end);
    });
  });
});

test('syncgroup.join succeeds if user has Read access', function(t) {
  var perms = new Map([
    ['Read', {
      'in': ['...']
    }]
  ]);
  var prefixes = [
    new nosql.TableRow({tableName: 't1', row: 'foo'})
  ];

  setupSyncgroup(t, perms, prefixes, function(err, o) {
    if (err) {
      return t.end(err);
    }

    o.syncgroup.join(o.ctx, myInfo, function(err) {
      t.error(err, 'should not error');
      o.teardown(t.end);
    });
  });
});

test('syncgroup.join fails if user does not have Read access', function(t) {
  var perms = new Map([
    ['Read', {
      'in': ['some/blessing/name']
    }]
  ]);
  var prefixes = [
    new nosql.TableRow({tableName: 't1', row: 'foo'})
  ];

  setupSyncgroup(t, perms, prefixes, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var sg = o.syncgroup;
    var ctx = o.ctx;

    sg.join(ctx, myInfo, function(err) {
      t.ok(err, 'should not error');
      t.ok(err instanceof verror.NoAccessError, 'err is NoAccessError');
      o.teardown(t.end);
    });
  });
});

// TODO(nlacasse): Enable this test once Syncbase server implements
// Database.GetSyncgroupNames.
test.skip('db.getSyncgroupNames returns the correct names', function(t) {
  setupDatabase(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var ctx = o.ctx;
    var db = o.database;

    var names = [
      uniqueName('syncgroup'),
      uniqueName('syncgroup'),
      uniqueName('syncgroup')
    ];

    var fullNames = names.map(function(name) {
      return naming.join(o.service.fullName,
                         syncbaseSuffix,
                         name);
    });

    createSyncgroups();

    function createSyncgroups() {
      async.forEach(fullNames, function(fullName, cb) {
        var spec = new nosql.SyncgroupSpec({
          description: 'syncgroup named ' + fullName,
          prefixes: [new nosql.TableRow({tableName: '', row: ''})]
        });

        db.syncgroup(fullName).create(ctx, spec, myInfo, cb);
      }, getSyncgroupNames);
    }

    function getSyncgroupNames(err) {
      if (err) {
        t.error(err);
        o.teardown(t.end);
      }

      db.getSyncgroupNames(ctx, assertSyncgroupNames);
    }

    function assertSyncgroupNames(err, gotNames) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      fullNames.sort();
      gotNames.sort();
      t.deepEqual(fullNames, gotNames, 'syncgroup names are correct');
      o.teardown(t.end);
    }
  });
});

test('syncgroup.get/setSpec', function(t) {
  var perms = {};
  var prefixes = [
    new nosql.TableRow({tableName: 'biz', row: 'bazz'})
  ];

  var firstVersion;

  var newSpec = new nosql.SyncgroupSpec({
    description: 'new spec',
    prefixes: prefixes
  });

  var newSpec2 = new nosql.SyncgroupSpec({
    description: 'another new spec',
    prefixes: prefixes
  });

  setupSyncgroup(t, perms, prefixes, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var sg = o.syncgroup;
    var ctx = o.ctx;
    sg.getSpec(ctx, assertSpec);

    function assertSpec(err, spec, version) {
      if (err) {
        return done(err);
      }

      firstVersion = version;

      t.deepEqual(spec.perms, perms, 'sg has correct perms');
      t.deepEqual(spec.prefixes, prefixes, 'sg has correct prefixes');
      t.equal(typeof version, 'string', 'version is string');

      // Set spec with bogus version.
      var bogusVersion = 'totally-bogus';

      sg.setSpec(ctx, newSpec, bogusVersion, assertSetSpecFails);
    }

    function assertSetSpecFails(err) {
      // TODO(nlacasse): Syncbase does not currently enforce that the version
      // sent on SetSpec matches the current version.  Once it does enforce
      // this, the following assertion should be uncommented.
      // t.ok(err, 'setting spec with bogus version should fail');

      // Set spec with empty version.
      sg.setSpec(ctx, newSpec, '', assertSetSpecSucceeds);
    }

    function assertSetSpecSucceeds(err) {
      if (err) {
        return done(err);
      }

      sg.getSpec(ctx, assertGetSpec);
    }

    function assertGetSpec(err, spec, version) {
      if (err) {
        return done(err);
      }

      t.equal(spec.name, newSpec.name, 'spec has the correct name');
      t.equal(typeof version, 'string', 'version is string');

      // Set spec with previous version.
      sg.setSpec(ctx, newSpec2, version, done);
    }

    function done(err) {
      t.error(err);
      o.teardown(t.end);
    }
  });
});
