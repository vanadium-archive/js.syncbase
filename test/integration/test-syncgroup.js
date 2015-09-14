// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var async = require('async');
var naming = require('vanadium').naming;
var test = require('prova');

var syncbase = require('../..');
var nosql = syncbase.nosql;
var syncbaseSuffix = syncbase.syncbaseSuffix;
var SyncGroup = require('../../src/nosql/syncgroup');
var verror = require('vanadium').verror;

var testUtil = require('./util');
var setupDatabase = testUtil.setupDatabase;
var setupSyncGroup = testUtil.setupSyncGroup;
var uniqueName = testUtil.uniqueName;

// TODO(nlacasse): Where does this magic number 8 come from? It's in
// syncgroup_test.go.
var myInfo = new nosql.SyncGroupMemberInfo({
  syncPriority: 8
});

test('db.syncGroup returns a SyncGroup with name', function(t) {
  setupDatabase(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.database;

    var sgName = uniqueName('syncgroup');
    var sg = db.syncGroup(sgName);
    t.ok(sg instanceof SyncGroup, 'syncgroup is instanceof SyncGroup');
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

    var spec = new nosql.SyncGroupSpec();
    var name = uniqueName('syncgroup');

    db.syncGroup(name).create(ctx, spec, myInfo, function(err) {
      t.ok(err, 'should error');
      t.ok(err instanceof verror.BadArgError, 'err is BadArgError');
      o.teardown(t.end);
    });
  });
});

test('syncgroup.create with valid spec', function(t) {
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
    var prefix = 't1/foo';

    var spec = new nosql.SyncGroupSpec({
      description: 'test syncgroup ' + name,
      perms: {},
      prefixes: [prefix]
    });

    db.syncGroup(name).create(ctx, spec, myInfo, function(err) {
      t.error(err, 'should not error');
      o.teardown(t.end);
    });
  });
});

test('creating a nested syncgroup', function(t) {
  var perms = {};
  var prefixes = ['t1/foo'];

  setupSyncGroup(t, perms, prefixes, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.database;
    var ctx = o.ctx;

    var prefixes2 = ['t1/foobar'];

    // TODO(nlacasse): It's not obvious that the syncgroup name needs to be
    // appended to a syncbase service name.
    var name = naming.join(o.service.fullName,
                           syncbaseSuffix,
                           uniqueName('syncgroup'));

    var spec = new nosql.SyncGroupSpec({
      description: 'another syncgroup named ' + name,
      perms: {},
      prefixes: prefixes2
    });

    var sg2 = db.syncGroup(name);
    sg2.create(ctx, spec, myInfo, function(err) {
      t.error(err, 'should not error');
      o.teardown(t.end);
    });
  });
});

test('creating a syncgroup that already exists', function(t) {
  var perms = {};
  var prefixes = ['t1/foo'];

  setupSyncGroup(t, perms, prefixes, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var db = o.database;
    var ctx = o.ctx;

    var name = o.syncgroup.name;

    var spec = new nosql.SyncGroupSpec({
      description: 'another syncgroup named ' + name,
      perms: {},
      prefixes: ['another/prefix']
    });

    var sg2 = db.syncGroup(name);
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
  var prefixes = ['t1/foo'];

  setupSyncGroup(t, perms, prefixes, function(err, o) {
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
  var prefixes = ['t1/foo'];

  setupSyncGroup(t, perms, prefixes, function(err, o) {
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
// Database.GetSyncGroupNames.
test.skip('db.getSyncGroupNames returns the correct names', function(t) {
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

    createSyncGroups();

    function createSyncGroups() {
      async.forEach(fullNames, function(fullName, cb) {
        var spec = new nosql.SyncGroupSpec({
          description: 'syncgroup named ' + fullName,
          prefixes: ['']
        });

        db.syncGroup(fullName).create(ctx, spec, myInfo, cb);
      }, getSyncGroupNames);
    }

    function getSyncGroupNames(err) {
      if (err) {
        t.error(err);
        o.teardown(t.end);
      }

      db.getSyncGroupNames(ctx, assertSyncGroupNames);
    }

    function assertSyncGroupNames(err, gotNames) {
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
  var prefixes = ['biz/bazz'];

  var firstVersion;

  var newSpec = new nosql.SyncGroupSpec({
    description: 'new spec',
    prefixes: ['a']
  });

  var newSpec2 = new nosql.SyncGroupSpec({
    description: 'another new spec',
    prefixes: ['b']
  });

  setupSyncGroup(t, perms, prefixes, function(err, o) {
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
