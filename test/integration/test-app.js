// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var async = require('async');
var test = require('prova');
var vanadium = require('vanadium');

var syncbase = require('../..');

var testUtils = require('./utils');
var setup = testUtils.setupService;
var uniqueName = testUtils.uniqueName;

var DEFAULT_PERMISSIONS = new Map([
  ['Read', {
    'in': ['...'],
    'notIn': []
  }],
  ['Write', {
    'in': ['...'],
    'notIn': []
  }],
  ['Admin', {
    'in': ['...'],
    'notIn': []
  }]
]);

/*
 * TODO(aghassemi) We should refactor some of the testing functionality,
 * specially around verifying children and setting/getting permissions, into a
 * common util as these types of test will be common across different layers.
 */

test('Creating a service and checking its full name', function(t) {
  var mockServiceName = 'foo/bar/baz';

  var service = syncbase.newService(mockServiceName);
  t.equals(service.fullName, mockServiceName, 'Service name matches');
  t.end();
});

test('Getting a handle to an app', function(t) {
  setup(t, function(err, o) {
    if (err) {
      return t.end(err, 'Failed to setup');
    }

    var appName = uniqueName('app');

    var app = o.service.app(appName);

    t.equals(app.name, appName, 'App name matches');
    t.equals(app.fullName, vanadium.naming.join(o.service.fullName, appName),
      'App full name matches');

    o.teardown(t.end);
  });
});

test('Creating and listing apps', function(t) {
  setup(t, function(err, o) {
    if (err) {
      return t.end(err, 'Failed to setup');
    }

    // Create multiple apps
    var expectedAppNames = [
      uniqueName('app'),
      uniqueName('app'),
      uniqueName('app')
    ];

    createAppsAndVerifyExistance(t, o.service, o.ctx, expectedAppNames,
      function() {
        o.teardown(t.end);
      }
    );
  });
});

test('Deleting an app', function(t) {
  setup(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var appName = uniqueName('app');

    createAppsAndVerifyExistance(t, o.service, o.ctx, [appName], deleteApp);

    function deleteApp() {
      o.service.app(appName).delete(o.ctx, verifyItNoLongerExists);
    }

    function verifyItNoLongerExists(err) {
      if (err) {
        t.fail(err, 'Failed to delete app');
        return o.teardown(t.end);
      }

      o.service.listApps(o.ctx, function(err, apps) {
        if (err) {
          t.fail(err, 'Failed to list apps');
          return o.teardown(t.end);
        }

        t.ok(apps.indexOf(appName) < 0, 'App is no longer listed');
        return o.teardown(t.end);
      });
    }
  });
});

test('Getting permissions of an app', function(t) {
  setup(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var appName = uniqueName('app');

    createAppsAndVerifyExistance(t, o.service, o.ctx, [appName],
                                 getPermissions);

    function getPermissions() {
      o.service.app(appName).getPermissions(o.ctx, verifyPermissions);
    }

    function verifyPermissions(err, perms, version) {
      if (err) {
        t.fail(err, 'Failed to get permissions for app');
        return o.teardown(t.end);
      }

      t.equal(perms.size, DEFAULT_PERMISSIONS.size,
        'Permissions size matches');
      DEFAULT_PERMISSIONS.forEach(function(value, key) {
        t.deepEqual(perms.get(key), value, 'Permission value matches');
      });
      t.equal(version, '0', 'Version matches');

      return o.teardown(t.end);
    }
  });
});

test('Setting permissions of an app', function(t) {
  setup(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var appName = uniqueName('app');
    var NEW_PERMS = new Map([
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

    createAppsAndVerifyExistance(t, o.service, o.ctx, [appName],
                                 setPermissions);

    function setPermissions() {
      o.service.app(appName)
        .setPermissions(o.ctx, NEW_PERMS, '0', getPermissions);
    }

    function getPermissions(err) {
      if (err) {
        t.fail(err, 'Failed to set permissions for app');
        return o.teardown(t.end);
      }
      o.service.app(appName).getPermissions(o.ctx, verifyPermissions);
    }

    function verifyPermissions(err, perms, version) {
      if (err) {
        t.fail(err, 'Failed to get permissions for app');
        return o.teardown(t.end);
      }

      t.equal(perms.size, NEW_PERMS.size,
        'Permissions size matches');
      NEW_PERMS.forEach(function(value, key) {
        t.deepEqual(perms.get(key), value, 'Permission value matches');
      });
      // Version should have been incremented after setPermission call
      t.equal(version, '1', 'Version matches');

      return o.teardown(t.end);
    }
  });
});

// Helper function that creates bunch apps in parallel and calls the callback
// when all are created.
function createAppsAndVerifyExistance(t, service, ctx, appNames, cb) {
  async.parallel(create(), verify);

  // Returns an array of functions that create apps for the given appNames.
  function create() {
    return appNames.map(function(appName) {
      return function(callback) {
        service.app(appName).create(ctx, DEFAULT_PERMISSIONS, callback);
      };
    });
  }

  function verify(err) {
    if (err) {
      t.fail('Failed to create apps');
      return cb(err);
    }

    service.listApps(ctx, verifyResults);

    function verifyResults(err, apps) {
      if (err) {
        t.fail(err, 'Failed to list apps');
        return cb(err);
      }

      var matchCounter = 0;
      appNames.forEach(function(appName) {
        if (apps.indexOf(appName) >= 0) {
          matchCounter++;
        }
      });

      var diff = appNames.length - matchCounter;
      if (diff === 0) {
        t.pass('All ' + matchCounter + ' expected app name(s) were listed');
        return cb();
      } else {
        var failedErr = new Error(
          'Some (' + diff + ') expected app name(s) were not listed'
        );
        t.fail(failedErr);
        return cb(failedErr);
      }
    }
  }
}
