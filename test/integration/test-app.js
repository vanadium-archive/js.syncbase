// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var async = require('async');
var test = require('prova');
var vanadium = require('vanadium');

var syncbase = require('../..');

var testUtil = require('./util');
var setupApp = testUtil.setupApp;
var setupService = testUtil.setupService;
var uniqueName = testUtil.uniqueName;

test('Creating a service and checking its full name', function(t) {
  var mockServiceName = 'foo/bar/baz';

  var service = syncbase.newService(mockServiceName);
  t.equals(service.fullName, mockServiceName, 'Service name matches');
  t.end();
});

test('Getting a handle to an app', function(t) {
  setupService(t, function(err, o) {
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
  setupService(t, function(err, o) {
    if (err) {
      return t.end(err, 'Failed to setup');
    }

    // Create multiple apps.
    var appNames = [
      uniqueName('app'),
      uniqueName('app'),
      uniqueName('app')
    ];

    async.waterfall([
      // Verify none of the apps exist using exists().
      async.apply(async.map, appNames, function(appName, cb) {
        o.service.app(appName).exists(o.ctx, cb);
      }),
      function(existsArray, cb) {
        t.deepEqual(existsArray, [false, false, false],
          'exists: no apps exist');
        cb(null);
      },

      // Verify none of the apps exist using listApps().
      o.service.listApps.bind(o.service, o.ctx),
      function(appList, cb) {
        t.deepEqual(appList, [],
          'listApps: no apps exist');
        cb(null);
      },

      // Create all apps.
      async.apply(async.forEach, appNames, function(appName, cb) {
        o.service.app(appName).create(o.ctx, {}, cb);
      }),

      // Verify each app exists using exists().
      async.apply(async.map, appNames, function(appName, cb) {
        o.service.app(appName).exists(o.ctx, cb);
      }),
      function(existsArray, cb) {
        t.deepEqual(existsArray, [true, true, true],
          'exists: all apps exist');
        cb(null);
      },

      // Verify all the apps exist using listApps().
      o.service.listApps.bind(o.service, o.ctx),
      function(appList, cb) {
        t.deepEqual(appList.sort(), appNames.sort(),
          'listApps: all apps exist');
        cb(null);
      }
    ], function(err) {
      t.error(err);
      o.teardown(t.end);
    });
  });
});

test('Destroy an app', function(t) {
  setupApp(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    async.waterfall([
      // Verify app exists.
      o.app.exists.bind(o.app, o.ctx),
      function(exists, cb) {
        t.ok(exists, 'app exists');
        cb(null);
      },

      // Destroy app.
      o.app.destroy.bind(o.app, o.ctx),

      // Verify app no longer exists.
      o.app.exists.bind(o.app, o.ctx),
      function(exists, cb) {
        t.notok(exists, 'app no longer exists');
        cb(null);
      }
    ], function(err, arg) {
      t.error(err);
      o.teardown(t.end);
    });
  });
});

test('Getting/Setting permissions of an app', function(t) {
  setupApp(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    testUtil.testGetSetPermissions(t, o.ctx, o.app, function(err) {
      t.error(err);
      return o.teardown(t.end);
    });
  });
});
