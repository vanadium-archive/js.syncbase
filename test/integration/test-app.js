// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var async = require('async');
var test = require('prova');
var vanadium = require('vanadium');

var syncbase = require('../..');

var testUtil = require('./util');
var appExists = testUtil.appExists;
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
    async.forEach(appNames, function(appName, cb) {
      o.service.app(appName).create(o.ctx, {}, cb);
    }, function(err) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      // Verify each app exists.
      async.map(appNames, function(appName, cb) {
        appExists(o.ctx, o.service, appName, cb);
      }, function(err, existsArray) {
        t.error(err);
        t.deepEqual(existsArray, [true, true, true], 'all apps exist');
        o.teardown(t.end);
      });
    });
  });
});

test('Deleting an app', function(t) {
  setupApp(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    o.app.delete(o.ctx, function(err) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      appExists(o.ctx, o.service, o.app.name, function(err, exists) {
        t.error(err);
        t.notok(exists, 'app no longer exists');
        o.teardown(t.end);
      });
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
