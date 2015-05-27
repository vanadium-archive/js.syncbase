// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var vanadium = require('vanadium');
var extend = require('xtend');

var syncbase = require('../..');

var SERVICE_NAME = require('./service-name');

// Helper function to generate unique names.
var uniqueName = (function() {
  var i = 0;
  return function(prefix) {
    prefix = prefix || 'name';
    i++;
    return prefix + '_' + i;
  };
})();

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
        app:app
      }));
    });
  });
}

// Initializes Vanadium runtime and creates an App and a Database.
function setupDatabase(t, cb) {
  setupApp(t, function(err, o) {

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

module.exports = {
  setupApp: setupApp,
  setupDatabase: setupDatabase,
  setupService: setupService,
  uniqueName: uniqueName
};
