// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var test = require('prova');
var vanadium = require('vanadium');

// TODO(nlacasse): These tests make RPCs directly to the syncbased server.
// Most users should use the syncbase client library instead of making RPCs
// directly. Get rid of these tests once we have tests that excercise the
// client library.

// Helper function to create a Vanadium runtime, context, and client.
function setup(t, cb) {
  vanadium.init(function(err, rt) {
    if (err) {
      return cb(err);
    }

    var ctx = rt.getContext();
    var client = rt.newClient();

    function teardown(cb) {
      rt.close(function(err) {
        t.error(err, 'rt.close should not error');
        cb(null);
      });
    }

    cb(null, {
      ctx: ctx,
      client: client,
      rt: rt,
      teardown: teardown
    });
  });
}

test('test bindTo syncbased', function(t) {
  setup(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    o.client.bindTo(o.ctx, 'test/syncbased', function(err, syncbase) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      t.ok(syncbase, 'syncbase service is defined');
      o.teardown(t.end);
    });
  });
});

test('test create app', function(t) {
  setup(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var appName = 'myCoolApp';

    o.client.bindTo(o.ctx, 'test/syncbased/' + appName, function(err, app) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      // TODO(nlacasse): Test with a real permissions map.
      var perms = new Map();

      app.create(o.ctx, perms, function(err) {
        t.error(err);
        o.teardown(t.end);
      });
    });
  });
});
