// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var async = require('async');
var test = require('prova');

var testUtil = require('./util');
var setupApp = testUtil.setupApp;
var uniqueName = testUtil.uniqueName;

var nosql = require('../..').nosql;
var Schema = nosql.Schema;
var SchemaMetadata = nosql.SchemaMetadata;

test('schema check', function(t) {
  setupApp(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var app = o.app;
    var ctx = o.ctx;

    var dbName = uniqueName('db');

    var upgraderCallCount = 0;
    var upgrader = function(db, oldVer, newVer, cb) {
      upgraderCallCount++;
      process.nextTick(function() {
        cb(null);
      });
    };

    var version = 123;
    var md = new SchemaMetadata({version: version});
    var schema = new Schema(md, upgrader);

    var otherDb, otherSchema, newVersion;

    var db = app.noSqlDatabase(dbName, schema);

    async.waterfall([
      // Verify that calling Upgrade on a non existing database does not throw
      // errors.
      db.upgradeIfOutdated.bind(db, ctx),
      function(upgraded, cb) {
        t.equal(upgraded, false, 'upgradeIfOutdated should return false');
        t.equal(upgraderCallCount, 0,
                'upgrader function should not have been called');
        cb(null);
      },

      // Create db, this step also stores the schema provided above
      db.create.bind(db, ctx, new Map()),
      // Verify schema was stored as part of create.
      function(cb) {
        cb(null);
      },

      db._getSchemaMetadata.bind(db, ctx),

      function(metadata, cb) {
        t.equal(metadata.version, version, 'metadata has correct version');
        cb(null);
      },

      // Make redundant call to Upgrade to verify that it is a no-op
      db.upgradeIfOutdated.bind(db, ctx),
      function(res, cb) {
        t.notOk(res, 'upgradeIfOutdated should not return true');
        t.equal(upgraderCallCount, 0,
                'upgrader function should not have been called');
        cb(null);
      },

      // Try to make a new database object for the same database but with an
      // incremented schema version.
      function(cb) {
        newVersion = version + 1;
        var otherMd = new SchemaMetadata({version: newVersion});
        otherSchema = new Schema(otherMd, upgrader);
        otherDb = app.noSqlDatabase(dbName, otherSchema);
        otherDb.upgradeIfOutdated(ctx, cb);
      },

      function(res, cb) {
        t.ok(res, 'otherDb.upgradeIfOutdated expected to return true');
        t.equal(upgraderCallCount, 1, 'upgrader should have been called once');
        cb(null);
      },

      // check if the contents of SchemaMetadata are correctly stored in the db.
      function(cb) {
        otherDb._getSchemaMetadata(ctx, cb);
      },

      function(metadata, cb) {
        t.equal(metadata.version, newVersion, 'metadata has correct version');
        cb(null);
      }
    ], function(err) {
      t.error(err);
      o.teardown(t.end);
    });
  });
});
