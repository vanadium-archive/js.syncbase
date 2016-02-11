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

    var version = 123;
    var md = new SchemaMetadata({version: version});
    var schema = new Schema(md);

    var otherDb, otherSchema, newVersion;

    var db = app.noSqlDatabase(dbName, schema);

    async.waterfall([
      // Verify that calling updateSchemaMetadata on a non existing database
      // does not throw errors.
      db.updateSchemaMetadata.bind(db, ctx),
      function(upgraded, cb) {
        t.equal(upgraded, false, 'updateSchemaMetadata should return false');
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

      // Try to make a new database object for the same database but with an
      // incremented schema version.
      function(cb) {
        newVersion = version + 1;
        var otherMd = new SchemaMetadata({version: newVersion});
        otherSchema = new Schema(otherMd);
        otherDb = app.noSqlDatabase(dbName, otherSchema);
        otherDb.updateSchemaMetadata(ctx, cb);
      },

      function(res, cb) {
        t.ok(res, 'otherDb.updateSchemaMetadata expected to return true');
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
