// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var async = require('async');
var test = require('prova');
var vom = require('vanadium').vom;

var syncbase = require('../..');
var WatchChange = syncbase.nosql.WatchChange;

var testUtil = require('./util');
var setupTable = testUtil.setupTable;
var uniqueName = testUtil.uniqueName;

// Tests the basic client watch functionality (no perms or batches).  First
// does some puts and deletes, fetching a ResumeMarker after each operation.
// Then calls 'watch' with different prefixes and ResumeMarkers and verifies
// that the resulting stream contains the correct changes.
test('basic client watch', function(t) {
  setupTable(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var ctx = o.ctx;
    var db = o.database;
    var table = o.table;

    var row1Prefix = 'row-abc';
    var row1 = table.row(uniqueName(row1Prefix));
    var value1 = uniqueName('value');

    var row2Prefix = 'row-a';
    var row2 = table.row(uniqueName(row2Prefix));
    var value2 = uniqueName('value');

    var resumeMarkers = [];

    function getAndAppendResumeMarker(cb) {
      db.getResumeMarker(ctx, function(err, rm) {
        if (err) {
          return cb(err);
        }
        resumeMarkers.push(rm);
        cb(null);
      });
    }

    // Generate the data and resume markers.
    async.waterfall([
      // Initial state.
      getAndAppendResumeMarker,

      // Put to row1.
      row1.put.bind(row1, ctx, value1),
      getAndAppendResumeMarker,

      // Delete row1.
      row1.delete.bind(row1, ctx),
      getAndAppendResumeMarker,

      // Put to row2.
      row2.put.bind(row2, ctx, value2),
      getAndAppendResumeMarker
    ], assertCorrectChanges);

    function assertCorrectChanges(err) {
      if (err) {
        t.error(err);
        return o.teardown(t.end);
      }

      var allExpectedChanges = [new WatchChange({
        tableName: table.name,
        rowName: row1.key,
        changeType: 'put',
        valueBytes: vom.encode(value1),
        resumeMarker: resumeMarkers[1]
      }), new WatchChange({
        tableName: table.name,
        rowName: row1.key,
        changeType: 'delete',
        valueBytes: null,
        resumeMarker: resumeMarkers[2]
      }), new WatchChange({
        tableName: table.name,
        rowName: row2.key,
        changeType: 'put',
        valueBytes: vom.encode(value2),
        resumeMarker: resumeMarkers[3]
      })];

      async.series([
        assertWatch.bind(null, t, ctx, db, table.name, row2Prefix,
                         resumeMarkers[0], allExpectedChanges),
        assertWatch.bind(null, t, ctx, db, table.name, row2Prefix,
                         resumeMarkers[1], allExpectedChanges.slice(1)),
        assertWatch.bind(null, t, ctx, db, table.name, row2Prefix,
                         resumeMarkers[2], allExpectedChanges.slice(2)),

        // Undefined resume marker - include initial state.
        assertWatch.bind(null, t, ctx, db, table.name, row2Prefix,
                         undefined, allExpectedChanges.slice(2)),

        assertWatch.bind(null, t, ctx, db, table.name, row1Prefix,
                         resumeMarkers[0], allExpectedChanges.slice(0,2)),
        assertWatch.bind(null, t, ctx, db, table.name, row1Prefix,
                         resumeMarkers[1], allExpectedChanges.slice(1,2)),
      ], function(err) {
        t.error(err);
        o.teardown(t.end);
      });
    }
  });
});

function assertWatch(t, ctx, db, tableName, rowPrefix, resumeMarker,
                     expectedWatchChanges, cb) {
  var cctx = ctx.withCancel();
  var stream = resumeMarker === undefined ?
    db.watch(ctx, tableName, rowPrefix) :
    db.watch(ctx, tableName, rowPrefix, resumeMarker);

  async.timesSeries(expectedWatchChanges.length, function(i, next) {
    stream.once('data', function(gotWatchChange) {
      t.deepEqual(gotWatchChange, expectedWatchChanges[i]);

      next(null);
    });
  }, function(err) {
    cctx.finish();
    if (err) {
      return cb(err);
    }
    cb(null);
  });
}
