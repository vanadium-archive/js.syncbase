// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var test = require('prova');

var runInBatch = require('../..').nosql.runInBatch;

function MockBatchDb(failOnCommit) {
  this.abortCalled = false;
  this.commitCalled = false;
  this._failOnCommit = failOnCommit;
}

MockBatchDb.prototype.abort = function(ctx, cb) {
  this.abortCalled = true;
  cb();
};

MockBatchDb.prototype.commit = function(ctx, cb) {
  this.commitCalled = true;
  if (this._failOnCommit) {
    return cb(new Error('error committing'));
  }
  cb();
};

function MockDb(failOnCommit) {
  this.batchDb = null;
  this._failOnCommit = failOnCommit;
}

MockDb.prototype.beginBatch = function(ctx, opts, cb) {
  this.batchDb = new MockBatchDb(this._failOnCommit);
  return cb(null, this.batchDb);
};

test('runInBatch commits on success', function(t) {
  var ctx = {};
  var db = new MockDb();

  function willSucceed(db, cb) {
    cb(null);
  }

  runInBatch(ctx, db, {}, willSucceed, function(err) {
    if (err) {
      return t.end(err);
    }

    t.ok(db.batchDb, 'batch db is created');
    t.ok(db.batchDb.commitCalled, 'batchDb.commit() was called');
    t.notok(db.batchDb.abortCalled, 'batchDb.abort() was not called');

    t.end();
  });
});

test('runInBatch aborts on failure', function(t) {
  var ctx = {};
  var db = new MockDb();
  var error = new Error('boom!');

  function willFail(db, cb) {
    cb(error);
  }

  runInBatch(ctx, db, {}, willFail, function(err) {
    t.ok(err, 'runInBatch should return an error');
    t.equal(err, error, 'runInBatch returns the correct error');

    t.ok(db.batchDb, 'batch db is created');
    t.notok(db.batchDb.commitCalled, 'batchDb.commit() was not called');
    t.ok(db.batchDb.abortCalled, 'batchDb.abort() was called');

    t.end();
  });
});

test('runInBatch aborts if commit fails', function(t) {
  var ctx = {};
  var db = new MockDb(true);

  function willSucceed(db, cb) {
    cb(null);
  }

  runInBatch(ctx, db, {}, willSucceed, function(err) {
    t.ok(err, 'runInBatch should return an error');

    t.ok(db.batchDb, 'batch db is created');
    t.ok(db.batchDb.commitCalled, 'batchDb.commit() was called');
    t.ok(db.batchDb.abortCalled, 'batchDb.abort() was called');

    t.end();
  });
});
