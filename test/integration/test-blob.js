// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var arrayToStream = require('stream-array');
var streamToArray = require('stream-to-array');
var test = require('prova');

/* jshint -W079 */
// Silence jshint's error about redefining 'Blob'.
var Blob = require('../../src/nosql/blob');
/* jshint +W079 */
var vdl =
  require('../../src/gen-vdl/v.io/syncbase/v23/services/syncbase/nosql');

var testUtil = require('./util');
var setupDatabase = testUtil.setupDatabase;
var uniqueName = testUtil.uniqueName;

test('db.blob returns the correct blob', function(t) {
  setupDatabase(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var blobRef = uniqueName('blobRef');
    var blob = o.database.blob(blobRef);

    t.ok(blob instanceof Blob);
    t.equals(blob.ref, blobRef);

    o.teardown(t.end);
  });
});

// Tests local blob get before and after a put.
test('blob put then get', function(t) {
  setupDatabase(t, function(err, o) {
    if (err) {
      return t.end(err);
    }

    var ctx = o.ctx;
    var db = o.database;

    var blob;

    var data = new Uint8Array(new ArrayBuffer(256));

    db.createBlob(ctx, function(err, _blob) {
      if (err) {
        return end(err);
      }

      blob = _blob;
      t.ok(blob instanceof Blob, 'createBlob returns a new blob');
      t.equals(typeof blob.ref, 'string', 'blob has blobRef');

      getEmptyBlob();
    });

    function getEmptyBlob() {
      var blobStream = blob.get(ctx, 0, function(err) {
        t.ok(err instanceof vdl.BlobNotCommittedError,
             'blob.get should fail for uncommitted blobs');
      });

      streamToArray(blobStream, function(err) {
        t.ok(err instanceof vdl.BlobNotCommittedError,
             'blob.get should fail for uncommitted blobs');
        fetchEmptyBlob();
      });
    }

    function fetchEmptyBlob() {
      var blobStatusStream = blob.fetch(ctx, 100, function(err) {
        t.ok(err instanceof vdl.BlobNotCommittedError,
             'blob.fetch should fail for uncommitted blobs');
      });

      streamToArray(blobStatusStream, function(err) {
        t.ok(err instanceof vdl.BlobNotCommittedError,
             'blob status stream should fail for uncommitted blobs');
        assertBlobIsEmpty();
      });
    }

    function assertBlobIsEmpty() {
      blob.size(ctx, function(err, size) {
        if (err) {
          return end(err);
        }
        t.equals(size.toNativeNumber(), 0, 'blob is empty');

        putToBlob();
      });
    }

    function putToBlob() {
      var byteStream = blob.put(ctx, function(err) {
        if (err) {
          return t.end(err);
        }

        assertBlobSize();
      });

      arrayToStream([data]).pipe(byteStream);
    }

    function assertBlobSize() {
      blob.size(ctx, function(err, size) {
        if (err) {
          return end(err);
        }
        t.equals(size.toNativeNumber(), data.length, 'blob has correct size');

        commitBlob();
      });
    }

    function commitBlob() {
      blob.commit(ctx, function(err) {
        if (err) {
          return end(err);
        }

        assertGetBlob();
      });
    }

    function assertGetBlob() {
      var blobStream = blob.get(ctx, 0, t.error);

      streamToArray(blobStream, function(err, gotData) {
        if (err) {
          return end(err);
        }

        t.deepEquals(gotData, [data], 'blob has correct data');
        end();
      });
    }

    function end(err) {
      t.error(err);
      o.teardown(t.end);
    }
  });
});
