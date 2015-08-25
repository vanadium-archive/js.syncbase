// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

module.exports = Blob;

function Blob(db, blobRef) {
  if (!(this instanceof Blob)) {
    return new Blob(db, blobRef);
  }

  /**
   * @private
   */
  Object.defineProperty(this, '_db', {
    enumerable: false,
    value: db,
    writable: false
  });

  /**
   * @property ref
   * @type {string}
   */
  Object.defineProperty(this, 'ref', {
    enumerable: true,
    value: blobRef,
    writable: false
  });
}

/**
 * Appends the byte stream to the blob.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 * @returns {Stream<Uint8Array>} Stream of bytes to append to blob.
 */
Blob.prototype.put = function(ctx, cb) {
  return this._db._wire(ctx).putBlob(ctx, this.ref, cb).stream;
};

/**
 * Marks the blob as immutable.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Blob.prototype.commit = function(ctx, cb) {
  this._db._wire(ctx).commitBlob(ctx, this.ref, cb);
};

/**
 * Gets the count of bytes written as part of the blob (committed or
 * uncommitted).
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Blob.prototype.size = function(ctx, cb) {
  this._db._wire(ctx).getBlobSize(ctx, this.ref, cb);
};

/**
 * Locally deletes the blob (committed or uncommitted).
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Blob.prototype.delete = function(ctx, cb) {
  this._db._wire(ctx).deleteBlob(ctx, this.ref, cb);
};

/**
 * Returns the byte stream from a committed blob starting at offset.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {number} offset Offset in bytes.
 * @param {function} cb Callback.
 * @returns {Stream<Uint8Array>} Stream of blob bytes.
 */
Blob.prototype.get = function(ctx, offset, cb) {
  return this._db._wire(ctx).getBlob(ctx, this.ref, offset, cb).stream;
};

/**
 * Initiates fetching a blob if not locally found, priority controls the
 * network priority of the blob.  Higher priority blobs are fetched before the
 * lower priority ones.  However an ongoing blob transfer is not interrupted.
 * Status updates are streamed back to the client as fetch is in progress.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {number} priority Priority.
 * @param {function} cb Callback.
 * @returns {Stream<BlobFetchStatus>} Stream of blob statuses.
 */
Blob.prototype.fetch = function(ctx, priority, cb) {
  return this._db._wire(ctx).fetchBlob(ctx, this.ref, priority, cb).stream;
};

/**
 * Locally pins the blob so that it is not evicted.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Blob.prototype.pin = function(ctx, cb) {
  this._db._wire(ctx).pinBlob(ctx, this.ref, cb);
};

/**
 * Locally unpins the blob so that it can be evicted if needed.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Blob.prototype.unpin = function(ctx, cb) {
  this._db._wire(ctx).unpinBlob(ctx, this.ref, cb);
};

/**
 * Locally caches the blob with the specified rank.  Lower ranked blobs are
 * more eagerly evicted.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {number} rank Rank of blob.
 * @param {function} cb Callback.
 */
Blob.prototype.keep = function(ctx, rank, cb) {
  this._db._wire(ctx).keepBlob(ctx, this.ref, rank, cb);
};
