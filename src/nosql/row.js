// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var vanadium = require('vanadium');

var nosqlVdl = require('../gen-vdl/v.io/syncbase/v23/services/syncbase/nosql');

module.exports = Row;

/**
 * @summary
 * Represents a single row in a Table.
 * Private constructor, use table.row() to get an instance.
 * @param {number} schemaVersion Database schema version expected by client.
 * @inner
 * @constructor
 * @memberof module:syncbase.nosql
 */
function Row(parentFullName, key, schemaVersion) {
  if (!(this instanceof Row)) {
    return new Row(parentFullName, key, schemaVersion);
  }

  // TODO(aghassemi) We may need to escape the key. Align with Go implementation
  // when that decision is made.
  // Also for Database and Table, we throw error if name has a slash.
  // Should they all behave the same or is row key really different?
  var fullName = vanadium.naming.join(parentFullName, key);

  this.schemaVersion = schemaVersion;
  /**
   * The key of this Row.
   * @property name
   * @type {string}
   */
  Object.defineProperty(this, 'key', {
    value: key,
    writable: false,
    enumerable: true
  });

  /**
   * @property name
   * @type {string}
   */
  Object.defineProperty(this, 'fullName', {
    value: fullName,
    writable: false,
    enumerable: true
  });

  /**
   * Caches the table wire object.
   * @private
   */
  Object.defineProperty(this, '_wireObj', {
    enumerable: false,
    value: null,
    writable: true
  });
}

/**
 * @private
 */
Row.prototype._wire = function(ctx) {
  if (this._wireObj) {
    return this._wireObj;
  }
  var client = vanadium.runtimeForContext(ctx).newClient();
  var signature = [nosqlVdl.Row.prototype._serviceDescription];

  this._wireObj = client.bindWithSignature(this.fullName, signature);
  return this._wireObj;
};

/**
 * Returns true only if this Row exists.
 * Insufficient permissions cause exists to return false instead of an error.
 * TODO(ivanpi): exists may fail with an error if higher levels of hierarchy
 * do not exist.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Row.prototype.exists = function(ctx, cb) {
  this._wire(ctx).exists(ctx, this.schemaVersion, cb);
};

/**
 * Returns the value for this Row.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Row.prototype.get = function(ctx, cb) {
  this._wire(ctx).get(ctx, this.schemaVersion, function(err, value) {
    if (err) {
      return cb(err);
    }

    vanadium.vom.decode(value, false, null, cb);
  });
};

/**
 * Writes the given value for this Row.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {*} value Value to write.
 * @param {module:vanadium.vdl.Type} [type] Type of value.
 * @param {function} cb Callback.
 */
Row.prototype.put = function(ctx, value, type, cb) {
  if (typeof cb === 'undefined' && typeof type === 'function') {
    cb = type;
    type = undefined;
  }

  // NOTE(aghassemi) Currently server side does not want to encode for
  // performance reasons, so encoding/decoding is happening on the client side.
  var encodedVal;
  try {
    encodedVal = vanadium.vom.encode(value, type);
  } catch (e) {
    return cb(e);
  }
  this._wire(ctx).put(ctx, this.schemaVersion, encodedVal, cb);
};

/**
 * Deletes this Row.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Row.prototype.delete = function(ctx, cb) {
  this._wire(ctx).delete(ctx, this.schemaVersion, cb);
};
