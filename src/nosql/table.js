// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var inherits = require('inherits');
var through2 = require('through2');
var vanadium = require('vanadium');

var nosqlVdl = require('../gen-vdl/v.io/v23/services/syncbase/nosql');
var Row = require('./row');
var RowRange = require('./rowrange');
var util = require('../util');

var prefix = RowRange.prefix;

inherits(Table, util.NamedResource);
module.exports = Table;

/**
 * Table represents a collection of Rows.
 * Private constructor. Use database.table() to get an instance.
 * @param {string} parentFullName Full name of parent Database.
 * @param {string} relativeName Relative name for this Table.
 * @param {number} schemaVersion Database schema version expected by client.
 * @constructor
 * @inner
 * @memberof {module:syncbase.nosql}
 */
function Table(parentFullName, relativeName, schemaVersion) {
  if (!(this instanceof Table)) {
    return new Table(parentFullName, relativeName, schemaVersion);
  }

  // Escape relativeName so that any forward slashes get dropped, thus ensuring
  // that the server will interpret fullName as referring to a table object.
  // Note that the server will still reject this name if util.ValidTableName
  // returns false.
  var fullName = vanadium.naming.join(
    parentFullName, util.escape(relativeName));
  util.NamedResource.call(this, parentFullName, relativeName, fullName);

  this.schemaVersion = schemaVersion;

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
Table.prototype._wire = function(ctx) {
  if (this._wireObj) {
    return this._wireObj;
  }
  var client = vanadium.runtimeForContext(ctx).getClient();
  var signature = [nosqlVdl.Table.prototype._serviceDescription];

  this._wireObj = client.bindWithSignature(this.fullName, signature);
  return this._wireObj;
};

/**
 * Returns true only if this Table exists.
 * Insufficient permissions cause exists to return false instead of an error.
 * TODO(ivanpi): exists may fail with an error if higher levels of hierarchy
 * do not exist.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Table.prototype.exists = function(ctx, cb) {
  this._wire(ctx).exists(ctx, this.schemaVersion, cb);
};

/**
 * Creates this Table.
 * If perms is nil, we inherit (copy) the Database perms.
 * Create must not be called from within a batch.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:vanadium.security.access.Permissions} perms Permissions for
 * the new database.  If perms is null, we inherit (copy) the Database perms.
 * @param {function} cb Callback.
 */
Table.prototype.create = function(ctx, perms, cb) {
  this._wire(ctx).create(ctx, this.schemaVersion, perms, cb);
};

/**
 * Destroys this Table, permanently removing all of its data.
 * Destroy must not be called from within a batch.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Table.prototype.destroy = function(ctx, cb) {
  this._wire(ctx).destroy(ctx, this.schemaVersion, cb);
};

/**
 * Returns the current permissions for the table.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Table.prototype.getPermissions = function(ctx, cb) {
  this._wire(ctx).getPermissions(ctx, this.schemaVersion, cb);
};

/**
 * Replaces the current permissions for the table.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:vanadium.security.access.Permissions} perms Permissions
 * @param {function} cb Callback.
 */
Table.prototype.setPermissions = function(ctx, perms, cb) {
  this._wire(ctx).setPermissions(ctx, this.schemaVersion, perms, cb);
};

/**
 * Creates a row the given primary key in this table.
 * @param {string} key Primary key for the row.
 * @return {module:syncbase.row.Row} Row object.
 */
Table.prototype.row = function(key) {
  return new Row(this.fullName, key, this.schemaVersion);
};

/**
 * Get stores the value for the given primary key in value. If value's type
 * does not match the stored type, Get will return an error.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {string} key Primary key of the row.
 * @param {function} cb Callback.
 */
Table.prototype.get = function(ctx, key, cb) {
  this.row(key).get(ctx, cb);
};

/**
 * Put writes the given value to this Table. The value's primary key field
 * must be set.
 *
 * Note that if you want to sync data with a Go syncbase client, or if you want
 * to use syncbase queries, you must either specify the type of the value, or
 * use a vdl value that includes its type.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {string} key Primary key of the row.
 * @param {*} value Value to put in the row.
 * @param {module:vanadium.vdl.Type} [type] Type of value.
 * @param {function} cb Callback.
 */
Table.prototype.put = function(ctx, key, value, type, cb) {
  this.row(key).put(ctx, value, type, cb);
};

/**
 * Delete deletes the row for the given primary key.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {string} key Primary key of the row.
 * @param {function} cb Callback.
 */
Table.prototype.delete = function(ctx, key, cb) {
   this.row(key).delete(ctx, cb);
};

/**
 * deleteRange deletes all rows in the given half-open range [start, limit). If
 * limit is "", all rows with keys >= start are included.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:syncbase.nosql.rowrange.RowRange|string} range Row range
 * to delete. If a string value is provided for the range, it is assumed to be
 * a prefix.
 * @param {function} cb Callback.
 */
Table.prototype.deleteRange = function(ctx, range, cb) {
  range = normalizeRangeParam(range);
  this._wire(ctx).deleteRange(
        ctx, this.schemaVersion, range.start, range.limit, cb);
};

/**
 * Scan returns all rows in the given range.
 * Concurrency semantics: It is legal to perform writes concurrently with
 * Scan. The returned stream reads from a consistent snapshot taken at the
 * time of the RPC, and will not reflect subsequent writes to keys not yet
 * reached by the stream.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:syncbase.nosql.rowrange.RowRange|string} range Row range to
 * scan. If a string value is provided for the range, it is assumed to be
 * a prefix.
 * @param {function} cb Callback.
 * @returns {stream} Stream of row objects.
 */
Table.prototype.scan = function(ctx, range, cb) {
  range = normalizeRangeParam(range);
  var vomStreamDecoder = through2({
    objectMode: true
  }, function(row, enc, cb) {
    vanadium.vom.decode(row.value, false, null, function(err, decodedVal) {
      if (err) {
        return cb(err);
      }
      row.value = decodedVal;
      cb(null, row);
    });
  });

  var stream = this._wire(ctx)
        .scan(ctx, this.schemaVersion, range.start, range.limit, cb).stream;
  var decodedStream = stream.pipe(vomStreamDecoder);
  stream.on('error', function(err) {
    decodedStream.emit('error', err);
  });

  return decodedStream;
};

/**
 * SetPrefixPermissions sets the permissions for all current and future rows
 * with the given prefix. If the prefix overlaps with an existing prefix, the
 * longest prefix that matches a row applies. For example:
 *     setPerfixPermissions(ctx, prefix('a/b'), perms1)
 *     setPrefixPermissions(ctx, prefix('a/b/c'), perms2)
 * The permissions for row "a/b/1" are perms1, and the permissions for row
 * "a/b/c/1" are perms2.
 *
 * SetPrefixPermissions will fail if called with a prefix that does not match
 * any rows.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {string} prefix Prefix.
 * @param {module:vanadium.security.access.Permissions} perms Permissions
 * for the rows matching the prefix.
 * @param {function} cb Callback.
 */
Table.prototype.setPrefixPermissions = function(ctx, prefix, perms, cb) {
  this._wire(ctx).setPrefixPermissions(
        ctx, this.schemaVersion, prefix, perms, cb);
};

/**
 * GetPrefixPermissions returns an array of (prefix, perms) pairs. The array is
 * sorted from longest prefix to shortest, so element zero is the one that
 * applies to the row with the given key. The last element is always the
 * prefix "" which represents the table's permissions -- the array will always
 * have at least one element.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {string} key Row key to get permissions for.
 * @param {function} cb Callback.
 */
Table.prototype.getPrefixPermissions = function(ctx, key, cb) {
  this._wire(ctx).getPrefixPermissions(ctx, this.schemaVersion, key, cb);
};

/**
 * DeletePrefixPermissions deletes the permissions for the specified prefix. Any
 * rows covered by this prefix will use the next longest prefix's permissions.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {string} prefix Prefix.
 * @param {function} cb Callback.
 */
Table.prototype.deletePrefixPermissions = function(ctx, prefix, cb) {
  this._wire(ctx).deletePrefixPermissions(
        ctx, this.schemaVersion, prefix, cb);
};

/**
 * Ensures range is either a string or a RowRange object.
 * If a string, it returns a prefix RowRange object.
 * If not a string or RowRange object, it throws a type error.
 * @private
 */
function normalizeRangeParam(range) {
  if (typeof range === 'string') {
    range = prefix(range);
  } else if(!(range instanceof RowRange.RowRange)) {
    var rangeType = (range.constructor ? range.constructor.name : typeof range);
    throw new TypeError('range must be of type string or RowRange. got ' +
      range + ' with type ' + rangeType);
  }

  return range;
}
