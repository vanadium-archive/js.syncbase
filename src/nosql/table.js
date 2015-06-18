// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var through2 = require('through2');
var vanadium = require('vanadium');

var nosqlVdl = require('../gen-vdl/v.io/syncbase/v23/services/syncbase/nosql');
var prefix = require('./rowrange').prefix;
var Row = require('./row');

module.exports = Table;

var util = require('../util');

/**
 * @summary
 * Table represents a collection of Rows.
 * Private constructor. Use database.table() to get an instance.
 * @param {string} parentFullName Full name of Database which contains this
 * Table.
 * @param {string} relativeName Relative name of this Table.  Must not
 * contain slashes.
 * @constructor
 * @inner
 * @memberof {module:syncbase.nosql}
 */
function Table(parentFullName, relativeName) {
  if (!(this instanceof Table)) {
    return new Table(parentFullName, relativeName);
  }

  util.addNameProperties(this, parentFullName, relativeName);

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
  var client = vanadium.runtimeForContext(ctx).newClient();
  var signature = [nosqlVdl.Table.prototype._serviceDescription];

  this._wireObj = client.bindWithSignature(this.fullName, signature);
  return this._wireObj;
};

/**
 * Creates a row the given primary key in this table.
 * @param {string} key Primary key for the row.
 * @return {module:syncbase.row.Row} Row object.
 */
Table.prototype.row = function(key) {
  return new Row(this.fullName, key);
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
 * Delete deletes all rows in the given range. If the last row that is covered
 * by a prefix from SetPermissions is deleted, that (prefix, perms) pair is
 * removed.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:syncbase.nosql.rowrange.RowRange} range Row ranges to delete.
 * @param {function} cb Callback.
 */
Table.prototype.delete = function(ctx, range, cb) {
  this._wire(ctx).deleteRowRange(ctx, range.start, range.limit, cb);
};

/**
 * Scan returns all rows in the given range. The returned stream reads from a
 * consistent snapshot taken at the time of the Scan RPC.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:syncbase.nosql.rowrange.RowRange} range Row ranges to scan.
 * @param {function} cb Callback.
 * @returns {stream} Stream of row objects.
 */
Table.prototype.scan = function(ctx, range, cb) {
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

  var stream = this._wire(ctx).scan(ctx, range.start, range.limit, cb).stream;
  var decodedStream = stream.pipe(vomStreamDecoder);
  stream.on('error', function(err) {
    decodedStream.emit('error', err);
  });

  return decodedStream;
};

/**
 * SetPermissions sets the permissions for all current and future rows with
 * the given prefix. If the prefix overlaps with an existing prefix, the
 * longest prefix that matches a row applies. For example:
 *     setPermissions(ctx, prefix('a/b'), perms1)
 *     setPermissions(ctx, prefix('a/b/c'), perms2)
 * The permissions for row "a/b/1" are perms1, and the permissions for row
 * "a/b/c/1" are perms2.
 *
 * SetPermissions will fail if called with a prefix that does not match any
 * rows.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:syncbase.nosql.rowrane.PrefixRange|string} prefix Prefix or
 * PrefixRange.
 * @param @param {module:vanadium.security.access.Permissions} perms Permissions
 * for the rows matching the prefix.
 * @param {function} cb Callback.
 */
Table.prototype.setPermissions = function(ctx, prefix, perms, cb) {
  this._wire(ctx).setPermissions(ctx, stringifyPrefix(prefix), perms, cb);
};



/**
 * GetPermissions returns an array of (prefix, perms) pairs. The array is
 * sorted from longest prefix to shortest, so element zero is the one that
 * applies to the row with the given key. The last element is always the
 * prefix "" which represents the table's permissions -- the array will always
 * have at least one element.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {string} key Row key to get permissions for.
 * @param {function} cb Callback.
 */
Table.prototype.getPermissions = function(ctx, key, cb) {
  // There are two PrefixPermission types, one is the wire type which has
  // Prefix as a string and then there is the client type where prefix is a
  // PrefixRange, therefore we convert between the wire and client types.
  this._wire(ctx).getPermissions(ctx, key, function(err, wirePerms) {
    if (err) {
      return cb(err);
    }

    var perms = wirePerms.map(function(v) {
      return new PrefixPermissions(
        prefix(v.prefix),
        v.perms
      );
    });

    cb(null, perms);
  });
};

/**
 * DeletePermissions deletes the permissions for the specified prefix. Any
 * rows covered by this prefix will use the next longest prefix's permissions.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:syncbase.nosql.rowrane.PrefixRange|string} prefix Prefix or
 * PrefixRange.
 * @param {function} cb Callback.
 */
Table.prototype.deletePermissions = function(ctx, prefix, cb) {
  //TODO(aghassemi): Why is prefix a PrefixRange in Go?
  this._wire(ctx).deletePermissions(ctx, stringifyPrefix(prefix), cb);
};

function stringifyPrefix(prefix) {
  var prefixStr = prefix;
  if (typeof prefix === 'object') {
    // assume it is a PrefixRange
    prefixStr = prefix.prefix;
  }
  return prefixStr;
}

/**
 * @summary
 * Represents a pair of {@link module:syncbase.nosql~PrefixRange} and
 * {@link module:vanadium.security.access.Permissions}.
 * @constructor
 * @inner
 * @memberof {module:syncbase.nosql}
 */
function PrefixPermissions(prefixRange, perms) {
  if (!(this instanceof PrefixPermissions)) {
    return new PrefixPermissions(prefixRange, perms);
  }

  /**
   * Prefix
   * @type {module:syncbase.nosql~PrefixRange}
   */
  Object.defineProperty(this, 'prefix', {
    value: prefixRange,
    writable: false,
    enumerable: true
  });

  /**
   * Permissions
   * @type {module:vanadium.security.access.Permissions}
   */
  Object.defineProperty(this, 'perms', {
    value: perms,
    writable: false,
    enumerable: true
  });
}
