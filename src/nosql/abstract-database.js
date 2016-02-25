// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var inherits = require('inherits');
var through2 = require('through2');
var vanadium = require('vanadium');

// TODO(nlacasse): We should expose unwrap and other type-util methods on the
// vanadium.vdl object.
var unwrap = require('vanadium/src/vdl/type-util').unwrap;

var nosqlVdl = require('../gen-vdl/v.io/v23/services/syncbase/nosql');

var Table = require('./table');
var util = require('../util');

inherits(AbstractDatabase, util.NamedResource);
module.exports = AbstractDatabase;

/**
 * AbstractDatabase is a base class for Database and BatchDatabase. A database
 * is a collection of Tables. Batches, queries, sync, watch, etc. all operate at
 * the database level.
 * Private constructor. Use app.noSqlDatabase() to get a Database, and
 * database.beginBatch() to get a BatchDatabase.
 * @param {string} parentFullName Full name of parent App.
 * @param {string} relativeName Relative name for this Database.
 * @param {string} batchSuffix Suffix for BatchDatabase, empty for non-batch
 * Database.
 * @param {number} schema Database schema expected by client.
 * @constructor
 * @inner
 * @memberof {module:syncbase.nosql}
 */
function AbstractDatabase(parentFullName, relativeName, batchSuffix, schema) {
  if (!(this instanceof AbstractDatabase)) {
    return new AbstractDatabase(parentFullName, relativeName, batchSuffix,
                                schema);
  }

  // Escape relativeName so that any forward slashes get dropped, thus ensuring
  // that the server will interpret fullName as referring to a database object.
  // Note that the server will still reject this name if util.ValidDatabaseName
  // returns false.
  var fullName = vanadium.naming.join(
    parentFullName, util.escape(relativeName) + batchSuffix);
  util.NamedResource.call(this, parentFullName, relativeName, fullName);

  // TODO(sadovsky): The schema and schemaVersion fields should be private.
  // Better yet, we shouldn't include any schema-related things in the JS client
  // library until the design is more fully baked.
  Object.defineProperty(this, 'schema', {
    enumerable: false,
    value: schema,
    writable: false
  });

  Object.defineProperty(this, 'schemaVersion', {
    enumerable: false,
    value: schema ? schema.metadata.version : -1,
    writable: false
  });

  /**
   * Caches the database wire object.
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
AbstractDatabase.prototype._wire = function(ctx) {
  if (this._wireObj) {
    return this._wireObj;
  }
  var client = vanadium.runtimeForContext(ctx).getClient();
  var signature = [nosqlVdl.Database.prototype._serviceDescription];

  this._wireObj = client.bindWithSignature(this.fullName, signature);
  return this._wireObj;
};


/**
 * Returns the Table with the given relative name.
 * @param {string} relativeName Table name.  Must not contain slashes.
 * @return {module:syncbase.table.Table} Table object.
 */
AbstractDatabase.prototype.table = function(relativeName) {
  return new Table(this.fullName, relativeName, this.schemaVersion);
};

/**
 * Returns a list of all Table names.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
AbstractDatabase.prototype.listTables = function(ctx, cb) {
  // See comment in v.io/v23/services/syncbase/nosql/service.vdl for why we
  // can't implement listTables using Glob (via util.listChildren).
  this._wire(ctx).listTables(ctx, cb);
};

/**
 * Executes a syncQL query.
 *
 * If the query is parameterized, paramValues must contain a value for each '?'
 * placeholder in the query. If there are no placeholders, paramValues must be
 * empty or omitted. paramTypes should be provided in addition to paramValues
 * for paramValue elements that lack VOM types, including JS primitives.
 *
 * Returns a stream of rows.  The first row contains an array of headers (i.e.
 * column names).  Subsequent rows contain an array of values for each row that
 * matches the query.  The number of values returned in each row will match the
 * size of the headers array.
 * Concurrency semantics: It is legal to perform writes concurrently with
 * Exec. The returned stream reads from a consistent snapshot taken at the
 * time of the RPC, and will not reflect subsequent writes to keys not yet
 * reached by the stream.
 *
 * NOTE(nlacasse): The Go client library returns the headers separately from
 * the stream.  We could potentially do something similar in JavaScript, by
 * pulling the headers off the stream and passing them to the callback.
 * However, by Vanadium JS convention the callback gets called at the *end* of
 * the RPC, so a developer would have to wait for the stream to finish before
 * seeing what the headers are, which is not ideal.  We also cannot return the
 * headers directly because reading from the stream is async.
 *
 * TODO(nlacasse): Syncbase queries don't work on values that were put without
 * type information.  When JavaScript encodes values with no type infomation,
 * it uses "vdl.Value" for the type.  Presumably, syncbase does not know how to
 * decode such objects, so queries that involve inspecting the object or its
 * type don't work.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {string} query Query string.
 * @param {Object[]} [paramValues] Query parameters, one per '?' placeholder in
 * the query.
 * @param {module:vanadium.vdl.Type[]} [paramTypes] Query parameter types, one
 * per value in paramValues. Not required if paramValues are VDL typed or if
 * values being queried are JSValues.
 * @param {function} cb Callback.
 * @returns {stream} Stream of rows.
 */
AbstractDatabase.prototype.exec = function(ctx, query, paramValues, paramTypes,
                                           cb) {
  if (typeof cb === 'undefined' && typeof paramValues === 'function') {
    cb = paramValues;
    paramValues = undefined;
    paramTypes = undefined;
  }
  if (typeof cb === 'undefined' && typeof paramTypes === 'function') {
    cb = paramTypes;
    paramTypes = undefined;
  }

  var params;
  if (typeof paramValues !== 'undefined') {
    paramTypes = paramTypes || [];
    try {
      params = paramValues.map(function(param, i) {
        var type = paramTypes[i] || vanadium.vdl.types.ANY;
        return vanadium.vdl.canonicalize.value(param, type);
      });
    } catch (e) {
      return cb(e);
    }
  }

  var streamUnwrapper = through2({
    objectMode: true
  }, function(res, enc, cb) {
    return cb(null, res.map(unwrap));
  });

  var stream = this._wire(ctx).exec(ctx, this.schemaVersion,
                                    query, params, cb).stream;

  var decodedStream = stream.pipe(streamUnwrapper);
  stream.on('error', function(err) {
    decodedStream.emit('error', err);
  });

  return decodedStream;
};

/**
 * Gets the ResumeMarker that points to the current end of the event log.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
AbstractDatabase.prototype.getResumeMarker = function(ctx, cb) {
  this._wire(ctx).getResumeMarker(ctx, cb);
};
