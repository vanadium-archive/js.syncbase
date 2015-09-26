// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var through2 = require('through2');
var vanadium = require('vanadium');
// TODO(nlacasse): We should expose unwrap and other type-util methods on the
// vanadium.vdl object.
var unwrap = require('vanadium/src/vdl/type-util').unwrap;
var verror = vanadium.verror;

var nosqlVdl = require('../gen-vdl/v.io/v23/services/syncbase/nosql');
var watchVdl = require('../gen-vdl/v.io/v23/services/watch');

var BatchDatabase = require('./batch-database');
/* jshint -W079 */
// Silence jshint's error about redefining 'Blob'.
var Blob = require('./blob');
/* jshint +W079 */
var SyncGroup = require('./syncgroup');
var Table = require('./table');
var util = require('../util');
var watch = require('./watch');

module.exports = Database;

/**
 * @summary
 * Database represents a collection of Tables. Batches, queries, sync, watch,
 * etc. all operate at the Database level.
 * Private constructor. Use app.noSqlDatabase() to get an instance.
 * @param {string} parentFullName Full name of parent App.
 * @param {string} relativeName Relative name for this Database.
 * @param {number} schemaVersion Database schema version expected by client.
 * @constructor
 * @inner
 * @memberof {module:syncbase.nosql}
 */
function Database(parentFullName, relativeName, batchSuffix, schema) {
  if (!(this instanceof Database)) {
    return new Database(parentFullName, relativeName);
  }

  // Escape relativeName so that any forward slashes get dropped, thus ensuring
  // that the server will interpret fullName as referring to a database object.
  // Note that the server will still reject this name if util.ValidDatabaseName
  // returns false.
  var fullName = vanadium.naming.join(
    parentFullName, util.escape(relativeName) + batchSuffix);
  util.addNameProperties(this, parentFullName, relativeName, fullName);

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
Database.prototype._wire = function(ctx) {
  if (this._wireObj) {
    return this._wireObj;
  }
  var client = vanadium.runtimeForContext(ctx).getClient();
  var signature = [nosqlVdl.Database.prototype._serviceDescription];

  this._wireObj = client.bindWithSignature(this.fullName, signature);
  return this._wireObj;
};

/**
 * Creates this Database.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:vanadium.security.access.Permissions} perms Permissions for
 * the new database.  If perms is null, we inherit (copy) the App perms.
 * @param {function} cb Callback.
 */
Database.prototype.create = function(ctx, perms, cb) {
  var schemaMetadata = null;
  if (this.schema) {
    schemaMetadata = this.schema.metadata;
  }
  this._wire(ctx).create(ctx, schemaMetadata, perms, cb);
};

/**
 * Destroys this Database, permanently removing all of its data.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Database.prototype.destroy = function(ctx, cb) {
  this._wire(ctx).destroy(ctx, this.schemaVersion, cb);
};

/**
 * Returns true only if this Database exists.
 * Insufficient permissions cause exists to return false instead of an error.
 * TODO(ivanpi): exists may fail with an error if higher levels of hierarchy
 * do not exist.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Database.prototype.exists = function(ctx, cb) {
  this._wire(ctx).exists(ctx, this.schemaVersion, cb);
};

/**
 * Executes a syncQL query.
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
 * NOTE(nlacasse): The Go client library returns the headers seperately from
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
 * @param {function} cb Callback.
 * @returns {stream} Stream of rows.
 */
Database.prototype.exec = function(ctx, query, cb) {
  var streamUnwrapper = through2({
    objectMode: true
  }, function(res, enc, cb) {
    return cb(null, res.map(unwrap));
  });

  var stream = this._wire(ctx).exec(ctx, this.schemaVersion, query, cb).stream;

  var decodedStream = stream.pipe(streamUnwrapper);
  stream.on('error', function(err) {
    decodedStream.emit('error', err);
  });

  return decodedStream;
};

/**
 * Returns the Table with the given name.
 * @param {string} relativeName Table name.  Must not contain slashes.
 * @return {module:syncbase.table.Table} Table object.
 */
Database.prototype.table = function(relativeName) {
  return new Table(this.fullName, relativeName, this.schemaVersion);
};

/**
 * Returns a list of all Table names.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Database.prototype.listTables = function(ctx, cb) {
  // See comment in v.io/v23/services/syncbase/nosql/service.vdl for why we
  // can't implement listTables using Glob (via util.listChildren).
  this._wire(ctx).listTables(ctx, cb);
};

/**
 * @private
 */
Database.prototype._tableWire = function(ctx, relativeName) {
  var client = vanadium.runtimeForContext(ctx).newClient();
  var signature = [nosqlVdl.Table.prototype._serviceDescription];

  var fullTableName = vanadium.naming.join(this.fullName, relativeName);
  return client.bindWithSignature(fullTableName, signature);
};

/**
 * Watches for updates to the database. For each watch request, the client will
 * receive a reliable stream of watch events without re-ordering.
 *
 * This method is designed to be used in the following way:
 * 1) begin a read-only batch
 * 2) read all information your app needs
 * 3) read the ResumeMarker
 * 4) abort the batch
 * 5) start watching for changes to the data using the ResumeMarker
 *
 * In this configuration the client doesn't miss any changes.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {string} table Name of table to watch.
 * @param {string} prefix Prefix of keys to watch.
 * @param {module:syncbase.nosql.watch.ResumeMarker} resumeMarker ResumeMarker
 * to resume watching from.
 * @param {function} [cb] Optional callback that will be called after watch RPC
 * finishes.
 * @returns {stream} Stream of WatchChange objects.
 */
Database.prototype.watch = function(ctx, tableName, prefix, resumeMarker, cb) {
  var globReq = new watchVdl.GlobRequest({
    pattern: vanadium.naming.join(tableName, prefix + '*'),
    resumeMarker: resumeMarker
  });

  var watchChangeEncoder = through2({
    objectMode: true
  }, function(change, enc, cb) {
    var changeType;
    switch (change.state) {
      case watchVdl.Exists.val:
        changeType = 'put';
        break;
      case watchVdl.DoesNotExist.val:
        changeType = 'delete';
        break;
      default:
        return cb(new Error('invalid change state ' + change.state));
    }

    var wc = new watch.WatchChange({
      tableName: vanadium.naming.stripBasename(change.name),
      rowName: vanadium.naming.basename(change.name),
      changeType: changeType,
      valueBytes: changeType === 'put' ? change.value.value : null,
      resumeMarker: change.resumeMarker,
      fromSync: change.value.fromSync,
      continued: change.continued
    });
    return cb(null, wc);
  });

  var stream = this._wire(ctx).watchGlob(ctx, globReq, cb).stream;

  // TODO(sadovsky): Our JS watch test times out after 20s when globReq is
  // invalid. That's strange, because the server should immediately return an
  // RPC error (since util.ParseTableRowPair returns an error). Does the JS
  // watch test not check for this error?
  var watchChangeStream = stream.pipe(watchChangeEncoder);
  stream.on('error', function(err) {
    watchChangeStream.emit('error', err);
  });

  return watchChangeStream;
};

/**
 * Gets the ResumeMarker that points to the current end of the event log.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Database.prototype.getResumeMarker = function(ctx, cb) {
  this._wire(ctx).getResumeMarker(ctx, cb);
};

/**
 * Replaces the current Permissions for the Database.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:vanadium.security.access.Permissions} perms Permissions for
 * the database.
 * @param {string} version Version of the current Permissions object which will
 * be overwritten. If empty, SetPermissions will perform an unconditional
 * update.
 * @param {function} cb Callback.
 */
Database.prototype.setPermissions = function(ctx, perms, version, cb) {
  this._wire(ctx).setPermissions(ctx, perms, version, cb);
};

/**
 * Returns the current Permissions for the Database.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Database.prototype.getPermissions = function(ctx, cb) {
  this._wire(ctx).getPermissions(ctx, cb);
};

/**
 * Creates a new batch. Instead of calling this function directly, clients are
 * encouraged to use the RunInBatch() helper function, which detects "concurrent
 * batch" errors and handles retries internally.
 *
 * Default concurrency semantics:
 * - Reads (e.g. gets, scans) inside a batch operate over a consistent snapshot
 *   taken during beginBatch(), and will see the effects of prior writes
 *   performed inside the batch.
 * - commit() may fail with errConcurrentBatch, indicating that after
 *   beginBatch() but before commit(), some concurrent routine wrote to a key
 *   that matches a key or row-range read inside this batch.
 * - Other methods will never fail with error errConcurrentBatch, even if it is
 *   known that commit() will fail with this error.
 *
 * Once a batch has been committed or aborted, subsequent method calls will
 * fail with no effect.
 *
 * Concurrency semantics can be configured using BatchOptions.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:vanadium.syncbase.nosql.BatchOptions} opts BatchOptions.
 * @param {function} cb Callback.
 */
Database.prototype.beginBatch = function(ctx, opts, cb) {
  var self = this;
  this._wire(ctx).beginBatch(ctx, this.schemaVersion, opts,
    function(err, batchSuffix) {
      if (err) {
        return cb(err);
      }
      var db = new Database(self._parentFullName, self.name, batchSuffix,
                            self.schema);
      return cb(null, new BatchDatabase(db));
    });
};

/**
 * Gets a handle to the SyncGroup with the given name.
 *
 * @param {string} name SyncGroup name.
 */
Database.prototype.syncGroup = function(name) {
  return new SyncGroup(this, name);
};

/**
 * Gets the global names of all SyncGroups attached to this database.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Database.prototype.getSyncGroupNames = function(ctx, cb) {
  this._wire(ctx).getSyncGroupNames(ctx, cb);
};

/**
 * Compares the current schema version of the database with the schema version
 * provided while creating this database handle. If the current database schema
 * version is lower, then schema.updater is called. If schema.updater is
 * successful this method stores the new schema metadata in database.
 *
 * It is important not to access or modify the database until upgradeIfOutdated
 * has called its callback.
 *
 * TODO(nlacasse): Consider locking the database in some way so that the
 * upgrader function can access it, but all other attempts fail immediately
 * with a helpful error.
 *
 * Note: schema can be nil, in which case this method skips schema check and
 * the caller is responsible for maintaining schema sanity.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Database.prototype.upgradeIfOutdated = function(ctx, cb) {
  var self = this;
  if (!self.schema) {
    return process.nextTick(function() {
      cb(new verror.BadStateError(ctx,
          'schema or schema.metadata cannot be nil.  ' +
          'A valid schema needs to be used when creating Database handle.'));
    });
  }

  if (self.schema.metadata.version < 0) {
    return process.nextTick(function() {
      cb(new verror.BadStateError(ctx,
          'schema version cannot be less than zero'));
    });
  }

  self._getSchemaMetadata(ctx, function(err, currMeta) {
    if (err) {
      if (!(err instanceof verror.NoExistError)) {
        return cb(err);
      }

      // If the client app did not set a schema as part of create db
      // getSchemaMetadata() will return a NoExistError. If so we set the
      // schema here.
      self._setSchemaMetadata(ctx, self.schema.metadata, function(err) {

        // The database may not yet exist. If so above call will return
        // NoExistError and we return db without error. If the error is
        // different then return the error to the caller.
        if (err && !(err instanceof verror.NoExistError)) {
          return cb(err);
        }
        return cb(null, false);
      });

      return;
    }

    if (currMeta.version >= self.schema.metadata.version) {
      return cb(null, false);
    }

    // Call the Upgrader provided by the app to upgrade the schema.
    //
    // TODO(nlacasse,jlodhia): disable sync before running Upgrader and
    // reenable once Upgrader is finished.
    //
    // TODO(nlacasse,jlodhia): prevent other processes (local/remote) from
    // accessing the database while upgrade is in progress.
    self.schema.upgrader(self, currMeta.version, self.schema.metadata.version,
        function(err) {
      if (err) {
        return cb(err);
      }

      // Update the schema metadata in db to the latest version.
      self._setSchemaMetadata(ctx, self.schema.metadata, function(err) {
        if (err) {
          return cb(err);
        }
        cb(null, true);
      });
    });
  });
};

/**
 * Retrieves the schema metadata for the database.
 * @private
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Database.prototype._getSchemaMetadata = function(ctx, cb) {
  return this._wire(ctx).getSchemaMetadata(ctx, cb);
};

/**
 * Stores the schema metadata for the database.
 * @private
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:syncbase.schema.SchemaMetadata} metadata Schema metadata.
 * @param {function} cb Callback.
 */
Database.prototype._setSchemaMetadata = function(ctx, metadata, cb) {
  return this._wire(ctx).setSchemaMetadata(ctx, metadata, cb);
};

/**
 * Returns a handle to the blob with the given blobRef.
 * @param {module:syncbase.nosql.BlobRef} blobRef BlobRef of blob to get.
 *
 */
Database.prototype.blob = function(blobRef) {
  return new Blob(this, blobRef);
};

/**
 * Creates a new blob.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 *
 */
Database.prototype.createBlob = function(ctx, cb) {
  var self = this;
  this._wire(ctx).createBlob(ctx, function(err, blobRef) {
    if (err) {
      return cb(err);
    }
    return cb(null, new Blob(self, blobRef));
  });
};
