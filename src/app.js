// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var vanadium = require('vanadium');

var Database = require('./nosql/database');
var util = require('./util');
var vdl = require('./gen-vdl/v.io/v23/services/syncbase');

var wireSignature = vdl.App.prototype._serviceDescription;

module.exports = App;

function App(parentFullName, relativeName) {
  if (!(this instanceof App)) {
    return new App(parentFullName, relativeName);
  }

  util.addNameProperties(this, parentFullName, relativeName);

  // TODO(nlacasse): Use the prr module to simplify all the
  // 'Object.defineProperty' calls scattered throughout the project.
  // https://www.npmjs.com/package/prr

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

// noSqlDatabase returns the noSqlDatabase with the given name. relativeName
// must not contain slashes. schema can be null or undefined only if a schema
// was never set for the database in the first place.
App.prototype.noSqlDatabase = function(relativeName, schema) {
  return new Database(this.fullName, relativeName, schema);
};

// listDatabases returns of all database names.
App.prototype.listDatabases = function(ctx, cb) {
  util.getChildNames(ctx, this.fullName, cb);
};

// create creates this app.  If perms is empty, we inherit (copy) the Service
// perms.
App.prototype.create = function(ctx, perms, cb) {
  this._wire(ctx).create(ctx, perms, cb);
};

// destroy destroys this app.
App.prototype.destroy = function(ctx, cb) {
  this._wire(ctx).destroy(ctx, cb);
};

// exists returns true only if this app exists. Insufficient permissions cause
// exists to return false instead of an error.
App.prototype.exists = function(ctx, cb) {
  this._wire(ctx).exists(ctx, cb);
};

App.prototype.getPermissions = function(ctx, cb) {
  this._wire(ctx).getPermissions(ctx, cb);
};

App.prototype.setPermissions = function(ctx, perms, version, cb) {
  this._wire(ctx).setPermissions(ctx, perms, version, cb);
};

App.prototype._wire = function(ctx) {
  if (!this._wireObj) {
    var rt = vanadium.runtimeForContext(ctx);
    var client = rt.newClient();
    this._wireObj = client.bindWithSignature(this.fullName, [wireSignature]);
  }

  return this._wireObj;
};
