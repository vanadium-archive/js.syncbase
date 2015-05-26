// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var vanadium = require('vanadium');

var vdl = require('./gen-vdl/v.io/syncbase/v23/services/syncbase');

var wireSignature = vdl.App.prototype._serviceDescription;

module.exports = App;

function App(fullName, name) {
  if (!(this instanceof App)) {
    return new App(fullName, name);
  }

  /**
   * @property name
   * @type {string}
   */
  Object.defineProperty(this, 'name', {
    value: name,
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

  this._wireObj = null;
}

// noSqlDatabase returns the noSqlDatabase with the given name. relativeName
// must not contain slashes.
App.prototype.noSqlDatabase = function(ctx, relativeName) {};

// listDatabases returns of all database names.
App.prototype.listDatabases = function(ctx) {};

// create creates this app.  If perms is empty, we inherit (copy) the Service
// perms.
App.prototype.create = function(ctx, perms, cb) {
  this._wire(ctx).create(ctx, perms, cb);
};

// delete deletes this app.
App.prototype.delete = function(ctx, cb) {
  this._wire(ctx).delete(ctx, cb);
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