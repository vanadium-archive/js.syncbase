// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var vanadium = require('vanadium');

var App = require('./app');
var vdl = require('./gen-vdl/v.io/v23/services/syncbase');

// TODO(aghassemi): This looks clunky,
// https://github.com/vanadium/issues/issues/499 to deal with it.
var wireSignature = vdl.Service.prototype._serviceDescription;

module.exports = Service;

/**
 * @summary
 * Service represents a collection of Apps.
 * @param {string} fullName Full Vanadium object name of this Service.
 * @constructor
 * @inner
 * @memberof {module:syncbase}
 */
function Service(fullName) {
  if (!(this instanceof Service)) {
    return new Service(fullName);
  }

  /**
   * @property fullName
   * @type {string}
   */
  Object.defineProperty(this, 'fullName', {
    value: fullName,
    writable: false,
    enumerable: true
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

// app returns the app with the given name. relativeName should not contain
// slashes.
Service.prototype.app = function(relativeName) {
  return new App(this.fullName, relativeName);
};

// listApps returns a list of all app names.
Service.prototype.listApps = function(ctx, cb) {
  this._wire(ctx).listApps(ctx, cb);
};

Service.prototype.getPermissions = function(ctx, cb) {
  this._wire(ctx).getPermissions(ctx, cb);
};

Service.prototype.setPermissions = function(ctx, perms, version, cb) {
  this._wire(ctx).setPermissions(ctx, perms, version, cb);
};

Service.prototype._wire = function(ctx, cb) {
  if (!this._wireObj) {
    var rt = vanadium.runtimeForContext(ctx);
    var client = rt.getClient();
    this._wireObj = client.bindWithSignature(this.fullName, [wireSignature]);
  }

  return this._wireObj;
};
