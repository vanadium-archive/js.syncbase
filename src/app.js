// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

module.exports = App;

function App(name) {
  if (typeof this !== App) {
    return new App(name);
  }

  this.name = name;
}

// noSqlDatabase returns the noSqlDatabase with the given name. relativeName
// must not contain slashes.
App.prototype.noSqlDatabase = function(ctx, relativeName) {};

// listDatabases returns of all database names.
App.prototype.listDatabases = function(ctx) {};

// create creates this app.  If perms is empty, we inherit (copy) the Service
// perms.
App.prototype.create = function(ctx, perms) {};

// delete deletes this app.
App.prototype.delete = function(ctx) {};

App.prototype.getPermissions = function(ctx) {};
App.prototype.setPermissions = function(ctx) {};
