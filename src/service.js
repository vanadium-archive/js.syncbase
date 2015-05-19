// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

module.exports = Service;

function Service() {
  if (typeof this !== Service) {
    return new Service();
  }
}

// app returns the app with the given name. relativeName should not contain
// slashes.
Service.prototype.app = function(ctx, relativeName) {};

// listApps returns a list of all app names.
Service.prototype.listApps = function(ctx) {};

Service.prototype.getPermissions = function(ctx) {};
Service.prototype.setPermissions = function(ctx) {};
