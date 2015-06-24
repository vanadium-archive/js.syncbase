// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var Service = require('./service');
var nosql = require('./nosql');

module.exports = {
  newService: newService,
  nosql: nosql,
  // syncbaseSuffix is used for Syncbase-to-Syncbase RPCs.  It should be
  // completely internal to syncbase, but currently syncgroup names must
  // include it for implementation-dependant reasons.
  //
  // TODO(nlacasse): This suffix should go away.  One possibility is to detect
  // "internal" RPCs by the method they call, and dispatch to different object
  // based on that method.  We could also have the client or server inject the
  // suffix automatically.
  syncbaseSuffix: '$sync'
};

function newService(fullName) {
  return new Service(fullName);
}
