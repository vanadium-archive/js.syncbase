// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var Service = require('./service');
var nosql = require('./nosql');

module.exports = {
  newService: newService,
  nosql: nosql
};

function newService(fullName) {
  return new Service(fullName);
}