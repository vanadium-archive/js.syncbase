// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var Database = require('./database');
var runInBatch = require('./batch');

module.exports = {
  newDatabase: newDatabase,
  runInBatch: runInBatch
};

function newDatabase(name, relativeName) {
  return new Database(name, relativeName);
}