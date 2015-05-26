// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.


/*
 * TODO(aghassemi) We want to have each test run in a completely clean state of
 * the syncbase but currently it is hard to do that since we would need to
 * restart a running service. For now we use a unique app name to isolate tests
 * but this is not the ideal approach.
 */

/*
 * Provides unique names for the duration of a full testing runtime so that
 * each test can create its own unique apps in the singleton syncbase and
 * therefore be independent of other tests.
 */
var counter = 0;
module.exports = function() {
  return 'testApp_' + counter++;
};