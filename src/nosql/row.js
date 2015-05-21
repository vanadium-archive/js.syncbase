// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

module.exports = Row;

// Row represents a single row in a Table.
function Row(fullName, key) {
  // TODO(aghassemi) We may need to escape the key. Align with Go implementation
  // when that decision is made.
  if (typeof this !== Row) {
    return new Row(fullName, key);
  }

  /**
   * The key of this Row.
   * @property name
   * @type {string}
   */
  Object.defineProperty(this, 'key', {
    value: key,
    writable: false
  });

  /**
   * The full name (object name) of this Row.
   * @property fullName
   * @type {string}
   */
  Object.defineProperty(this, 'fullName', {
    value: fullName,
    writable: false
  });
}

// Get stores the value for the given primary key in value. If value's type
// does not match the stored type, Get will return an error. Expected usage:
Row.prototype.get = function(ctx, key, value) {};

// Put writes the given value to this Table. The value's primary key field
// must be set.
Row.prototype.put = function(ctx, key, value) {};

// Delete deletes all rows in the given range. If the last row that is covered
// by a prefix from SetPermissions is deleted, that (prefix, perms) pair is
// removed.
Row.prototype.delete = function(ctx, range) {};