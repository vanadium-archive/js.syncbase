// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

module.exports = Table;

// Table represents a collection of Rows.
function Table(fullname, relativeName) {
  if (typeof this !== Table) {
    return new Table(fullname, relativeName);
  }

  /**
   * The relative name of this Table.
   * @property name
   * @type {string}
   */
  Object.defineProperty(this, 'name', {
    value: relativeName,
    writable: false
  });

  /**
   * The full name (object name) of this Table.
   * @property fullname
   * @type {string}
   */
  Object.defineProperty(this, 'fullname', {
    value: fullname,
    writable: false
  });
}

// Row returns the Row with the given primary key.
Table.prototype.row = function(key) {};

// Get stores the value for the given primary key in value. If value's type
// does not match the stored type, Get will return an error. Expected usage:
Table.prototype.get = function(ctx, key, value) {};

// Put writes the given value to this Table. The value's primary key field
// must be set.
Table.prototype.put = function(ctx, key, value) {};

// Delete deletes all rows in the given range. If the last row that is covered
// by a prefix from SetPermissions is deleted, that (prefix, perms) pair is
// removed.
Table.prototype.delete = function(ctx, range) {};

// Scan returns all rows in the given range. The returned stream reads from a
// consistent snapshot taken at the time of the Scan RPC.
Table.prototype.scan = function(ctx, range) {};

// SetPermissions sets the permissions for all current and future rows with
// the given prefix. If the prefix overlaps with an existing prefix, the
// longest prefix that matches a row applies. For example:
//     SetPermissions(ctx, Prefix("a/b"), perms1)
//     SetPermissions(ctx, Prefix("a/b/c"), perms2)
// The permissions for row "a/b/1" are perms1, and the permissions for row
// "a/b/c/1" are perms2.
//
// SetPermissions will fail if called with a prefix that does not match any
// rows.
Table.prototype.setPermissions = function(ctx, prefix, perms) {};

// GetPermissions returns an array of (prefix, perms) pairs. The array is
// sorted from longest prefix to shortest, so element zero is the one that
// applies to the row with the given key. The last element is always the
// prefix "" which represents the table's permissions -- the array will always
// have at least one element.
Table.prototype.getPermissions = function(ctx, key) {};

// DeletePermissions deletes the permissions for the specified prefix. Any
// rows covered by this prefix will use the next longest prefix's permissions
Table.prototype.deletePermissions = function(ctx, prefix) {};