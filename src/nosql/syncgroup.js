// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

module.exports = SyncGroup;

/**
 * SyncGroup is the interface for a SyncGroup in the store.
 */
function SyncGroup(db, name) {
  if (!(this instanceof SyncGroup)) {
    return new SyncGroup(db, name);
  }

  /**
   * @private
   */
  Object.defineProperty(this, '_db', {
    enumerable: false,
    value: db,
    writable: false
  });

  /**
   * @property name
   * @type {string}
   */
  Object.defineProperty(this, 'name', {
    enumerable: true,
    value: name,
    writable: false
  });
}

/**
 * Creates a new SyncGroup with the given spec.
 *
 * Requires: Client must have at least Read access on the Database; prefix ACL
 * must exist at each SyncGroup prefix; Client must have at least Read access
 * on each of these prefix ACLs.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:syncbase.nosql.SyncGroupSpec} spec SyncGroupSpec.
 * @param {module:syncbase.nosql.SyncGroupMemberInfo} myInfo
 * SyncGroupMemberInfo.
 * @param {function} cb Callback.
 */
SyncGroup.prototype.create = function(ctx, spec, myInfo, cb) {
  this._db._wire(ctx).createSyncGroup(ctx, this.name, spec, myInfo, cb);
};

/**
 * Joins a SyncGroup.
 *
 * Requires: Client must have at least Read access on the Database and on the
 * SyncGroup ACL.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:syncbase.nosql.SyncGroupMemberInfo} myInfo
 * SyncGroupMemberInfo.
 * @param {function} cb Callback.
 */
SyncGroup.prototype.join = function(ctx, myInfo, cb) {
  this._db._wire(ctx).joinSyncGroup(ctx, this.name, myInfo, cb);
};

/**
 * Leaves the SyncGroup. Previously synced data will continue to be
 * available.
 *
 * Requires: Client must have at least Read access on the Database.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
SyncGroup.prototype.leave = function(ctx, cb) {
  this._db._wire(ctx).leaveSyncGroup(ctx, this.name, cb);
};

/**
 * Destroys a SyncGroup. Previously synced data will continue to be available
 * to all members.
 *
 * Requires: Client must have at least Read access on the Database, and must
 * have Admin access on the SyncGroup ACL.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
SyncGroup.prototype.destroy = function(ctx, cb) {
  this._db._wire(ctx).destroySyncGroup(ctx, this.name, cb);
};

/**
 * Ejects a member from the SyncGroup. The ejected member will not be able to
 * sync further, but will retain any data it has already synced.
 *
 * Requires: Client must have at least Read access on the Database, and must
 * have Admin access on the SyncGroup ACL.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:syncbase.nosql.SyncGroupMemberInfo} member
 * SyncGroupMemberInfo.
 * @param {function} cb Callback.
 */
SyncGroup.prototype.eject = function(ctx, member, cb) {
  this._db._wire(ctx).ejectFromSyncGroup(ctx, this.name, member, cb);
};

/**
 * Gets the SyncGroup spec. version allows for atomic read-modify-write of the
 * spec - see comment for setSpec.
 *
 * Requires: Client must have at least Read access on the Database and on the
 * SyncGroup ACL.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
SyncGroup.prototype.getSpec = function(ctx, cb) {
  this._db._wire(ctx).getSyncGroupSpec(ctx, this.name, cb);
};

/**
 * Sets the SyncGroup spec. version may be either empty or the value from a
 * previous Get. If not empty, Set will only succeed if the current version
 * matches the specified one.
 *
 * Requires: Client must have at least Read access on the Database, and must
 * have Admin access on the SyncGroup ACL.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:syncbase.nosql.SyncGroupSpec} spec SyncGroupSpec.
 * @param {string} version Version of the current SyncGroupSpec object which
 * will be overwritten. If empty, setSpec will perform an unconditional update.
 * @param {function} cb Callback.
 */
SyncGroup.prototype.setSpec = function(ctx, spec, version, cb) {
  this._db._wire(ctx).setSyncGroupSpec(ctx, this.name, spec, version, cb);
};

/**
 * Gets the info objects for members of the SyncGroup.
 *
 * Requires: Client must have at least Read access on the Database and on the
 * SyncGroup ACL.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
SyncGroup.prototype.getMembers = function(ctx, cb) {
  this._db._wire(ctx).getSyncGroupMembers(ctx, this.name, cb);
};
