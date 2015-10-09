// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

module.exports = Syncgroup;

/**
 * Syncgroup is the interface for a syncgroup in the store.
 */
function Syncgroup(db, name) {
  if (!(this instanceof Syncgroup)) {
    return new Syncgroup(db, name);
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
 * Creates a new syncgroup with the given spec.
 *
 * Requires: Client must have at least Read access on the Database; prefix ACL
 * must exist at each syncgroup prefix; Client must have at least Read access
 * on each of these prefix ACLs.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:syncbase.nosql.SyncgroupSpec} spec SyncgroupSpec.
 * @param {module:syncbase.nosql.SyncgroupMemberInfo} myInfo
 * SyncgroupMemberInfo.
 * @param {function} cb Callback.
 */
Syncgroup.prototype.create = function(ctx, spec, myInfo, cb) {
  this._db._wire(ctx).createSyncgroup(ctx, this.name, spec, myInfo, cb);
};

/**
 * Joins a syncgroup.
 *
 * Requires: Client must have at least Read access on the Database and on the
 * syncgroup ACL.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:syncbase.nosql.SyncgroupMemberInfo} myInfo
 * SyncgroupMemberInfo.
 * @param {function} cb Callback.
 */
Syncgroup.prototype.join = function(ctx, myInfo, cb) {
  this._db._wire(ctx).joinSyncgroup(ctx, this.name, myInfo, cb);
};

/**
 * Leaves the syncgroup. Previously synced data will continue to be
 * available.
 *
 * Requires: Client must have at least Read access on the Database.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Syncgroup.prototype.leave = function(ctx, cb) {
  this._db._wire(ctx).leaveSyncgroup(ctx, this.name, cb);
};

/**
 * Destroys a syncgroup. Previously synced data will continue to be available
 * to all members.
 *
 * Requires: Client must have at least Read access on the Database, and must
 * have Admin access on the syncgroup ACL.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Syncgroup.prototype.destroy = function(ctx, cb) {
  this._db._wire(ctx).destroySyncgroup(ctx, this.name, cb);
};

/**
 * Ejects a member from the syncgroup. The ejected member will not be able to
 * sync further, but will retain any data it has already synced.
 *
 * Requires: Client must have at least Read access on the Database, and must
 * have Admin access on the syncgroup ACL.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:syncbase.nosql.SyncgroupMemberInfo} member
 * SyncgroupMemberInfo.
 * @param {function} cb Callback.
 */
Syncgroup.prototype.eject = function(ctx, member, cb) {
  this._db._wire(ctx).ejectFromSyncgroup(ctx, this.name, member, cb);
};

/**
 * Gets the syncgroup spec. version allows for atomic read-modify-write of the
 * spec - see comment for setSpec.
 *
 * Requires: Client must have at least Read access on the Database and on the
 * syncgroup ACL.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Syncgroup.prototype.getSpec = function(ctx, cb) {
  this._db._wire(ctx).getSyncgroupSpec(ctx, this.name, cb);
};

/**
 * Sets the syncgroup spec. version may be either empty or the value from a
 * previous Get. If not empty, Set will only succeed if the current version
 * matches the specified one.
 *
 * Requires: Client must have at least Read access on the Database, and must
 * have Admin access on the syncgroup ACL.
 *
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {module:syncbase.nosql.SyncgroupSpec} spec SyncgroupSpec.
 * @param {string} version Version of the current SyncgroupSpec object which
 * will be overwritten. If empty, setSpec will perform an unconditional update.
 * @param {function} cb Callback.
 */
Syncgroup.prototype.setSpec = function(ctx, spec, version, cb) {
  this._db._wire(ctx).setSyncgroupSpec(ctx, this.name, spec, version, cb);
};

/**
 * Gets the info objects for members of the syncgroup.
 *
 * Requires: Client must have at least Read access on the Database and on the
 * syncgroup ACL.
 * @param {module:vanadium.context.Context} ctx Vanadium context.
 * @param {function} cb Callback.
 */
Syncgroup.prototype.getMembers = function(ctx, cb) {
  this._db._wire(ctx).getSyncgroupMembers(ctx, this.name, cb);
};
