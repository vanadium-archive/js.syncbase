// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var vom = require('vanadium').vom;

var watchVdl = require('../gen-vdl/v.io/v23/services/watch');

module.exports = {
  ResumeMarker: watchVdl.ResumeMarker,
  WatchChange: WatchChange
};

/**
 * WatchChange represents the new value for a watched entity.
 * @constructor
 */
function WatchChange(opts) {
  /**
   * @property tableName
   * The name of the table that contains the changed row
   */
  Object.defineProperty(this, 'tableName', {
    enumerable: true,
    value: opts.tableName,
    writable: false
  });

  /**
   * @property rowName
   * Name of the changed row.
   */
  Object.defineProperty(this, 'rowName', {
    enumerable: true,
    value: opts.rowName,
    writable: false
  });

  /**
   * @property changeType
   * Describes the type of the change. If the changeType equals 'put', then the
   * row exists in the table and the value contains the new value for this row.
   * If the state equals 'delete', then the row was removed from the table.
   */
  Object.defineProperty(this, 'changeType', {
    enumerable: true,
    value: opts.changeType,
    writable: false
  });

  /**
   * @property valueBytes
   * The new VOM-encoded value for the row if the changeType is 'put' or nil
   * otherwise.
   */
  Object.defineProperty(this, 'valueBytes', {
    enumerable: true,
    value: opts.valueBytes,
    writable: false
  });

  /**
   * @property resumeMarker
   * Provides a compact representation of all the messages that have been
   * received by the caller for the given watch call.  This marker can be
   * provided in the request message to allow the caller to resume the stream
   * watching at a specific point without fetching the initial state.
   */
  Object.defineProperty(this, 'resumeMarker', {
    enumerable: true,
    value: opts.resumeMarker,
    writable: false
  });

  /**
   * @property fromSync
   * Indicates whether the change came from sync. If fromSync is false, then
   * the change originated from the local device.
   */
  Object.defineProperty(this, 'fromSync', {
    enumerable: true,
    value: opts.fromSync || false,
    writable: false
  });

  /**
   * @property continued
   * If true, this WatchChange is followed by more WatchChanges that are in the
   * same batch as this WatchChange
   */
  Object.defineProperty(this, 'continued', {
    enumerable: true,
    value: opts.continued || false,
    writable: false
  });
}

/**
 * Decodes the new value of the watched element.
 */
WatchChange.prototype.getValue = function(cb) {
  if (this.changeType === 'delete') {
    return cb(new Error('invalid change type'));
  }

  vom.decode(this.valueBytes, false, null, cb);
};
