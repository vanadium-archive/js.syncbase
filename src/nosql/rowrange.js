// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var util = require('../util');

/**
 * @summary
 * Provides utility methods to create different rowranges.
 * @namespace
 * @name rowrange
 * @memberof module:syncbase.nosql
 */
module.exports = {
  range: range,
  singleRow: singleRow,
  prefix: prefix,
  RowRange: RowRange
};

/**
 * Creates a range for the given start and limit.
 * RowRange represents all rows with keys in [start, limit). If limit is "", all
 * rows with keys >= start are included.
 * @param {string} start Start of the range.
 * @param {string} limit Range limit.
 * @return {module:syncbase.nosql~RowRange} A RowRange object.
 */
function range(start, limit) {
  var startBytes = util.stringToUTF8Bytes(start);
  var limitBytes = util.stringToUTF8Bytes(limit);
  return new RowRange(startBytes, limitBytes);
}

/**
 * Creates a range that only matches items of the given prefix.
 * @param {string} prefix Prefix.
 * @return {module:syncbase.nosql~Range} A Range object covering the prefix.
 * inherits from {@link module:syncbase.nosql~RowRange}
 */
function prefix(p) {
  var startBytes = util.stringToUTF8Bytes(p);
  var limitBytes = util.stringToUTF8Bytes(p);
  util.prefixRangeLimit(limitBytes);
  return new RowRange(startBytes, limitBytes);
}

var ASCII_NULL = '\x00';
/**
 * Creates a range that only matches a single row of the given key.
 * @param {string} row Row key.
 * @return {module:syncbase.nosql~RowRange} A RowRange object.
 */
function singleRow(row) {
  var startBytes = util.stringToUTF8Bytes(row);
  var limitBytes = util.stringToUTF8Bytes(row + ASCII_NULL);
  return new RowRange(startBytes, limitBytes);
}

/*
 * @summary
 * Represents a range of row values.
 * Private constructor. Use one of the utility methods such as
 * {@link module:syncbase.nosql.rowrange#rowrange},
 * {@link module:syncbase.nosql.rowrange#prefix},
 * {@link module:syncbase.nosql.rowrange#singleRow}
 * to create instances.
 * @inner
 * @constructor
 * @memberof {module:syncbase.nosql.rowrange}
 */
function RowRange(start, limit) {
  if (!(this instanceof RowRange)) {
    return new RowRange(start, limit);
  }

  /**
   * Start of range as byte[]
   * @type {Uint8Array}
   */
  Object.defineProperty(this, 'start', {
    value: start,
    writable: false,
    enumerable: true
  });

  /**
   * Limit of range as byte[]
   * @type {Uint8Array}
   */
  Object.defineProperty(this, 'limit', {
    value: limit,
    writable: false,
    enumerable: true
  });
}
