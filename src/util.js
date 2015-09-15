// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var vanadium = require('vanadium');

var NAME_SEP = '$';

module.exports = {
  addNameProperties: addNameProperties,
  prefixRangeLimit: prefixRangeLimit,
  stringToUTF8Bytes: stringToUTF8Bytes,
  NAME_SEP: NAME_SEP
};

/**
 * Creates the 'name' and 'fullName' properties on an object.
 * @private
 */
function addNameProperties(self, parentFullName, relativeName, addNameSep) {
  var fullName;
  if (addNameSep) {
    fullName = vanadium.naming.join(parentFullName, NAME_SEP, relativeName);
  } else {
    fullName = vanadium.naming.join(parentFullName, relativeName);
  }

  /**
   * @property _parentFullName
   * @private
   * @type {string}
   */
  Object.defineProperty(self, '_parentFullName', {
    value: parentFullName,
    writable: false,
    enumerable: false
  });

  /**
   * @property name
   * @type {string}
   */
  Object.defineProperty(self, 'name', {
    value: relativeName,
    writable: false,
    enumerable: true
  });

  /**
   * @property fullName
   * @type {string}
   */
  Object.defineProperty(self, 'fullName', {
    value: fullName,
    writable: false,
    enumerable: true
  });
}

/**
 * prefixRangeLimit modifies the input bytes to be the limit of the row range
 * for the given prefix.
 * TODO(sadovsky): Why do we modify the input bytes, rather than returning a new
 * byte array?
 * @private
 * @param {Uint8Array} bytes Integer ArrayBuffer to modify.
 */
function prefixRangeLimit(bytes) {
  // bytes can be thought of as a base-256 number. The code below effectively
  // adds 1 to this number, then chops off any trailing \x00 bytes. If the input
  // string consists entirely of \xff bytes, we return an empty string.
  while (bytes.length > 0) {
    var last = bytes.length - 1;
    if (bytes[last] === 255) {
      bytes = bytes.slice(0, last); // chop off trailing \x00
    } else {
      bytes[last] += 1; // add 1
      return; // no carry
    }
  }
}

/**
 * stringToUTF8Bytes converts a JavaScript string to a array of bytes
 * representing the string in UTF8 format.
 * @private
 * @param {string} str String to convert to UTF8 bytes.
 * @return {Uint8Array} UTF8 bytes.
 */
function stringToUTF8Bytes(str) {
  var utf8String = unescape(encodeURIComponent(str)); //jshint ignore:line
  var bytes = new Uint8Array(utf8String.length);

  for (var i = 0; i < utf8String.length; i++) {
    bytes[i] = utf8String.charCodeAt(i);
  }

  return bytes;
}
