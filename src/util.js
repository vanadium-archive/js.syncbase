// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var vanadium = require('vanadium');

module.exports = {
  addNameProperties: addNameProperties,
  listChildren: listChildren,
  prefixRangeLimit: prefixRangeLimit,
  stringToUTF8Bytes: stringToUTF8Bytes,
  escape: escape,
  unescape: unescape
};

/**
 * Creates public 'name' and 'fullName' properties on an object, as well as a
 * private '_parentFullName' property.
 * @private
 */
function addNameProperties(self, parentFullName, name, fullName) {
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
    value: name,
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
 * listChildren returns the relative names of all children of parentFullName.
 * @private
 */
function listChildren(ctx, parentFullName, cb) {
  var rt = vanadium.runtimeForContext(ctx);
  var globPattern = vanadium.naming.join(parentFullName, '*');

  var childNames = [];
  var streamErr = null;
  var stream = rt.getNamespace().glob(ctx, globPattern, function(err) {
    if (err) {
      return cb(err);
    }
    if (streamErr) {
      return cb(streamErr);
    }
    cb(null, childNames);
  }).stream;

  stream.on('data', function(globResult) {
    var fullName = globResult.name;
    var escName = vanadium.naming.basename(fullName);
    // Component names within object names are always escaped. See comment in
    // server/nosql/dispatcher.go for explanation.
    // If unescape throws an exception, there's a bug in the Syncbase server.
    // Glob should return names with escaped components.
    childNames.push(unescape(escName));
  });

  stream.on('error', function(err) {
    console.error('Stream error: ' + JSON.stringify(err));
    streamErr = streamErr || err.error;
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

/**
 * escape escapes a component name for use in a Syncbase object name. In
 * particular, it replaces bytes "%" and "/" with the "%" character followed by
 * the byte's two-digit hex code. Clients using the client library need not
 * escape names themselves; the client library does so on their behalf.
 * @param {string} s String to escape.
 * @return {string} Escaped string.
 */
function escape(s) {
  return s
    .replace(/%/g, '%25')
    .replace(/\//g, '%2F');
}

/**
 * unescape applies the inverse of escape. Throws exception if the given string
 * is not a valid escaped string.
 * @param {string} s String to unescape.
 * @return {string} Unescaped string.
 */
function unescape(s) {
  return decodeURIComponent(s);
}
