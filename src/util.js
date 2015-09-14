// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var inherits = require('inherits');
var vanadium = require('vanadium');

module.exports = {
  addNameProperties: addNameProperties,
  getChildNames: getChildNames,
  prefixRangeLimit: prefixRangeLimit,
  InvalidNameError: InvalidNameError,
  stringToUTF8Bytes: stringToUTF8Bytes
};

/**
 * Creates the 'name' and 'fullName' properties on an object.
 * @private
 */
function addNameProperties(self, parentFullName, relativeName) {
  if (relativeName.indexOf('/') >= 0) {
    throw new InvalidNameError(relativeName);
  }

  var fullName = vanadium.naming.join(parentFullName, relativeName);

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

function InvalidNameError(name) {
  Error.call(this);
  this.message = 'Invalid name "' + name + '". ' +
    ' Use vanadium.naming.encodeAsNamePart() to escape.';
}
inherits(InvalidNameError, Error);

/**
 * getChildNames returns all names that are children of the parentFullName.
 * @private
 */
function getChildNames(ctx, parentFullName, cb) {
  var rt = vanadium.runtimeForContext(ctx);
  var namespace = rt.getNamespace();
  var childNames = [];

  var globPattern = vanadium.naming.join(parentFullName, '*');

  var streamErr = null;

  var stream = namespace.glob(ctx, globPattern, function(err) {
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
    var name = vanadium.naming.basename(fullName);
    childNames.push(name);
  });

  stream.on('error', function(err) {
    console.error('Stream error: ' + JSON.stringify(err));
    // Store the first stream error in streamErr.
    streamErr = streamErr || err.error;
  });
}

/**
 * PrefixRangeLimit returns the limit of the row range for the given prefix.
 * @private
 * @param {Uint8Array} bytes Integer ArrayBuffer to modify.
 */
function prefixRangeLimit(bytes) {
  // For a given Uint8Array,
  // The code below effectively adds 1 to it, then chops off any
  // trailing \x00 bytes.
  // If the input string consists entirely of \xff bytes, we would empty out the
  // buffer
  while (bytes.length > 0) {
    var last = bytes.length - 1;
    if (bytes[last] === 255) {
      bytes = bytes.slice(0, last); // remove trailing \x00
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
