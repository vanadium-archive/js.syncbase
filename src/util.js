// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

var inherits = require('inherits');
var vanadium = require('vanadium');

module.exports = {
  addNameProperties: addNameProperties,
  getChildNames: getChildNames,
  InvalidNameError: InvalidNameError
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
   * @property name
   * @type {string}
   */
  Object.defineProperty(self, 'name', {
    value: relativeName,
    writable: false,
    enumerable: true
  });

  /**
   * @property name
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
  var namespace = rt.namespace();
  var childNames = [];

  var globPattern = vanadium.naming.join(parentFullName, '*');

  var stream = namespace.glob(ctx, globPattern, function(err) {
    if (err) {
      return cb(err);
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
  });
}
