#!/bin/bash
# Copyright 2015 The Vanadium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

# Start syncbased and mount in the mounttable.

# TODO(nlacasse): This file is needed because the javascript service-runner
# does not allow flags or arguments to the executables it starts.  We should
# fix service-runner to allow flags/arguments, and then have it start syncbased
# directly with the appropriate flags.  Then we can delete this file.
# TODO(rdaoud): how to cleanup the tmp test dir; "rm" here doesn't do it.

testdir="$(mktemp -d "${TMPDIR:-/tmp}"/sbtest.XXXXXXXX)"
syncbased -v=1 --name test/syncbased --engine memstore --root-dir "${testdir}" --v23.tcp.address 127.0.0.1:0
