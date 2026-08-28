#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
# SPDX-License-Identifier: Apache-2.0
#

version=${1#v}
if [[ "x$version" == "x" ]]; then
  echo "You need to provide a version number for building ursa storage"
  exit 1
fi

mvn versions:set -DnewVersion=${version}
