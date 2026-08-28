#!/usr/bin/env bash
#
# SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
# SPDX-License-Identifier: Apache-2.0
#

set -e

version=${1#v}
if [[ "$version" == "" ]]; then
  echo "You need give a version number of the ursa storage"
  exit 1
fi

# Create a directory to save assets
ASSETS_DIR=release
mkdir -p "$ASSETS_DIR" "$ASSETS_DIR/bin" "$ASSETS_DIR/conf" "$ASSETS_DIR/lib"

mvn clean install -DskipTests

cp ursa-storage-compact/target/*.jar "$ASSETS_DIR/"
cp -R ursa-storage-compact/target/lib/. "$ASSETS_DIR/lib/"

rm -rf ursa-storage-core/target/*-with-dependencies.jar
rm -rf ursa-storage-core/target/*-tests.jar
cp ursa-storage-core/target/*.jar "$ASSETS_DIR/"

rm -rf ursa-storage-lakehouse/target/*-with-dependencies.jar
cp ursa-storage-lakehouse/target/*.jar "$ASSETS_DIR/"
cp ursa-storage-lakehouse-kafka-reader/target/*.jar "$ASSETS_DIR/"
cp ursa-storage-lakestream/target/*.jar "$ASSETS_DIR/"
cp lakestream-api/target/*.jar "$ASSETS_DIR/"
cp ursa-storage-materialization/target/*.jar "$ASSETS_DIR/"
cp ursa-storage-clickhouse/target/*.jar "$ASSETS_DIR/"

cp ursa-storage-common/target/*.jar "$ASSETS_DIR/"

cp bin/compact "$ASSETS_DIR/bin/"
cp conf/* "$ASSETS_DIR/conf/"
cp LICENSE "$ASSETS_DIR/"

ls "$ASSETS_DIR"
