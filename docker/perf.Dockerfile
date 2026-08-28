#
# SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
# SPDX-License-Identifier: Apache-2.0
#

FROM eclipse-temurin:17-jre

USER root

WORKDIR /opt/ursa

ADD ursa-storage-tools/target/ursa-storage-*-bin.tar.gz /opt/ursa
