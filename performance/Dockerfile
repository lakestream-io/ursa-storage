#
# SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
# SPDX-License-Identifier: Apache-2.0
#

FROM eclipse-temurin:21.0.3_9-jdk

ARG ARTHAS_VERSION=4.0.5

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates unzip vim wget \
    && rm -rf /var/lib/apt/lists/* \
    && wget -q "https://github.com/alibaba/arthas/releases/download/arthas-all-${ARTHAS_VERSION}/arthas-bin.zip" \
    && unzip -q arthas-bin.zip -d /arthas \
    && rm arthas-bin.zip

WORKDIR /opt/ursa

COPY ursa-storage-tools/target/ursa-storage-*-bin.tar.gz /tmp/ursa-storage-bin.tar.gz
RUN tar -xzf /tmp/ursa-storage-bin.tar.gz --strip-components=1 -C /opt/ursa \
    && rm /tmp/ursa-storage-bin.tar.gz
