/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction;

import java.util.Map;

public record TopicMetadata(String topic, Long streamId, Map<String, String> properties) {
}
