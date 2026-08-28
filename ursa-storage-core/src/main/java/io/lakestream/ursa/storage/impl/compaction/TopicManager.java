/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface TopicManager {

    List<String> getAllTopics();

    CompletableFuture<TopicMetadata> getTopicMetadata(String topic);

    void close();

}
