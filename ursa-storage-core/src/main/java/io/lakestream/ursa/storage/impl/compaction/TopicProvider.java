/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.impl.compaction;

import java.util.ArrayList;
import java.util.List;

public class TopicProvider {

    private final List<String> allTopics = new ArrayList<>();

    public synchronized void updateTopics(List<String> newTopics) {
        allTopics.clear();
        allTopics.addAll(newTopics);
    }

    public synchronized List<String> getAllTopics() {
        return new ArrayList<>(allTopics);
    }

    public synchronized int getNumTopics() {
        return allTopics.size();
    }
}
