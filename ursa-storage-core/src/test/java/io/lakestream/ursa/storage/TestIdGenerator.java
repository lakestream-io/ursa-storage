/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.lakestream.ursa.storage.impl.WALIdGenerator;
import io.lakestream.ursa.storage.impl.compaction.CompactFileIDGenerator;
import io.lakestream.ursa.storage.impl.exception.IDGeneratorException;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Cleanup;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

public class TestIdGenerator extends OxiaBasedTestClass {


    @Test
    public void testWALIdGenerate() throws Exception {
        WALIdGenerator walIdGenerator = WALIdGenerator.buildOxiaIdGenerator(oxiaClient);
        List<String> ids = new CopyOnWriteArrayList<>();
        @Cleanup("shutdownNow")
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            executorService.submit(() -> {
                try {
                    ids.add(walIdGenerator.generate().toString());
                } catch (IDGeneratorException ignore) {
                }
            });
        }
        Awaitility.await().until(() -> ids.size() == 10);
        ids.sort(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return Integer.parseInt(o1) - Integer.parseInt(o2);
            }
        });
        for (int i = 0; i < ids.size(); i++) {
            assertEquals(i, Integer.parseInt(ids.get(i)));
        }
    }

    @Test
    public void testCompactedStreamIdGenerate() throws Exception {
        CompactFileIDGenerator compactFileIDGenerator = CompactFileIDGenerator.buildOxiaGenerator(oxiaClient);
        List<String> ids = new CopyOnWriteArrayList<>();
        @Cleanup("shutdownNow")
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                int finalI = i;
                executorService.submit(() -> {
                    try {
                        ids.add(compactFileIDGenerator.generate(finalI).toString());
                    } catch (IDGeneratorException ignore) {
                        ignore.printStackTrace();
                    }
                });
            }
        }
        Awaitility.await().until(() -> ids.size() == 100);
        ids.sort(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return Integer.parseInt(o1) - Integer.parseInt(o2);
            }
        });
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                assertEquals(Integer.parseInt(ids.get(i * 10 + j)), i);
            }
        }
    }

}
