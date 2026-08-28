/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.metrics;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;


public class SlidingWindowPercentileTest {
    int windowSize = 256;

    @Test
    void shouldCalculatePercentileCorrectly() throws InterruptedException {
        long delay = 200;

        SlidingWindowPercentile swp = new SlidingWindowPercentile(windowSize, 50, delay, 10);

        assertThat(swp.percentile()).isEqualTo(0);

        // Fill the window
        for (int i = 0; i < windowSize - 1; i++) {
            swp.record(i);
            assertThat(swp.percentile()).isEqualTo(0);
        }
        swp.record(windowSize - 1);
        assertThat(swp.percentile()).isEqualTo(127);
        swp.record(windowSize);
        assertThat(swp.percentile()).isEqualTo(127);

        Thread.sleep(delay + 10);
        swp.record(windowSize + 1);
        assertThat(swp.percentile()).isEqualTo(129);
    }

    @Test
    void shouldHandleConcurrency() throws InterruptedException {
        SlidingWindowPercentile swp = new SlidingWindowPercentile(windowSize, 50, 200, 10);

        // Create multiple threads to record values concurrently
        Thread[] threads = new Thread[10];
        AtomicInteger num = new AtomicInteger();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                int j;
                while ((j = num.getAndIncrement()) < windowSize) {
                    swp.record(j);
                }
            });
            threads[i].start();
        }

        // Wait for all threads to finish
        for (Thread thread : threads) {
            thread.join();
        }

        // Assert that percentile is calculated correctly (even with concurrency)
        assertThat(swp.percentile()).isEqualTo(127);
    }

    @Test
    void shouldReturnZeroForStalePercentile() throws InterruptedException {
        long delay = 10;
        int staleFactor = 10;
        SlidingWindowPercentile swp = new SlidingWindowPercentile(windowSize, 50, delay, staleFactor);

        // Fill the window and calculate percentile
        for (int i = 0; i < windowSize; i++) {
            swp.record(i);
        }
        assertThat(swp.percentile()).isEqualTo(127);

        // Wait for more than twice the compute delay
        Thread.sleep(delay * staleFactor + 10);

        assertThat(swp.percentile()).isZero();

        // Assert that percentile is considered too old and returns 0
        for (int i = 0; i < windowSize - 1; i++) {
            swp.record(i * 10);
            assertThat(swp.percentile()).isZero();
        }

        swp.record(windowSize * 10);
        assertThat(swp.percentile()).isEqualTo(1270);


    }
}
