/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.metrics;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class SlidingWindowPercentile {
    private final int windowSize;
    private final long computeDelayInMillis;
    private final long stalePercentileDelayInMillis;
    private final int[] window;
    private final int[] sortedWindow;
    private final int targetPercentile;
    private final AtomicInteger counter = new AtomicInteger();
    private final AtomicBoolean isComputingPercentile = new AtomicBoolean(false);
    private int percentile = 0;
    private long lastPercentileComputeTimeStamp = 0;

    public SlidingWindowPercentile(int windowSize,
                                   int targetPercentile,
                                   long computeDelayInMillis,
                                   int stalePercentileDelayFactor) {
        this.windowSize = windowSize;
        this.targetPercentile = targetPercentile;
        this.computeDelayInMillis = computeDelayInMillis;
        this.stalePercentileDelayInMillis = computeDelayInMillis * stalePercentileDelayFactor;
        this.window = new int[windowSize];
        this.sortedWindow = new int[windowSize];
    }


    public boolean record(int num) {
        int i = counter.getAndUpdate(
                current -> current < Integer.MAX_VALUE ? current + 1 :
                        windowSize + (Integer.MAX_VALUE % windowSize) + 1);
        window[i % windowSize] = num;

        if (i < (windowSize - 1)) {
            return false;
        }

        if (!isComputingPercentile.get()
                && System.currentTimeMillis() - lastPercentileComputeTimeStamp >= computeDelayInMillis
                && isComputingPercentile.compareAndSet(false, true)) {
            try {
                if (isFull()) {
                    percentile = computePercentile();
                    lastPercentileComputeTimeStamp = System.currentTimeMillis();
                    return true;
                }
            } finally {
                isComputingPercentile.set(false);
            }
        }

        return false;
    }

    private int computePercentile() {
        System.arraycopy(window, 0, sortedWindow, 0, windowSize);
        Arrays.sort(sortedWindow);
        int targetPercentileIndex = Math.max(0, (windowSize * targetPercentile / 100 - 1));
        return sortedWindow[targetPercentileIndex];
    }

    public int percentile() {
        if (isComputingPercentile.get()) {
            return 0;
        }

        if (System.currentTimeMillis() - lastPercentileComputeTimeStamp >= stalePercentileDelayInMillis) {
            if (isFull() && isComputingPercentile.compareAndSet(false, true)) {
                try {
                    counter.set(0);
                    percentile = 0;
                } finally {
                    isComputingPercentile.set(false);
                }
            }
            return 0;
        }

        return percentile;
    }

    private boolean isFull() {
        return counter.get() / windowSize > 0;
    }

    @Override
    public String toString() {
        return percentile + "," + Arrays.toString(sortedWindow);
    }

}
