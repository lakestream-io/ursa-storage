/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.v2;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Random;

public class DataGenerator {
    public static Instant generateRandomeInstant() {
        Random random = new Random();
        Instant start = Instant.now().minus(365, ChronoUnit.DAYS); // 1 year ago
        Instant end = Instant.now(); // current time
        long startSeconds = start.getEpochSecond();
        long endSeconds = end.getEpochSecond();
        long randomSeconds = startSeconds + (long) (random.nextDouble() * (endSeconds - startSeconds));
        return Instant.ofEpochSecond(randomSeconds);
    }
}
