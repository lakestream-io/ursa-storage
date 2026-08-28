/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.utils.lock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.TimeUnit;


public record OptionBackoff(
        long initDelay, TimeUnit initDelayUnit, long maxDelay, TimeUnit maxDelayUnit) {
    public static final OptionBackoff DEFAULT = new OptionBackoff(10, MILLISECONDS, 15, SECONDS);
}
