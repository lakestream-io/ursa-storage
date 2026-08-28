/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.utils.lock;

import java.util.concurrent.TimeUnit;

public record OptionAutoRevalidate(boolean enabled, long initDelay, long delay, TimeUnit unit) {

    public static final OptionAutoRevalidate DEFAULT =
            new OptionAutoRevalidate(true, 15, 15, TimeUnit.MINUTES);
}
