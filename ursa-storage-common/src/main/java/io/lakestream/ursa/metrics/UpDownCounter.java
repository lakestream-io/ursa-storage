/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.metrics;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.LongUpDownCounterBuilder;
import io.opentelemetry.api.metrics.Meter;

public class UpDownCounter {

    private final LongUpDownCounter counter;
    private final Attributes attributes;

    UpDownCounter(Meter meter, String name, Unit unit, String description, Attributes attributes) {
        LongUpDownCounterBuilder builder = meter.upDownCounterBuilder(name)
                .setDescription(description)
                .setUnit(unit.toString());

        this.counter = builder.build();
        this.attributes = attributes;
    }

    public void increment() {
        add(1);
    }

    public void decrement() {
        add(-1);
    }

    public void add(long delta) {
        counter.add(delta, attributes);
    }

    public void subtract(long diff) {
        add(-diff);
    }
}
