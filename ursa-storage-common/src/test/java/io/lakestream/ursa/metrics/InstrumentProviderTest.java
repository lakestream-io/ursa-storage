/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.metrics;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterBuilder;
import io.opentelemetry.api.metrics.MeterProvider;
import org.junit.jupiter.api.Test;

class InstrumentProviderTest {

    @Test
    void usesUrsaInstrumentationScope() {
        OpenTelemetry openTelemetry = mock(OpenTelemetry.class);
        MeterProvider meterProvider = mock(MeterProvider.class);
        MeterBuilder meterBuilder = mock(MeterBuilder.class);
        Meter meter = mock(Meter.class);

        when(openTelemetry.getMeterProvider()).thenReturn(meterProvider);
        when(meterProvider.meterBuilder(InstrumentProvider.INSTRUMENTATION_SCOPE)).thenReturn(meterBuilder);
        when(meterBuilder.setInstrumentationVersion(anyString())).thenReturn(meterBuilder);
        when(meterBuilder.build()).thenReturn(meter);

        InstrumentProvider provider = new InstrumentProvider(openTelemetry);

        assertSame(meter, provider.getMeter());
        verify(meterProvider).meterBuilder("io.lakestream.ursa.storage");
    }
}
