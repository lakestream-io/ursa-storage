/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.metrics;

import io.lakestream.ursa.compaction.metrics.CompactionLatencyHistogram;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import lombok.Getter;

public class InstrumentProvider {

    static final String INSTRUMENTATION_SCOPE = "io.lakestream.ursa.storage";
    private static final String UNKNOWN_VERSION = "unknown";
    private static final String VERSION_RESOURCE =
            "/META-INF/maven/io.lakestream/ursa-storage-common/pom.properties";

    public static final InstrumentProvider NOOP = new InstrumentProvider(OpenTelemetry.noop());

    @Getter
    private final Meter meter;

    public InstrumentProvider(OpenTelemetry otel) {
        if (otel == null) {
            // By default, metrics are disabled, unless the OTel java agent is configured.
            // This allows to enable metrics without any code change.
            otel = GlobalOpenTelemetry.get();
        }
        this.meter = otel.getMeterProvider()
                .meterBuilder(INSTRUMENTATION_SCOPE)
                .setInstrumentationVersion(getInstrumentationVersion())
                .build();
    }

    private static String getInstrumentationVersion() {
        Package providerPackage = InstrumentProvider.class.getPackage();
        if (providerPackage != null && providerPackage.getImplementationVersion() != null) {
            return providerPackage.getImplementationVersion();
        }

        try (InputStream input = InstrumentProvider.class.getResourceAsStream(VERSION_RESOURCE)) {
            if (input == null) {
                return UNKNOWN_VERSION;
            }
            Properties properties = new Properties();
            properties.load(input);
            return properties.getProperty("version", UNKNOWN_VERSION);
        } catch (IOException ignored) {
            return UNKNOWN_VERSION;
        }
    }

    public Counter newCounter(String name, Unit unit, String description, Attributes attributes) {
        return new Counter(meter, name, unit, description, attributes);
    }

    public UpDownCounter newUpDownCounter(String name, Unit unit, String description, Attributes attributes) {
        return new UpDownCounter(meter, name, unit, description, attributes);
    }

    public LatencyHistogram newLatencyHistogram(String name, String description, Attributes attributes) {
        return new LatencyHistogram(meter, name, description, attributes);
    }

    public CompactionLatencyHistogram newCompactionLatencyHistogram(String name,
                                                                    String description,
                                                                    Attributes attributes) {
        return new CompactionLatencyHistogram(meter, name, description, attributes);
    }

    public RequestSizeHistogram newRequestSizeHistogram(String name, String description, Attributes attributes) {
        return new RequestSizeHistogram(meter, name, description, attributes);
    }
}
