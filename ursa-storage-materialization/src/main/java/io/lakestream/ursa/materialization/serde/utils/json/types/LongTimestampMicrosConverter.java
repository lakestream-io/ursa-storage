/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lakestream.ursa.materialization.serde.utils.json.types;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;

public class LongTimestampMicrosConverter extends AbstractLongDateTimeConverter {
    public static final AvroTypeConverter INSTANCE = new LongTimestampMicrosConverter(DateTimeFormatter.ISO_DATE_TIME);

    private final DateTimeFormatter dateTimeFormatter;

    public LongTimestampMicrosConverter(DateTimeFormatter dateTimeFormatter) {
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    protected Object convertDateTimeString(String dateTimeString) {
        Instant instant = parseInstant(dateTimeString);
        // based on org.apache.avro.data.TimestampMicrosConversion
        long seconds = instant.getEpochSecond();
        int nanos = instant.getNano();

        if (seconds < 0 && nanos > 0) {
            long micros = Math.multiplyExact(seconds + 1, 1_000_000);
            long adjustment = (nanos / 1_000L) - 1_000_000;

            return Math.addExact(micros, adjustment);
        } else {
            long micros = Math.multiplyExact(seconds, 1_000_000);

            return Math.addExact(micros, nanos / 1_000);
        }
    }

    protected Instant parseInstant(String dateTimeString) {
        return Instant.from(dateTimeFormatter.parse(dateTimeString));
    }

    @Override
    protected LogicalType getLogicalType() {
        return LogicalTypes.timestampMicros();
    }

    @Override
    protected String getValidStringFormat() {
        return "date time";
    }

    @Override
    protected String getValidNumberFormat() {
        return "timestamp";
    }

}
