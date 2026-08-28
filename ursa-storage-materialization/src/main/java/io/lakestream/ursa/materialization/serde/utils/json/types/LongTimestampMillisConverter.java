/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lakestream.ursa.materialization.serde.utils.json.types;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;

public class LongTimestampMillisConverter extends AbstractLongDateTimeConverter {
    public static final AvroTypeConverter INSTANCE = new LongTimestampMillisConverter(DateTimeFormatter.ISO_DATE_TIME);

    private final DateTimeFormatter dateTimeFormatter;

    public LongTimestampMillisConverter(DateTimeFormatter dateTimeFormatter) {
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    protected Object convertDateTimeString(String dateTimeString) {
        return parseInstant(dateTimeString).toEpochMilli();
    }

    protected Instant parseInstant(String dateTimeString) {
        return Instant.from(dateTimeFormatter.parse(dateTimeString));
    }

    @Override
    protected LogicalType getLogicalType() {
        return LogicalTypes.timestampMillis();
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
