/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lakestream.ursa.materialization.serde.utils.json.types;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;

public class IntTimeMillisConverter extends AbstractIntDateTimeConverter {
    public static final AvroTypeConverter INSTANCE = new IntTimeMillisConverter(DateTimeFormatter.ISO_TIME);

    private final DateTimeFormatter dateTimeFormatter;

    public IntTimeMillisConverter(DateTimeFormatter dateTimeFormatter) {
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    protected Object convertDateTimeString(String dateTimeString) {
        long nanoOfDay = parseLocalTime(dateTimeString).toNanoOfDay();
        return Math.toIntExact(TimeUnit.NANOSECONDS.toMillis(nanoOfDay));
    }

    protected LocalTime parseLocalTime(String dateTimeString) {
        return LocalTime.from(dateTimeFormatter.parse(dateTimeString));
    }

    @Override
    protected LogicalType getLogicalType() {
        return LogicalTypes.timeMillis();
    }

    @Override
    protected String getValidStringFormat() {
        return "time";
    }

    @Override
    protected String getValidNumberFormat() {
        return "millis";
    }
}
