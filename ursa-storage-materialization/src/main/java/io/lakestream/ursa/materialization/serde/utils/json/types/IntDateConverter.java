/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.lakestream.ursa.materialization.serde.utils.json.types;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;

public class IntDateConverter extends AbstractIntDateTimeConverter {
    public static final AvroTypeConverter INSTANCE = new IntDateConverter(DateTimeFormatter.ISO_DATE);

    private final DateTimeFormatter dateTimeFormatter;

    public IntDateConverter(DateTimeFormatter dateTimeFormatter) {
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    protected Object convertDateTimeString(String dateTimeString) {
        return Math.toIntExact(parseLocalDate(dateTimeString).toEpochDay());
    }

    protected LocalDate parseLocalDate(String dateTimeString) {
        return LocalDate.from(dateTimeFormatter.parse(dateTimeString));
    }

    @Override
    protected LogicalType getLogicalType() {
        return LogicalTypes.date();
    }

    @Override
    protected String getValidStringFormat() {
        return "date";
    }

    @Override
    protected String getValidNumberFormat() {
        return "epoch days";
    }
}
