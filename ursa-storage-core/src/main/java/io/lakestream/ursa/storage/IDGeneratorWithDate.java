/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage;

import io.lakestream.ursa.storage.impl.exception.IDGeneratorException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import lombok.extern.slf4j.Slf4j;

/**
 * An abstract class that generates IDs with a date prefix.
 */
@Slf4j
public abstract class IDGeneratorWithDate implements IDGenerator {

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH/mm/ss");
    private static final String DATE_ID_SEPARATOR = "/";

    /**
     * Generates an ID with a date prefix.
     *
     * @return A string containing the current date and a generated ID, separated by a slash.
     * @throws IDGeneratorException If there's an error generating the ID.
     */
    @Override
    public String generate() throws IDGeneratorException {
        String formattedDate = LocalDateTime.now().format(dateTimeFormatter);
        return formattedDate + DATE_ID_SEPARATOR + generateId();
    }

    public static boolean hasDatePrefix(String id) {
        try {
            getDatePrefix(id);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public static LocalDateTime getDatePrefix(String id) {
        if (id == null || !id.contains(DATE_ID_SEPARATOR)) {
            throw new IllegalArgumentException("Invalid ID format");
        }
        // Find position of 5th separator to validate format and extract date portion
        int pos = -1;
        int count = 0;
        for (int i = 0; i < id.length() && count < 6; i++) {
            if (id.charAt(i) == DATE_ID_SEPARATOR.charAt(0)) {
                pos = i;
                count++;
            }
        }

        if (count < 6) {
            throw new IllegalArgumentException("Invalid ID format: insufficient date segments");
        }

        // Extract the date portion without splitting the entire string
        String dateStr = id.substring(0, pos);
        return LocalDateTime.parse(dateStr, dateTimeFormatter);
    }

    public static String getDatePrefix(LocalDateTime dateTime) {
        return dateTime.format(dateTimeFormatter);
    }

    public static String getDummyDatePrefix() {
        return LocalDateTime.now().withHour(0).format(dateTimeFormatter) + DATE_ID_SEPARATOR + "__dummy";
    }

    /**
     * Check if the date prefix of the given ID is over than the given duration.
     *
     * @param prefix   The date prefix to check.
     * @param duration The duration to compare against.
     * @param from     The date to compare against.
     * @return True if the date prefix is over than the given duration, false otherwise.
     * @throws IllegalArgumentException If the date prefix is invalid.
     */
    public static boolean isDatePrefixOverThan(String prefix, Duration duration, LocalDateTime from)
        throws IllegalArgumentException {
        try {
            LocalDateTime prefixTime = LocalDateTime.parse(prefix, dateTimeFormatter);
            return Duration.between(prefixTime, from).toDays() > duration.toDays();
        } catch (Exception e) {
            log.warn("Found invalidated date prefix: {}. If it already expired, please delete it manually.", prefix);
            return false;
        }
    }

    protected abstract String generateId() throws IDGeneratorException;
}
