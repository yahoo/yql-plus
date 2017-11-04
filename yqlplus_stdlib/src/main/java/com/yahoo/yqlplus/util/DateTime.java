/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.util;

import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.annotations.Export;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalAmount;


/**
 * <p>Utility class providing basic date and time manipulation.</p>
 * <p>All Strings assume ISO 8601 format unless otherwise noted.</p>
 *
 * @version $id$
 */
public class DateTime implements Exports {

    /**
     * Returns an immutable {@code Instant} representing 1970-01-01T00:00:00Z.
     *
     * @return {@code Instant} representing 1970-01-01T00:00:00Z.
     */
    @Export
    public Instant epoch() {
        return Instant.EPOCH;
    }

    /**
     * Max supported time value.
     *
     * @return {@code Instant} representing the max time value.
     */
    @Export
    public Instant max() {
        return Instant.MAX;
    }

    /**
     * Min supported time value.
     *
     * @return {@code Instant} representing the min time value.
     */
    @Export Instant min() {
        return Instant.MIN;
    }

    /**
     * Current time including timezone.
     *
     * @return Current time
     */
    @Export
    public OffsetTime current_time() {
        return OffsetTime.now();
    }

    /**
     * Current date without time.
     *
     * @return Current date
     */
    @Export
    public LocalDate current_date() {
        return LocalDate.now();
    }

    /**
     * Current datetime including timezone.
     *
     * @return Current datetime
     */
    @Export
    public OffsetDateTime current_datetime() {
        return OffsetDateTime.now();
    }

    /**
     * Current datetime including timezone. Same as {@code current_datetime}.
     *
     * @return Current datetime
     */
    @Export
    public OffsetDateTime now() {
        return current_datetime();
    }

    /**
     * Current time without timezone.
     *
     * @return Current time
     */
    @Export
    public LocalTime local_time() {
        return LocalTime.now();
    }

    /**
     * Current datetime without timezone.
     *
     * @return Current datetime
     */
    @Export
    public LocalDateTime local_datetime() {
        return LocalDateTime.now();
    }

    /*
     * Parsers
     */

    /**
     * Parses the number of seconds since the epoch to an {@code Instant}.
     *
     * @param epochSecond seconds since epoch
     * @return {@code Instant} representation of epoch
     */
    @Export
    public Instant from_epoch_second(long epochSecond) {
        return Instant.ofEpochSecond(epochSecond);
    }

    /**
     * Parses the number of milliseconds since the epoch to an {@code Instant}.
     *
     * @param epochMilli Milliseconds since epoch
     * @return {@code Instant} representation of epoch
     */
    @Export
    public Instant from_epoch_millisecond(long epochMilli) {
        return Instant.ofEpochMilli(epochMilli);
    }

    /**
     * Parses the number of seconds since the epoch using the given hours and minutes as zone offset.
     *
     * @param epochSecond seconds since epoch
     * @param hours zone hours offset, +/-
     * @param minutes zone minutes offset, +/-
     * @return {@code OffsetDateTime} representation of epoch
     */
    @Export
    public OffsetDateTime from_epoch_second(long epochSecond, int hours, int minutes) {
        return OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), ZoneOffset.ofHoursMinutes(hours, minutes));
    }

    /**
     * Parses the number of milliseconds since the epoch using the given hours and minutes as zone offset.
     *
     * @param epochMilli seconds since epoch
     * @param hours zone hours offset, +/-
     * @param minutes zone minutes offset, +/-
     * @return {@code OffsetDateTime} representation of epoch
     */
    @Export
    public OffsetDateTime from_epoch_millisecond(long epochMilli, int hours, int minutes) {
        return OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochMilli), ZoneOffset.ofHoursMinutes(hours, minutes));
    }

    /**
     * Parses date into a {@code TemporalAccessor}, without offset.
     * Note: dates with timezone offset are NOT supported.
     *
     * @see <a href="http://stackoverflow.com/questions/7788267/what-are-the-use-cases-justifying-the-310-offsetdate-type">Why no OffsetDate</a>
     *
     * @param date String representation of a date in ISO 8601 format
     * @return {@code LocalDate} representation of the date
     */
    @Export
    public LocalDate from_date_string(String date) {
        return DateTimeFormatter.ISO_DATE.parse(date, LocalDate::from);
    }

    /**
     * Parses time into a {@code TemporalAccessor}, with or without offset.
     *
     * @param time String representation of a time in ISO 8601 format
     * @return {@code TemporalAccessor} representation of the time
     */
    @Export
    public TemporalAccessor from_time_string(String time) {
        return DateTimeFormatter.ISO_TIME.parseBest(time, OffsetTime::from, LocalTime::from);
    }

    /**
     * Parses a datetime into a {@code TemporalAccessor}, with or without offset.
     *
     * @param datetime String representation of a datetime in ISO 8601 format
     * @return {@code TemporalAccessor} representation of the datetime
     */
    @Export
    public TemporalAccessor from_datetime_string(String datetime) {
        return DateTimeFormatter.ISO_DATE_TIME.parseBest(datetime, ZonedDateTime::from, LocalDateTime::from);
    }

    /**
     * Parses a date into a {@code TemporalAccessor} based on the provided pattern
     *
     * @see <a href="http://download.java.net/jdk8/docs/api/java/time/format/DateTimeFormatter.html#patterns">Supported Patterns</a>
     *
     * @param datetime Datetime to parse
     * @param pattern Pattern matching the date
     * @return {@code TemporalAccessor} representation of the parsed time
     */
    @Export
    public TemporalAccessor from_string(String datetime, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return formatter.parseBest(datetime, ZonedDateTime::from, LocalDateTime::from, LocalDate::from,
                OffsetTime::from, LocalTime::from);
    }

    /**
     * Extract the value of a specific field from the given {@code TemporalAccessor} if the field is supported.
     *
     * <p>Note: Querying any date field from a time value will throw a {@code DateTimeException}.</p>
     *
     * <p>Note: Unlike postgres for example, stand-alone dates are not internally converted to datetime. This means
     * that querying any time field will throw a {@code DateTimeException}.</p>
     *
     * @see <a href="http://download.java.net/jdk8/docs/api/java/time/temporal/ChronoField.html">Supported fields</a>
     *
     * @param temporal Temporal value to extract the field from
     * @param field field to extract the value from
     * @return int value of requested field, if found
     */
    @Export
    public int extract_field_value(TemporalAccessor temporal, String field) {
        return temporal.get(ChronoField.valueOf(field));
    }

    /* Formatters */

    /**
     * Converts the provided {@code TemporalAccessor} to seconds since epoch
     *
     * @param temporal {@code TemporalAccessor} to convert
     * @return seconds since epoch
     */
    @Export
    public long to_epoch_second(TemporalAccessor temporal) {
        return Instant.from(temporal).getEpochSecond();
    }

    /**
     * Converts the provided {@code TemporalAccessor} to milliseconds since epoch
     *
     * @param temporal {@code TemporalAccessor} to convert
     * @return milliseconds since epoch
     */
    @Export
    public long to_epoch_millisecond(TemporalAccessor temporal) {
        return Instant.from(temporal).toEpochMilli();
    }

    /**
     * Formats the provided {@code TemporalAccessor} according to ISO-8601.
     *
     * @param temporal {@code TemporalAccessor} representation of the datetime
     * @return ISO-8601 representation of the datetime
     */
    @Export
    public String format(TemporalAccessor temporal) {
        return temporal.toString();
    }

    /**
     * Formats the given datetime object according to the provided pattern.
     *
     * @see <a href="http://download.java.net/jdk8/docs/api/java/time/format/DateTimeFormatter.html#patterns">Supported Patterns</a>
     *
     * @param datetime datetime object
     * @param pattern Supported pattern
     * @return The formatted {@code TemporalAccessor}
     */
    @Export
    public String format(TemporalAccessor datetime, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return formatter.format(datetime);
    }

    /**
     * <p>Add an interval to {@code TemporalAccessor}.</p>
     * <p>Note: Please refer to the links for the difference between Period and Duration.</p>
     *
     * @see <a href="http://download.java.net/jdk8/docs/api/java/time/Period.html">Period</a>
     * @see <a href="http://download.java.net/jdk8/docs/api/java/time/Duration.html">Duration</a>
     *
     * @param datetime - {@code TemporalAccessor} representation of the datetime
     * @param interval - String representation of {@code Period} or {@code Duration}
     *                   eg: P1Y1M0D or P1DT1H1M1S or PT1H1M1S
     * @return {@code TemporalAccessor} representation of the result
     */
    @Export
    public TemporalAccessor add(TemporalAccessor datetime, String interval) {
        if (! (datetime instanceof Temporal)) {
            throw new IllegalArgumentException(datetime.toString() + " is not supported.");
        }
        TemporalAmount p;
        if(!interval.contains("T")) {
            p = Period.parse(interval);
        }
        else {
            p = Duration.parse(interval);
        }

        Temporal t = (Temporal) datetime;
        t = t.plus(p);
        return t;
    }

    /**
     * <p>Subtract an interval from {@code TemporalAccessor} based on ISO-8601.</p>
     * <p>Note: Please refer to the links for the difference between Period and Duration.</p>
     *
     * @see <a href="http://download.java.net/jdk8/docs/api/java/time/Period.html">Period</a>
     * @see <a href="http://download.java.net/jdk8/docs/api/java/time/Duration.html">Duration</a>
     *
     * @param datetime - {@code TemporalAccessor} representation of the datetime
     * @param interval - String representation of {@code Period} or {@code Duration}
     *                   eg: P1Y1M0D or P5DT1H1M1S or PT1H1M1S
     * @return {@code TemporalAccessor} representation of the result
     */
    @Export
    public TemporalAccessor sub(TemporalAccessor datetime, String interval) {
        if (! (datetime instanceof Temporal)) {
            throw new IllegalArgumentException(datetime.toString() + " is not supported.");
        }
        TemporalAmount p;
        if(!interval.contains("T")) {
            p = Period.parse(interval);
        }
        else {
            p = Duration.parse(interval);
        }

        Temporal t = (Temporal) datetime;
        t = t.minus(p);
        return t;
    }

    /**
     * Truncates according to the specified unit. Note: Currently only time truncation is supported.
     *
     * @see <a href="http://download.java.net/jdk8/docs/api/java/time/temporal/ChronoUnit.html">Supported Units</a>
     *
     * @param temporal Temporal value to truncate
     * @param unit Time unit to truncate
     * @return New and immutable truncated {@code TemporalAccessor}
     */
    @Export
    public TemporalAccessor truncate_datetime(TemporalAccessor temporal, String unit) {
        ChronoUnit chronoUnit = ChronoUnit.valueOf(unit);

        if (temporal.isSupported(ChronoField.OFFSET_SECONDS)) {
            ZonedDateTime zonedDataTime = ZonedDateTime.from(temporal);
            return zonedDataTime.truncatedTo(chronoUnit);
        } else {
            LocalDateTime localDateTime = LocalDateTime.from(temporal);
            return localDateTime.truncatedTo(chronoUnit);
        }
    }

    /**
     * Truncates according to the specified unit. Note: Currently only time truncation is supported.
     *
     * @see <a href="http://download.java.net/jdk8/docs/api/java/time/temporal/ChronoUnit.html">Supported Units</a>
     *
     * @param temporal Temporal value to truncate
     * @param unit Time unit to truncate
     * @return New and immutable truncated {@code TemporalAccessor}
     */
    @Export
    public TemporalAccessor truncate_time(TemporalAccessor temporal, String unit) {
        ChronoUnit chronoUnit = ChronoUnit.valueOf(unit);

        if (temporal.isSupported(ChronoField.OFFSET_SECONDS)) {
            OffsetTime offsetTime = OffsetTime.from(temporal);
            return offsetTime.truncatedTo(chronoUnit);
        } else {
            LocalTime localTime = LocalTime.from(temporal);
            return localTime.truncatedTo(chronoUnit);
        }
    }

    /**
     * Sets the specified field in the {@code Temporal} to the given value.
     *
     * @param temporalAccessor {@code TemporalAccessor} with the field to update
     * @param field Field to update
     * @param value Desired value
     * @return A new Object of same type as the one specified, with the field updated
     */
    @Export
    public TemporalAccessor set_field(TemporalAccessor temporalAccessor, String field, long value) {
        if (temporalAccessor instanceof Temporal) {
            return ((Temporal) temporalAccessor).with(ChronoField.valueOf(field), value);
        } else {
            throw new IllegalArgumentException(temporalAccessor.toString() + " is not supported.");
        }
    }

}
