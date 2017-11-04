/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.util;


import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.multibindings.MapBinder;
import com.yahoo.cloud.metrics.api.MetricDimension;
import com.yahoo.cloud.metrics.api.StandardRequestEmitter;
import com.yahoo.cloud.metrics.api.TaskMetricEmitter;
import com.yahoo.yqlplus.api.Exports;
import com.yahoo.yqlplus.api.annotations.Export;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.api.Record;
import com.yahoo.yqlplus.engine.api.ViewRegistry;
import com.yahoo.yqlplus.engine.guice.JavaEngineModule;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQuery;
import java.time.temporal.ValueRange;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class DateTimeTest {
    
    private Injector injector = Guice.createInjector(new JavaEngineModule(), new MetricModule(),
            new StandardLibraryModule(), new MyDummyModule(), new ViewRegistryModule());

    @Test
    public void requireThatCurDateWorks() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.now() cur_date OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertTrue(f2.get(0).get("cur_date") instanceof OffsetDateTime);
    }

    @Test
    public void requireThatCurrentTimeWorks() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.current_time() current_time OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertTrue(f2.get(0).get("current_time") instanceof OffsetTime);
    }

    @Test
    public void requireThatEpochCanBeParsed() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.from_epoch_second(1378489457) date OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f2.get(0).get("date").toString(), "2013-09-06T17:44:17Z");
    }

    @Test
    public void requireThatEpochWithOffsetCanBeParsed() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.from_epoch_second(1378489457, 4, 30) date OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f2.get(0).get("date").toString(), "2013-09-06T22:14:17+04:30");
    }

    @Test
    public void requireThatEpochWithOffsetCanBeParsedAndFormatted() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.to_epoch_second(datetime.from_epoch_second(1378489457, 4, 30)) date OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f2.get(0).get("date").toString(), "1378489457");
    }

    @Test
    public void requireThatDateWithoutOffsetCanBeParsed() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.from_date_string(\"2013-09-03\") date OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertTrue(f2.get(0).get("date") instanceof LocalDate);
        Assert.assertEquals(f2.get(0).get("date").toString(), "2013-09-03");
    }

    @Test
    public void requireThatTimeWithOffsetCanBeParsed() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.from_time_string(\"11:10:20+01:00\") time OUTPUT AS d1;");
        //program.dump(System.err);
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertTrue(f2.get(0).get("time") instanceof OffsetTime);
        Assert.assertEquals(f2.get(0).get("time").toString(), "11:10:20+01:00");
    }

    @Test
    public void requireThatTimeWithoutOffsetCanBeParsed() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.from_time_string(\"11:10:20\") time OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertTrue(f2.get(0).get("time") instanceof LocalTime);
        Assert.assertEquals(f2.get(0).get("time").toString(), "11:10:20");
    }

    @Test
    public void requireThatDateTimeWithoutOffsetCanBeParsed() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.from_datetime_string(\"2011-12-03T10:15:30\") date OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertTrue(f2.get(0).get("date") instanceof LocalDateTime);
        Assert.assertEquals(f2.get(0).get("date").toString(), "2011-12-03T10:15:30");
    }

    @Test
    public void requireThatDateTimeWithOffsetCanBeParsed() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.from_datetime_string(\"2011-12-03T10:15:30+01:00\") date OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertTrue(f2.get(0).get("date") instanceof ZonedDateTime);
        Assert.assertEquals(f2.get(0).get("date").toString(), "2011-12-03T10:15:30+01:00");
    }

    @Test
    public void requireThatDateTimeWithZoneCanBeParsed() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.from_datetime_string(\"2011-12-03T10:15:30+01:00[Europe/Paris]\") date OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertTrue(f2.get(0).get("date") instanceof ZonedDateTime);
        Assert.assertEquals(f2.get(0).get("date").toString(), "2011-12-03T10:15:30+01:00[Europe/Paris]");
    }

    @Test(expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp =
            "java.time.format.DateTimeParseException: Text \\'15:30\\+01:00\\[Europe\\/Paris\\]\\' could not be parsed at index 0")
    public void requireThatTimeCannotBeParsedAsADateTime() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.from_datetime_string(\"15:30+01:00[Europe/Paris]\") date OUTPUT AS d1;");
        result.getResult("d1").get().getResult();
    }

    @Test
    public void requireThatDateTimeWithCustomPatternCanBeParsed() throws Exception {
        ProgramResult result = getProgramResult("SELECT " +
                "datetime.from_string(\"2011-12-03\", \"yyyy-MM-dd\") date, " +
                "datetime.from_string(\"2011-12-03 10:15:30+01:00\", \"yyyy-MM-dd HH:mm:ssxxx\") date2, " +
                "datetime.from_string(\"2011-12-03 10:15:30+01:00 Europe/Paris\", \"yyyy-MM-dd HH:mm:ssxxx VV\") date3, " +
                "datetime.from_string(\"2011-12-03 10:15:30\", \"yyyy-MM-dd HH:mm:ss\") date4, " +
                "datetime.from_string(\"10---15---30\", \"HH---mm---ss\") date5, " +
                "datetime.from_string(\"10---15---30......+01:30\", \"HH---mm---ss......XXX\") date6 " +
                "OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertTrue(f2.get(0).get("date") instanceof LocalDate);
        Assert.assertEquals(f2.get(0).get("date").toString(), "2011-12-03");
        Assert.assertTrue(f2.get(0).get("date2") instanceof ZonedDateTime);
        Assert.assertEquals(f2.get(0).get("date2").toString(), "2011-12-03T10:15:30+01:00");
        Assert.assertTrue(f2.get(0).get("date3") instanceof ZonedDateTime);
        Assert.assertEquals(f2.get(0).get("date3").toString(), "2011-12-03T10:15:30+01:00[Europe/Paris]");
        Assert.assertTrue(f2.get(0).get("date4") instanceof LocalDateTime);
        Assert.assertEquals(f2.get(0).get("date4").toString(), "2011-12-03T10:15:30");
        Assert.assertTrue(f2.get(0).get("date5") instanceof LocalTime);
        Assert.assertEquals(f2.get(0).get("date5").toString(), "10:15:30");
        Assert.assertTrue(f2.get(0).get("date6") instanceof OffsetTime);
        Assert.assertEquals(f2.get(0).get("date6").toString(), "10:15:30+01:30");
    }

    @Test
    public void requireThatVariousFieldsCanBeExtractedFromDateTime() throws Exception {
        ProgramResult result = getProgramResult("SELECT " +
        "datetime.extract_field_value(datetime.from_datetime_string(\"2011-12-03T10:15:30+04:30\"), \"YEAR\") year, " +
        "datetime.extract_field_value(datetime.from_datetime_string(\"2011-12-03T10:15:30+04:30\"), \"MONTH_OF_YEAR\") month, " +
        "datetime.extract_field_value(datetime.from_datetime_string(\"2011-12-03T10:15:30+04:30\"), \"DAY_OF_MONTH\") day, " +
        "datetime.extract_field_value(datetime.from_datetime_string(\"2011-12-03T10:15:30+04:30\"), \"DAY_OF_WEEK\") day_week, " +
        "datetime.extract_field_value(datetime.from_datetime_string(\"2011-12-03T10:15:30+04:30\"), \"DAY_OF_YEAR\") day_year, " +
        "datetime.extract_field_value(datetime.from_datetime_string(\"2011-12-03T10:15:30+04:30\"), \"HOUR_OF_DAY\") hour, " +
        "datetime.extract_field_value(datetime.from_datetime_string(\"2011-12-03T10:15:30+04:30\"), \"MINUTE_OF_HOUR\") minute, " +
        "datetime.extract_field_value(datetime.from_datetime_string(\"2011-12-03T10:15:30+04:30\"), \"SECOND_OF_MINUTE\") second, " +
        "datetime.extract_field_value(datetime.from_datetime_string(\"2011-12-03T10:15:30+04:30\"), \"OFFSET_SECONDS\") offset2 " +
        " OUTPUT AS d1;");
        List<Record> f1 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f1.get(0).get("year"), 2011);
        Assert.assertEquals(f1.get(0).get("month"), 12);
        Assert.assertEquals(f1.get(0).get("day"), 3);
        Assert.assertEquals(f1.get(0).get("day_week"), 6);
        Assert.assertEquals(f1.get(0).get("day_year"), 337);
        Assert.assertEquals(f1.get(0).get("hour"), 10);
        Assert.assertEquals(f1.get(0).get("minute"), 15);
        Assert.assertEquals(f1.get(0).get("second"), 30);
        Assert.assertEquals(f1.get(0).get("offset2"), 16200);
    }

    @Test(expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp =
            "java.lang.IllegalArgumentException: No enum constant java.time.temporal.ChronoField.INVALID_FIELD")
    public void requireThatFieldExtractionReturnsExpectedExceptionForUnknownField() throws Exception {
        ProgramResult result = getProgramResult("SELECT " +
        "datetime.extract_field_value(datetime.from_datetime_string(\"2011-12-03T10:15:30+04:30\"), " +
                "\"INVALID_FIELD\") hour OUTPUT AS d1;");
        result.getResult("d1").get().getResult();
    }

    @Test
    public void requireThatVariousFieldsCanBeExtractedFromDate() throws Exception {
        ProgramResult result = getProgramResult("SELECT " +
                "datetime.extract_field_value(datetime.from_date_string(\"2011-12-03\"), \"YEAR\") year, " +
                "datetime.extract_field_value(datetime.from_date_string(\"2011-12-03\"), \"MONTH_OF_YEAR\") month, " +
                "datetime.extract_field_value(datetime.from_date_string(\"2011-12-03\"), \"DAY_OF_MONTH\") day, " +
                "datetime.extract_field_value(datetime.from_date_string(\"2011-12-03\"), \"DAY_OF_WEEK\") day_week, " +
                "datetime.extract_field_value(datetime.from_date_string(\"2011-12-03\"), \"DAY_OF_YEAR\") day_year " +
                " OUTPUT AS d1;");
        List<Record> f1 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f1.get(0).get("year"), 2011);
        Assert.assertEquals(f1.get(0).get("month"), 12);
        Assert.assertEquals(f1.get(0).get("day"), 3);
        Assert.assertEquals(f1.get(0).get("day_week"), 6);
        Assert.assertEquals(f1.get(0).get("day_year"), 337);
    }

    @Test(expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp =
            "java.time.temporal.UnsupportedTemporalTypeException: Unsupported field: MinuteOfHour")
    public void requireThatTimeValueFromDateReturnsAnException() throws Exception {
        ProgramResult result = getProgramResult("SELECT " +
             "datetime.extract_field_value(datetime.from_date_string(\"2011-12-03\"), \"MINUTE_OF_HOUR\") minute OUTPUT AS d1;");
        result.getResult("d1").get().getResult();
    }

    @Test
    public void requireThatVariousFieldsCanBeExtractedFromTime() throws Exception {
        ProgramResult result = getProgramResult("SELECT " +
                "datetime.extract_field_value(datetime.from_time_string(\"10:15:30+04:30\"), \"HOUR_OF_DAY\") hour, " +
                "datetime.extract_field_value(datetime.from_time_string(\"10:15:30+04:30\"), \"MINUTE_OF_HOUR\") minute, " +
                "datetime.extract_field_value(datetime.from_time_string(\"10:15:30+04:30\"), \"SECOND_OF_MINUTE\") second, " +
                "datetime.extract_field_value(datetime.from_time_string(\"10:15:30+04:30\"), \"OFFSET_SECONDS\") offset2 " +
                " OUTPUT AS d1;");
        List<Record> f1 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f1.get(0).get("hour"), 10);
        Assert.assertEquals(f1.get(0).get("minute"), 15);
        Assert.assertEquals(f1.get(0).get("second"), 30);
        Assert.assertEquals(f1.get(0).get("offset2"), 16200);
    }

    @Test(expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp =
            "java.time.temporal.UnsupportedTemporalTypeException: Unsupported field: DayOfYear")
    public void requireThatDateValueFromTimeReturnsAnException() throws Exception {
        ProgramResult result = getProgramResult("SELECT " +
        "datetime.extract_field_value(datetime.from_time_string(\"10:15:30+04:30\"), \"DAY_OF_YEAR\") day OUTPUT AS d1;");
        result.getResult("d1").get().getResult();
    }

    @Test
    public void requireThatDateTimeFormattingReturnsISO8601() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.format(datetime.from_epoch_second(1378489457)) date OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f2.get(0).get("date").toString(), "2013-09-06T17:44:17Z");
    }

    @Test
    public void requireThatDateTimeFormattingSupportsCustomPatterns() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.format(datetime.from_datetime_string(" +
                "\"2011-12-03T10:15:30+01:00\"), \"yyyy MM dd\") date OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f2.get(0).get("date").toString(), "2011 12 03");
    }

    @Test
    public void requireThatAddWorksWithDateTime() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.add(datetime.from_datetime_string(" +
                "\"2011-11-03T10:15:30+01:00[Europe/Paris]\"),\"P1Y1M5D\") add_date OUTPUT AS d1;");
        //program.dump(System.err);
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f2.get(0).get("add_date").toString(), "2012-12-08T10:15:30+01:00[Europe/Paris]");
    }

    @Test
    public void requireThatAddWorksWithTime() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.add(datetime.from_time_string(" +
                "\"10:15:30+04:30\"),\"PT1H1M1S\") add_date OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f2.get(0).get("add_date").toString(), "11:16:31+04:30");
    }

    @Test
    public void requireThatDurationCanBeAddedToDateTime() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.add(datetime.from_datetime_string(" +
                "\"2011-11-03T10:15:30+01:00[Europe/Paris]\"), \"PT1H1M1S\" ) add_date OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f2.get(0).get("add_date").toString(), "2011-11-03T11:16:31+01:00[Europe/Paris]");
    }

    @Test(expectedExceptions= ExecutionException.class, expectedExceptionsMessageRegExp= "java\\.lang\\.IllegalArgumentException: " +
            "com\\.yahoo\\.yqlplus\\.util\\.DateTimeTest\\$MyDummyExport\\$MyTemporalAccessor.*is not supported\\.")
    public void requireThatAddThrowsIllegalArgumentException() throws Exception {
        String query = "SELECT datetime.add(mydummymodule.get_mytemporal_accessor(), \"PT1H1M1S\" ) sub_date OUTPUT AS d1;";
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(query);
        ProgramResult result = program.run(ImmutableMap.of(), false);
        result.getResult("d1").get().getResult();

    }

    @Test
    public void requireThatSubWorksWithDateTime() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.sub(datetime.from_datetime_string(" +
                "\"2012-12-08T10:15:30+01:00[Europe/Paris]\"), \"P1Y1M5D\" ) sub_date OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f2.get(0).get("sub_date").toString(), "2011-11-03T10:15:30+01:00[Europe/Paris]");
    }

    @Test
    public void requireThatSubWorksWithTime() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.sub(datetime.from_time_string(" +
                "\"11:16:31+04:30\"),\"PT1H1M1S\") add_date OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f2.get(0).get("add_date").toString(), "10:15:30+04:30");
    }

    @Test
    public void requireThatDurationCanBeSubtractedFromDateTime() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.sub(datetime.from_datetime_string(" +
                "\"2012-12-08T10:15:30+01:00[Europe/Paris]\"), \"PT1H1M1S\" ) sub_date OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f2.get(0).get("sub_date").toString(), "2012-12-08T09:14:29+01:00[Europe/Paris]");
    }

    @Test(expectedExceptions= ExecutionException.class, expectedExceptionsMessageRegExp= "java\\.lang\\.IllegalArgumentException: " +
            "com\\.yahoo\\.yqlplus\\.util\\.DateTimeTest\\$MyDummyExport\\$MyTemporalAccessor.*is not supported\\.")
    public void requireThatSubThrowsIllegalArgumentException() throws Exception {
        String query = "SELECT datetime.sub(mydummymodule.get_mytemporal_accessor(), \"PT1H1M1S\" ) sub_date OUTPUT AS d1;";
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(query);
        ProgramResult result = program.run(ImmutableMap.of(), false);
        result.getResult("d1").get().getResult();
    }

    @Test
    public void requireThatEpochCanBeConvertedToDateTime() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.from_epoch_second(1378319479) from_epoch OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f2.get(0).get("from_epoch").toString(), "2013-09-04T18:31:19Z");
    }

    @Test
    public void requireThatEpochCanBeConvertedToDateTimeAndBack() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.to_epoch_second(datetime.from_epoch_second(1378319479)) " +
                "to_epoch OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f2.get(0).get("to_epoch").toString(), "1378319479");
    }

    @Test
    public void requireThatEpochMilliCanBeConvertedToDateTimeAndBack() throws Exception {
        ProgramResult result = getProgramResult("SELECT datetime.to_epoch_millisecond(datetime." +
                "from_epoch_millisecond(1378319479)) to_epoch OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f2.get(0).get("to_epoch").toString(), "1378319479");
    }

    @Test
    public void requireThatDateTimeCanBeTruncated() throws Exception {
        ProgramResult result = getProgramResult("SELECT " +
                "datetime.truncate_datetime(datetime.from_datetime_string(\"2013-11-03T10:15:30+04:30\"), \"HOURS\") truncated1, " +
                "datetime.truncate_datetime(datetime.from_datetime_string(\"2013-11-03T10:15:30\"), \"HOURS\") truncated2, " +
                "datetime.truncate_datetime(datetime.from_datetime_string(\"2013-11-03T10:15:30+04:30\"), \"MINUTES\") truncated3, " +
                "datetime.truncate_time(datetime.from_time_string(\"10:15:30+04:30\"), \"MINUTES\") truncated4, " +
                "datetime.truncate_time(datetime.from_time_string(\"10:15:30\"), \"HOURS\") truncated5 " +
                "OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f2.get(0).get("truncated1").toString(), "2013-11-03T10:00+04:30");
        Assert.assertEquals(f2.get(0).get("truncated2").toString(), "2013-11-03T10:00");
        Assert.assertEquals(f2.get(0).get("truncated3").toString(), "2013-11-03T10:15+04:30");
        Assert.assertEquals(f2.get(0).get("truncated4").toString(), "10:15+04:30");
        Assert.assertEquals(f2.get(0).get("truncated5").toString(), "10:00");
    }

    @Test(expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp =
            "java.time.temporal.UnsupportedTemporalTypeException: Unit is too large to be used for truncation")
    public void requireThatDateTimeCannotBeTruncated() throws Exception {
        ProgramResult result = getProgramResult("SELECT " +
                "datetime.truncate_datetime(datetime.from_datetime_string(\"2013-11-03T10:15:30+04:30\"), \"MONTHS\") " +
                "truncated1 OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f2.get(0).get("truncated1").toString(), "2013-11-03T10:00+04:30");
    }

    @Test
    public void requireThatFieldsCanBeUpdated() throws Exception {
        ProgramResult result = getProgramResult("PROGRAM (@datetime String = \"2013-11-03T10:15:30+04:30\"); SELECT " +
                "datetime.set_field(datetime.from_datetime_string(@datetime), \"YEAR\", 2014) updated1, " +
                "datetime.set_field(datetime.from_datetime_string(@datetime), \"DAY_OF_MONTH\", 15) updated2, " +
                "datetime.set_field(datetime.from_datetime_string(@datetime), \"HOUR_OF_DAY\", 23) updated3, " +
                "datetime.set_field(datetime.from_datetime_string(@datetime), \"SECOND_OF_MINUTE\", 31) updated4 " +
                " OUTPUT AS d1;");
        List<Record> f2 = result.getResult("d1").get().getResult();
        Assert.assertEquals(f2.get(0).get("updated1").toString(), "2014-11-03T10:15:30+04:30");
        Assert.assertEquals(f2.get(0).get("updated2").toString(), "2013-11-15T10:15:30+04:30");
        Assert.assertEquals(f2.get(0).get("updated3").toString(), "2013-11-03T23:15:30+04:30");
        Assert.assertEquals(f2.get(0).get("updated4").toString(), "2013-11-03T10:15:31+04:30");
    }

    @Test(expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp =
            "java.lang.IllegalArgumentException: No enum constant java.time.temporal.ChronoField.INVALID")
    public void requireThatInvalidFieldsThrowException() throws Exception {
        ProgramResult result = getProgramResult("PROGRAM (@datetime String = \"2013-11-03T10:15:30+04:30\"); SELECT " +
                "datetime.set_field(datetime.from_datetime_string(@datetime), \"INVALID\", 2014) updated1 OUTPUT AS d1;");
        result.getResult("d1").get().getResult();
    }

    private ProgramResult getProgramResult(String query) throws Exception {
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile(query);
        return program.run(ImmutableMap.of(), false);
    }
    
    public static class MetricModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(TaskMetricEmitter.class).toInstance(
                    new StandardRequestEmitter(new MetricDimension(),
                            arg0 -> {
                            }).start(new MetricDimension()));
        }
    }
    
    private static class ViewRegistryModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(ViewRegistry.class).toInstance(name -> null);
        }
    }

    public static class MyDummyModule extends AbstractModule {

        @Override
        protected void configure() {
            MapBinder<String, Exports> exportsBindings = MapBinder.newMapBinder(binder(), String.class, Exports.class);
            exportsBindings.addBinding("mydummymodule").to(MyDummyExport.class);
        }
    }

    public static class MyDummyExport implements Exports {

        @Export
        public MyTemporalAccessor get_mytemporal_accessor() {
            return new MyTemporalAccessor();
        }


        class MyTemporalAccessor implements TemporalAccessor {

            @Override
            public int get(TemporalField arg0) {
                return 0;
            }

            @Override
            public long getLong(TemporalField arg0) {
                return 0;
            }

            @Override
            public boolean isSupported(TemporalField arg0) {
                return false;
            }

            @Override
            public <R> R query(TemporalQuery<R> arg0) {
                return null;
            }

            @Override
            public ValueRange range(TemporalField arg0) {
                return null;
            }

        }
    }

}
