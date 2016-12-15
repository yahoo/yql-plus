/*
 * Copyright (c) 2016 Yahoo Inc.
 * Licensed under the terms of the Apache version 2.0 license.
 * See LICENSE file for terms.
 */

package com.yahoo.yqlplus.engine.java;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.yqlplus.api.trace.TraceRequest;
import com.yahoo.yqlplus.engine.CompiledProgram;
import com.yahoo.yqlplus.engine.ProgramResult;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.YQLResultSet;
import com.yahoo.yqlplus.engine.api.Record;
import com.yahoo.yqlplus.engine.tools.TraceFormatter;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class JoinTest {

	private static final boolean DEBUG_DUMP = false;
	
	@Test
	public void testJoin() throws Exception {
		Injector injector = Guice.createInjector(new JavaTestModule());
		YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
		CompiledProgram program = compiler.compile("SELECT people.value master_name, people2.value minion_name " +
				"FROM people " +
				"JOIN minions ON people.id = minions.master_id " +
				"JOIN people people2 ON people2.id = minions.minion_id " +
				"OUTPUT AS foo;");
		// program.dump(System.err);
		ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
		YQLResultSet rez = myResult.getResult("foo").get();
		List<Record> foo = rez.getResult();
		Assert.assertEquals(foo.size(), 2);
		Assert.assertEquals(foo.get(0).get("master_name"), "bob");
		Assert.assertEquals(foo.get(0).get("minion_name"), "joe");
		Assert.assertEquals(foo.get(1).get("master_name"), "bob");
		Assert.assertEquals(foo.get(1).get("minion_name"), "smith");
	}

	@Test
	public void testVariableJoin() throws Exception {
		Injector injector = Guice.createInjector(new JavaTestModule());
		YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
		CompiledProgram program = compiler.compile(
				"SELECT people.value master_name, people.id master_id FROM people OUTPUT AS p1;" +
						"SELECT minions.master_id, minions.minion_id FROM minions OUTPUT AS m1;" +
						"SELECT people.master_name, people2.master_name minion_name " +
						"FROM p1 people " +
						"JOIN m1 minions ON people.master_id = minions.master_id " +
						"JOIN p1 people2 ON people2.master_id= minions.minion_id " +
				"OUTPUT AS foo;");
		// program.dump(System.err);
		ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
		YQLResultSet rez = myResult.getResult("foo").get();
		List<Record> foo = rez.getResult();
		Assert.assertEquals(foo.size(), 2);
		Assert.assertEquals(foo.get(0).get("master_name"), "bob");
		Assert.assertEquals(foo.get(0).get("minion_name"), "joe");
		Assert.assertEquals(foo.get(1).get("master_name"), "bob");
		Assert.assertEquals(foo.get(1).get("minion_name"), "smith");
	}

	@Test
	public void testInner() throws Exception {
		Injector injector = Guice.createInjector(new JavaTestModule());
		YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
		CompiledProgram program = compiler.compile("SELECT * from innersource OUTPUT as foo;" +
				"SELECT * FROM innersource WHERE id = '1' OUTPUT AS b1;" +
				"SELECT * FROM innersource WHERE id = '2' OUTPUT as b2;" +
				"SELECT * FROM innersource WHERE id IN ('1', '2') OUTPUT as b3;" +
                "SELECT * FROM innersource WHERE id IN (SELECT id FROM innersource) OR id = '3' ORDER BY id DESC OUTPUT as b4;");
        // program.dump(System.err);
		ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
		YQLResultSet rez = myResult.getResult("foo").get();
		List<Person> foo = rez.getResult();
		Assert.assertEquals(foo.size(), 1);
		Assert.assertEquals(foo.get(0).getId(), "1");

		YQLResultSet b1 = myResult.getResult("b1").get();
		List<Person> b1r = b1.getResult();
		Assert.assertEquals(b1r.size(), 1);
		Assert.assertEquals(b1r.get(0).getId(), "1");

		YQLResultSet b2 = myResult.getResult("b2").get();
		List<Person> b2r = b2.getResult();
		Assert.assertEquals(b2r.size(), 0);

		Assert.assertEquals(myResult.getResult("b3").get().getResult(), Lists.newArrayList(new Person("1", "joe", 1)));
		Assert.assertEquals(myResult.getResult("b4").get().getResult(), Lists.newArrayList(new Person("3", "smith", 1), new Person("1", "joe", 1)));

		dumpDebugInfo(program, myResult);
	}

    /**
     * Unit test for Ticket 6943641 ("Skip executing the right side source on a JOIN with NULL key")
     *
     * @throws Exception
     */
    @Test
    public void testJoinWithNullKeySkipped() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT people.value master_name, moreMinions.minion_id minion_name " +
                "FROM people " +
                "JOIN moreMinions ON people.id = moreMinions.master_id " +
                "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 3);
        Assert.assertEquals(foo.get(0).get("master_name"), "bob");
        Assert.assertEquals(foo.get(0).get("minion_name"), "2");
        Assert.assertEquals(foo.get(1).get("master_name"), "bob");
        Assert.assertEquals(foo.get(1).get("minion_name"), "3");
        Assert.assertEquals(foo.get(2).get("master_name"), "joe");
        Assert.assertEquals(foo.get(2).get("minion_name"), "1");

        program = compiler.compile("SELECT peopleWithNullId.value master_name, moreMinions.minion_id minion_name " +
                "FROM peopleWithNullId " +
                "JOIN moreMinions ON peopleWithNullId.id = moreMinions.master_id " +
                "OUTPUT AS foo;");
        myResult = program.run(ImmutableMap.<String, Object>of(), true);
        rez = myResult.getResult("foo").get();
        foo = rez.getResult();
        Assert.assertEquals(foo.size(), 1);
        Assert.assertEquals(foo.get(0).get("master_name"), "joe");
        Assert.assertEquals(foo.get(0).get("minion_name"), "1");
    }

    /*
     * Assert that the source on the right side of a JOIN is invoked with "null" keys (which are normally
     * filtered out by the container) due to the skipNull=false setting on its @Key annotation
     */
    @Test
    public void testJoinWithNullKeyNotSkipped() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT peopleWithNullId.value master_name, minionsWithSkipNullSetToFalse.minion_id minion_name " +
                "FROM peopleWithNullId " +
                "JOIN minionsWithSkipNullSetToFalse ON peopleWithNullId.id = minionsWithSkipNullSetToFalse.master_id " +
                "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 2);
        Assert.assertEquals(foo.get(0).get("master_name"), "bob");
        Assert.assertEquals(foo.get(0).get("minion_name"), "2");
        Assert.assertEquals(foo.get(1).get("master_name"), "joe");
        Assert.assertEquals(foo.get(1).get("minion_name"), "1");
    }

    @Test
    public void testJoinEmptyStringKeyWithSkipEmptyOrZeroSetToTrue() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT people.value master_name, moreMinions.minion_id minion_name " +
                "FROM people " +
                "JOIN moreMinions ON people.id = moreMinions.master_id " +
                "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 3);
        Assert.assertEquals(foo.get(0).get("master_name"), "bob");
        Assert.assertEquals(foo.get(0).get("minion_name"), "2");
        Assert.assertEquals(foo.get(1).get("master_name"), "bob");
        Assert.assertEquals(foo.get(1).get("minion_name"), "3");
        Assert.assertEquals(foo.get(2).get("master_name"), "joe");
        Assert.assertEquals(foo.get(2).get("minion_name"), "1");

        program = compiler.compile("SELECT peopleWithEmptyId.value master_name, moreMinions.minion_id minion_name " +
                "FROM peopleWithEmptyId " +
                "JOIN moreMinions ON peopleWithEmptyId.id = moreMinions.master_id " +
                "OUTPUT AS foo;");
        myResult = program.run(ImmutableMap.<String, Object>of(), true);
        rez = myResult.getResult("foo").get();
        foo = rez.getResult();
        Assert.assertEquals(foo.size(), 1);
        Assert.assertEquals(foo.get(0).get("master_name"), "joe");
        Assert.assertEquals(foo.get(0).get("minion_name"), "1");
    }

    /**
     * Ticket 6988462 - [YQLP] allow to specify single table to select all column (*) for JOIN
     *
     * @throws Exception
     */
    @Test
    public void testJoinWildcardOnLeftTable() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT people.*, moreMinions.minion_id minion_name " +
                "FROM people JOIN moreMinions ON people.id = moreMinions.master_id " +
                "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 3);

        /*
         * Assert record size is 7 fields: all the fields (6) from table "people"
         * plus the projected field from table "moreMinions"
         */

        Record record = foo.get(0);
        Assert.assertEquals(Records.getRecordSize(record), 7);
        Assert.assertEquals(record.get("value"), "bob");
        Assert.assertEquals(record.get("id"), "1");
        Assert.assertEquals(record.get("iid"), 1);
        Assert.assertEquals(record.get("iidPrimitive"), 1);
        Assert.assertEquals(record.get("otherId"), "1");
        Assert.assertEquals(record.get("score"), 0);
        Assert.assertEquals(record.get("minion_name"), "2");

        record = foo.get(1);
        Assert.assertEquals(Records.getRecordSize(record), 7);
        Assert.assertEquals(record.get("value"), "bob");
        Assert.assertEquals(record.get("id"), "1");
        Assert.assertEquals(record.get("iid"), 1);
        Assert.assertEquals(record.get("iidPrimitive"), 1);
        Assert.assertEquals(record.get("otherId"), "1");
        Assert.assertEquals(record.get("score"), 0);
        Assert.assertEquals(record.get("minion_name"), "3");

        record = foo.get(2);
        Assert.assertEquals(Records.getRecordSize(record), 7);
        Assert.assertEquals(record.get("value"), "joe");
        Assert.assertEquals(record.get("id"), "2");
        Assert.assertEquals(record.get("iid"), 2);
        Assert.assertEquals(record.get("iidPrimitive"), 2);
        Assert.assertEquals(record.get("otherId"), "2");
        Assert.assertEquals(record.get("score"), 1);
        Assert.assertEquals(record.get("minion_name"), "1");
    }

    /**
     * @throws Exception
     */
    @Test
    public void testLeftJoinWildcardOnLeftTable() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT people.*, moreMinions.minion_id minion_name " +
                "FROM people LEFT JOIN moreMinions ON people.id = moreMinions.master_id " +
                "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 4);

        /*
         * Assert record size is 7 fields: all the fields (6) from table "people"
         * plus the projected field from table "moreMinions"
         */

        Record record = foo.get(0);
        Assert.assertEquals(Records.getRecordSize(record), 7);
        Assert.assertEquals(record.get("value"), "bob");
        Assert.assertEquals(record.get("id"), "1");
        Assert.assertEquals(record.get("iid"), 1);
        Assert.assertEquals(record.get("iidPrimitive"), 1);
        Assert.assertEquals(record.get("otherId"), "1");
        Assert.assertEquals(record.get("score"), 0);
        Assert.assertEquals(record.get("minion_name"), "2");

        record = foo.get(1);
        Assert.assertEquals(Records.getRecordSize(record), 7);
        Assert.assertEquals(record.get("value"), "bob");
        Assert.assertEquals(record.get("id"), "1");
        Assert.assertEquals(record.get("iid"), 1);
        Assert.assertEquals(record.get("iidPrimitive"), 1);
        Assert.assertEquals(record.get("otherId"), "1");
        Assert.assertEquals(record.get("score"), 0);
        Assert.assertEquals(record.get("minion_name"), "3");

        record = foo.get(2);
        Assert.assertEquals(Records.getRecordSize(record), 7);
        Assert.assertEquals(record.get("value"), "joe");
        Assert.assertEquals(record.get("id"), "2");
        Assert.assertEquals(record.get("iid"), 2);
        Assert.assertEquals(record.get("iidPrimitive"), 2);
        Assert.assertEquals(record.get("otherId"), "2");
        Assert.assertEquals(record.get("score"), 1);
        Assert.assertEquals(record.get("minion_name"), "1");

        record = foo.get(3);
        Assert.assertEquals(Records.getRecordSize(record), 6); // Records do not emit NULL fields as being present
        Assert.assertEquals(record.get("value"), "smith");
        Assert.assertEquals(record.get("id"), "3");
        Assert.assertEquals(record.get("iid"), 3);
        Assert.assertEquals(record.get("iidPrimitive"), 3);
        Assert.assertEquals(record.get("otherId"), "3");
        Assert.assertEquals(record.get("score"), 2);
        Assert.assertNull(record.get("minion_name"));
    }

    /**
     * Ticket 6988462
     *
     * @throws Exception
     */
    @Test
    public void testJoinWildcardOnLeftTableNoMatchingRows() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT people.*, noMatchMinions.minion_id minion_name " +
                "FROM people JOIN noMatchMinions ON people.id = noMatchMinions.master_id " +
                "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 0);
    }

    /**
     * @throws Exception
     */
    @Test
    public void testJoinWildcardOnRightTable() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT people.value master_name, moreMinions.* " +
                "FROM people JOIN moreMinions ON people.id = moreMinions.master_id " +
                "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 3);

        /*
         * Assert record size is 3 fields: the projected field from table "people"
         * plus all the fields (2) from table "moreMinions"
         */

        Record record = foo.get(0);
        Assert.assertEquals(Records.getRecordSize(record), 3);
        Assert.assertEquals(record.get("master_name"), "bob");
        Assert.assertEquals(record.get("master_id"), "1");
        Assert.assertEquals(record.get("minion_id"), "2");

        record = foo.get(1);
        Assert.assertEquals(Records.getRecordSize(record), 3);
        Assert.assertEquals(record.get("master_name"), "bob");
        Assert.assertEquals(record.get("master_id"), "1");
        Assert.assertEquals(record.get("minion_id"), "3");

        record = foo.get(2);
        Assert.assertEquals(Records.getRecordSize(record), 3);
        Assert.assertEquals(record.get("master_name"), "joe");
        Assert.assertEquals(record.get("master_id"), "2");
        Assert.assertEquals(record.get("minion_id"), "1");
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void testJoinWildcardOnLeftAndRightTables() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT people.*, moreMinions.* " +
                "FROM people JOIN moreMinions ON people.id = moreMinions.master_id " +
                "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 3);

        /*
         * Assert record size is 8 fields: all the fields (6) from table "people"
         * plus all the fields (2) from table "moreMinions"
         */

        Record record = foo.get(0);
        Assert.assertEquals(Records.getRecordSize(record), 8);
        Assert.assertEquals(record.get("value"), "bob");
        Assert.assertEquals(record.get("id"), "1");
        Assert.assertEquals(record.get("iid"), 1);
        Assert.assertEquals(record.get("iidPrimitive"), 1);
        Assert.assertEquals(record.get("otherId"), "1");
        Assert.assertEquals(record.get("score"), 0);
        Assert.assertEquals(record.get("master_id"), "1");
        Assert.assertEquals(record.get("minion_id"), "2");

        record = foo.get(1);
        Assert.assertEquals(Records.getRecordSize(record), 8);
        Assert.assertEquals(record.get("value"), "bob");
        Assert.assertEquals(record.get("id"), "1");
        Assert.assertEquals(record.get("iid"), 1);
        Assert.assertEquals(record.get("iidPrimitive"), 1);
        Assert.assertEquals(record.get("otherId"), "1");
        Assert.assertEquals(record.get("score"), 0);
        Assert.assertEquals(record.get("master_id"), "1");
        Assert.assertEquals(record.get("minion_id"), "3");

        record = foo.get(2);
        Assert.assertEquals(Records.getRecordSize(record), 8);
        Assert.assertEquals(record.get("value"), "joe");
        Assert.assertEquals(record.get("id"), "2");
        Assert.assertEquals(record.get("iid"), 2);
        Assert.assertEquals(record.get("iidPrimitive"), 2);
        Assert.assertEquals(record.get("otherId"), "2");
        Assert.assertEquals(record.get("score"), 1);
        Assert.assertEquals(record.get("master_id"), "2");
        Assert.assertEquals(record.get("minion_id"), "1");
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void testLeftJoinWildcardOnLeftAndRightTables() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT people.*, moreMinions.* " +
                "FROM people LEFT JOIN moreMinions ON people.id = moreMinions.master_id " +
                "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 4);

        /*
         * Assert record size is 6 fields: all the fields (6) from table "people"
         * plus all the fields (2) from table "moreMinions" -- except now getFieldNames does not return
         * fields with null values.
         */

        Record record = foo.get(3);
        Assert.assertEquals(Records.getRecordSize(record), 6);
        Assert.assertEquals(record.get("value"), "smith");
        Assert.assertEquals(record.get("id"), "3");
        Assert.assertEquals(record.get("iid"), 3);
        Assert.assertEquals(record.get("iidPrimitive"), 3);
        Assert.assertEquals(record.get("otherId"), "3");
        Assert.assertEquals(record.get("score"), 2);
        Assert.assertNull(record.get("master_id"));
        Assert.assertNull(record.get("minion_id"));
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void testMultiJoinWildcard() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT people.*, images.*, moreMinions.* " +
                "FROM people " +
                "JOIN moreMinions ON people.id = moreMinions.master_id " +
                "JOIN images ON moreMinions.minion_id = images.imageId " +
                "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 2);

        /*
         * Assert record size is 10 fields: all the fields (6) from table "people"
         * plus all the fields (2) from table "moreMinions" plus all the fields (2) from table "images"
         */

        Record record = foo.get(0);
        Assert.assertEquals(Records.getRecordSize(record), 10);
        Assert.assertEquals(record.get("value"), "bob");
        Assert.assertEquals(record.get("id"), "1");
        Assert.assertEquals(record.get("iid"), 1);
        Assert.assertEquals(record.get("iidPrimitive"), 1);
        Assert.assertEquals(record.get("otherId"), "1");
        Assert.assertEquals(record.get("score"), 0);
        Assert.assertEquals(record.get("master_id"), "1");
        Assert.assertEquals(record.get("minion_id"), "3");
        Assert.assertEquals(record.get("imageId"), "3");
        Assert.assertEquals(record.get("filename"), "3.jpg");

        record = foo.get(1);
        Assert.assertEquals(Records.getRecordSize(record), 10);
        Assert.assertEquals(record.get("value"), "joe");
        Assert.assertEquals(record.get("id"), "2");
        Assert.assertEquals(record.get("iid"), 2);
        Assert.assertEquals(record.get("iidPrimitive"), 2);
        Assert.assertEquals(record.get("otherId"), "2");
        Assert.assertEquals(record.get("score"), 1);
        Assert.assertEquals(record.get("master_id"), "2");
        Assert.assertEquals(record.get("minion_id"), "1");
        Assert.assertEquals(record.get("imageId"), "1");
        Assert.assertEquals(record.get("filename"), "1.jpg");
    }

    /**
     * @throws Exception
     */
    @Test
    public void testProjectJoinRecord() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT people.*, moreMinions " +
                "FROM people LEFT JOIN moreMinions ON people.id = moreMinions.master_id " +
                "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 4);

        /*
         * Assert record size is 7 fields: all the fields (6) from table "people" plus nested "moreMinions" record
         */

        Record record = foo.get(0);
        Assert.assertEquals(Records.getRecordSize(record), 7);
        Assert.assertEquals(record.get("value"), "bob");
        Assert.assertEquals(record.get("id"), "1");
        Assert.assertEquals(record.get("iid"), 1);
        Assert.assertEquals(record.get("iidPrimitive"), 1);
        Assert.assertEquals(record.get("otherId"), "1");
        Assert.assertEquals(record.get("score"), 0);
        Minion minion = (Minion) record.get("moreMinions");
        Assert.assertEquals(minion.master_id, "1");
        Assert.assertEquals(minion.minion_id, "2");

        record = foo.get(1);
        Assert.assertEquals(Records.getRecordSize(record), 7);
        Assert.assertEquals(record.get("value"), "bob");
        Assert.assertEquals(record.get("id"), "1");
        Assert.assertEquals(record.get("iid"), 1);
        Assert.assertEquals(record.get("iidPrimitive"), 1);
        Assert.assertEquals(record.get("otherId"), "1");
        Assert.assertEquals(record.get("score"), 0);
        minion = (Minion) record.get("moreMinions");
        Assert.assertEquals(minion.master_id, "1");
        Assert.assertEquals(minion.minion_id, "3");
    }

    /**
     * Ticket 7143393
     *
     * @throws Exception
     */
    @Test
    public void testProjectJoinRecordAlias() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT p.*, m " +
                "FROM people p LEFT JOIN moreMinions m ON p.id = m.master_id " +
                "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 4);

        /*
         * Assert record size is 7 fields: all the fields (6) from table "people" plus nested "m" record
         */

        Record record = foo.get(0);
        Assert.assertEquals(Records.getRecordSize(record), 7);
        Assert.assertEquals(record.get("value"), "bob");
        Assert.assertEquals(record.get("id"), "1");
        Assert.assertEquals(record.get("iid"), 1);
        Assert.assertEquals(record.get("iidPrimitive"), 1);
        Assert.assertEquals(record.get("otherId"), "1");
        Assert.assertEquals(record.get("score"), 0);
        Minion minion = (Minion) record.get("m");
        Assert.assertEquals(minion.master_id, "1");
        Assert.assertEquals(minion.minion_id, "2");

        record = foo.get(1);
        Assert.assertEquals(Records.getRecordSize(record), 7);
        Assert.assertEquals(record.get("value"), "bob");
        Assert.assertEquals(record.get("id"), "1");
        Assert.assertEquals(record.get("iid"), 1);
        Assert.assertEquals(record.get("iidPrimitive"), 1);
        Assert.assertEquals(record.get("otherId"), "1");
        Assert.assertEquals(record.get("score"), 0);
        minion = (Minion) record.get("m");
        Assert.assertEquals(minion.master_id, "1");
        Assert.assertEquals(minion.minion_id, "3");
    }

    /**
     * @throws Exception
     */
    @Test
    public void testProjectJoinRecords() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT people, moreMinions " +
                "FROM people LEFT JOIN moreMinions ON people.id = moreMinions.master_id " +
                "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 4);

        /*
         * Assert record size is 2 fields: nested "people" record plus nested "moreMinions" record
         */

        Record record = foo.get(0);
        Assert.assertEquals(Records.getRecordSize(record), 2);
        Person person = (Person) record.get("people");
        Assert.assertEquals(person.getValue(), "bob");
        Assert.assertEquals(person.getId(), "1");
        Assert.assertEquals(person.getIid().intValue(), 1);
        Assert.assertEquals(person.getIidPrimitive(), 1);
        Assert.assertEquals(person.getOtherId(), "1");
        Assert.assertEquals(person.getScore(), 0);
        Minion minion = (Minion) record.get("moreMinions");
        Assert.assertEquals(minion.master_id, "1");
        Assert.assertEquals(minion.minion_id, "2");

        record = foo.get(1);
        Assert.assertEquals(Records.getRecordSize(record), 2);
        person = (Person) record.get("people");
        Assert.assertEquals(person.getValue(), "bob");
        Assert.assertEquals(person.getId(), "1");
        Assert.assertEquals(person.getIid().intValue(), 1);
        Assert.assertEquals(person.getIidPrimitive(), 1);
        Assert.assertEquals(person.getOtherId(), "1");
        Assert.assertEquals(person.getScore(), 0);
        minion = (Minion) record.get("moreMinions");
        Assert.assertEquals(minion.master_id, "1");
        Assert.assertEquals(minion.minion_id, "3");
    }

    /**
     * Ticket 7143393
     *
     * @throws Exception
     */
    @Test
    public void testProjectJoinRecordsWithSelectAlias() throws Exception {
        Injector injector = Guice.createInjector(new JavaTestModule());
        YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
        CompiledProgram program = compiler.compile("SELECT people AS pp, moreMinions AS mm " +
                "FROM people LEFT JOIN moreMinions ON people.id = moreMinions.master_id " +
                "OUTPUT AS foo;");
        ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
        YQLResultSet rez = myResult.getResult("foo").get();
        List<Record> foo = rez.getResult();
        Assert.assertEquals(foo.size(), 4);

        /*
         * Assert record size is 2 fields: nested "pp" record plus nested "mm" record
         */

        Record record = foo.get(0);
        Assert.assertEquals(Records.getRecordSize(record), 2);
        Person person = (Person) record.get("pp");
        Assert.assertEquals(person.getValue(), "bob");
        Assert.assertEquals(person.getId(), "1");
        Assert.assertEquals(person.getIid().intValue(), 1);
        Assert.assertEquals(person.getIidPrimitive(), 1);
        Assert.assertEquals(person.getOtherId(), "1");
        Assert.assertEquals(person.getScore(), 0);
        Minion minion = (Minion) record.get("mm");
        Assert.assertEquals(minion.master_id, "1");
        Assert.assertEquals(minion.minion_id, "2");

        record = foo.get(1);
        Assert.assertEquals(Records.getRecordSize(record), 2);
        person = (Person) record.get("pp");
        Assert.assertEquals(person.getValue(), "bob");
        Assert.assertEquals(person.getId(), "1");
        Assert.assertEquals(person.getIid().intValue(), 1);
        Assert.assertEquals(person.getIidPrimitive(), 1);
        Assert.assertEquals(person.getOtherId(), "1");
        Assert.assertEquals(person.getScore(), 0);
        minion = (Minion) record.get("mm");
        Assert.assertEquals(minion.master_id, "1");
        Assert.assertEquals(minion.minion_id, "3");
    }

    private void dumpDebugInfo(CompiledProgram program, ProgramResult myResult) throws InterruptedException, ExecutionException, IOException {
		if (DEBUG_DUMP) {
			//program.dump(System.err);
			TraceRequest trace = myResult.getEnd().get();
			TraceFormatter.dump(System.err, trace);
		}
	}
}
