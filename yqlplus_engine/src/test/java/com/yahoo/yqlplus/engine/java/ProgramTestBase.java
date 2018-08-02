package com.yahoo.yqlplus.engine.java;

import com.google.common.collect.ImmutableList;
import com.yahoo.yqlplus.engine.YQLPlusCompiler;
import com.yahoo.yqlplus.engine.YQLPlusEngine;

public class ProgramTestBase {
    public YQLPlusCompiler createCompiler(Object... bindings) {
        return YQLPlusEngine.builder().bind(bindings).build();
    }

    protected PersonSource createPeopleTable() {
        return new PersonSource(ImmutableList.of(new Person("1", "bob", 0), new Person("2", "joe", 1), new Person("3", "smith", 2)));
    }

    protected MinionSource createMoreMinionsTable() {
        return new MinionSource(ImmutableList.of(new Minion("1", "2"), new Minion("1", "3"), new Minion("2", "1")));
    }

    protected CitizenSource createCitizenTable() {
        return new CitizenSource(ImmutableList.of(new Citizen("1", "German"), new Citizen("2", "Italian"), new Citizen("3", "U.S. American")));
    }
}
