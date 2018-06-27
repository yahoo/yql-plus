[![Build Status](https://travis-ci.org/yahoo/yql-plus.svg?branch=jdk10)](https://travis-ci.org/yahoo/yql-plus)

# YQL+ Engine

This is the YQL+ parser, execution engine, and source SDK.

## Modules

- yqlplus_language - YQL+ grammar and parser

  An ANTLRv4-based parser for the YQL+ grammar (see docs/language.md) which parses programs into 
  "OperatorNodes" -- an AST representation of a YQL+ program consisting of OperatorNodes of different types 
  of operators:
  
  - `StatementOperator`  - The top-level PROGRAM as well as argument declarations, view statements, queries, etc.
   - `SequenceOperators`  - Represents queries. Logically produces a sequence of records, includes operators like PROJECT, FILTER, SCAN
   - `ExpressionOperator` - Represents an expression -- used to represent projection expressions, filters, sorts, etc. 
   - `SortOperator`       - Represents ASC and DESC operators on sort expressions.
   - `ProjectOperator`    - Operations in a projection -- FIELD, FLATTEN (SELECT a.* ...)

- yqlplus_engine -- YQL+ execution engine

  This is the engine implementation. It has APIs for turning a stream or file into a `CompiledProgram` which is the interface for executing a program and getting results.

- yqlplus_source_api -- Source annotations and interfaces

 This module defines the API for writing sources, transforms (pipes), and UDFs. It contains interfaces for tagging an implementation of each as well as annotations for communicating binding information to the engine.

 Guice Multibindings are used to publish these implementations to the engine with names.


## Usage

- YQL+ language parser
```java     
ProgramParser parser = new ProgramParser();
parser.parse("query", 
             "PROGRAM (@uuid string,
                       @logo_type string=""); 
              SELECT p.id,
                     p.provider_id, 
                     p.provider_name,
                     p.provider_alias,  
                    {"name" : plogo.name, "image" :  plogo.image} logo
              FROM provider({}) AS p 
              LEFT JOIN provider_logo(@logo_type,{}) AS plogo 
              ON p.id = plogo.provider_id  
              WHERE p.id=@uuid
              OUTPUT AS provider;");
``` 
- Query a Data `Source`
 
    (1) Create a Data `Source`
```
  public class InnerSource implements Source {
      @Query
      @TimeoutBudget(minimumMilliseconds = 5, maximumMilliseconds = 100)
      public List<Person> scan(@TimeoutMilliseconds long timeoutMs) {
        Assert.assertTrue(timeoutMs <= 100, "timeoutBudget <= 100");
        // checking minimum is dodgy and leads to failures
        return ImmutableList.of(new Person("1", "joe", 1));
      }

      @Query
      public Person lookup(@Key("id") String id) {
        if ("1".equals(id)) {
            return new Person("1", "joe", 1);
        } else if ("3".equals(id)) {
            return new Person("3", "smith", 1);
        } else {
            return null;
        }
      }

      @Query
      public Person lookup(@Key("iid") Integer id) {
        return lookup(String.valueOf(id));
      }
  }
```

   
  (2) Bind `Source` instance
```java
public class JavaTestModule extends AbstractModule {
   @Override
    protected void configure() {
        install(new JavaEngineModule());
        MapBinder<String, Source> sourceBindings = MapBinder.newMapBinder(binder(), String.class, Source.class);
        sourceBindings.addBinding("innersource").to(InnerSource.class);
    }
}
```
   
  (3) Create `Program` to query the Data `Source`
 
```java
Injector injector = Guice.createInjector(new JavaTestModule());
YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
CompiledProgram program = compiler.compile("SELECT * FROM innersource WHERE id = '1' OUTPUT AS b1;");
ProgramResult myResult = program.run(ImmutableMap.<String, Object>of(), true);
YQLResultSet b1 = myResult.getResult("b1").get();
List<Person> b1r = b1.getResult();
 
```


- Import Java function to YQL+ `Program`

  (1) Create an `Export` class 
```
  public class DateTime implements Exports {

      @Export
      public Instant from_epoch_second(long epochSecond) {
          return Instant.ofEpochSecond(epochSecond);
      }
  }
```

   
  (2) Bind `Export` class
```java
public class StandardLibraryModule extends AbstractModule {

    @Override
    protected void configure() {
        MapBinder<String, Exports> exportsBindings = MapBinder.newMapBinder(binder(), String.class, Exports.class);
        exportsBindings.addBinding("datetime").to(DateTime.class);
    }
}
```
   
  (3) Use the `Export` function in `Program`
 
```java
YQLPlusCompiler compiler = injector.getInstance(YQLPlusCompiler.class);
CompiledProgram program = compiler.compile("SELECT datetime.from_epoch_second(1378489457) date OUTPUT AS d1;");
ProgramResult result = program.run(ImmutableMap.<String, Object>of(), false);
List<Record> f2 = result.getResult("d1").get().getResult();
```

## Maven

```xml
   
   <dependency>
      <groupId>com.yahoo.yqlplus</groupId>
      <artifactId>yqlplus_engine</artifactId>
      <version>1.0.1</version>
   </dependency>
   ...
   <dependency>   
      <groupId>com.yahoo.yqlplus</groupId>
      <artifactId>yqlplus_source_api</artifactId>
      <version>$1.0.1</version>
   </dependency>
   ...
   <dependency>   
      <groupId>com.yahoo.yqlplus</groupId>
      <artifactId>yqlplus_language</artifactId>
      <version>$1.0.1</version>
   </dependency> 
   ...
   <dependency>
      <groupId>com.yahoo.yqlplus</groupId>
      <artifactId>yqlplus_stdlib</artifactId>
      <version>1.0.1</version>
   </dependency>
   ...
   <repositories>
      <repository>
        <snapshots>
          <enabled>false</enabled>
        </snapshots>
        <id>bintray-yahoo-maven</id>
        <name>bintray</name>
        <url>http://yahoo.bintray.com/maven</url>
      </repository>
   </repositories>
```



## LICENSE

Copyright 2016 Yahoo Inc.

Licensed under the terms of the Apache version 2.0 license. See [LICENSE](/LICENSE) file for terms.

