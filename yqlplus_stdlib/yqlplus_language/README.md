# YQL+ 1.0 Language Specification

# Introduction

YQL+ is for writing data access for serving applications. It provides access to one or more serving systems via a query language and provides for describing processing and queries. YQL+ is *declarative*.

The developer specifies the queries to return and the execution engine plans and executes based on the dependencies between statements. The developer does not need to worry about ordering and parallelizing execution to minimize latency.

YQL+ provides for slow queries and transient failures by permitting the developer to specify timeouts and fallback queries to ensure there are always results without needing to write a lot of backend code for each case.

YQL+ is for defining queries and flows but does not need to duplicate the functionality of an imperative programming language because it provides for extensions in the familiar languages supported by Yahoo!'s serving platforms.

# MUST, MUST NOT, SHOULD, SHOULD NOT, MAY, MAY NOT

This document uses the terms MUST, MUST NOT, SHOULD, SHOULD NOT, MAY and MAY NOT as defined in [RFC 2119](http://www.ietf.org/rfc/rfc2119.txt).

# Definitions

   * QL - "Query Language" - this language (YQL+)
   * record - representation of a single data item with ordered, named fields
   * sequence - sequence of records. output of a query or pipeline.
   * source - A producer of records. 

# Syntax Notation

This document uses Extended Backus-Naur Form (EBNF) to describe syntax.

    identifier = letter , { letter | digit | " " } ;
    terminal = "'" character { character } "'";
             | "'" character "'" '..' "'" character "'" 
    lhs = identifier;
    rhs = identifier
        | terminal
        | "["  rhs  "]"
        | "{"  rhs  "}"
        | "("  rhs  ")"
        | rhs  "|"  rhs
        | rhs rhs 
        | "~" rhs ;

    rule = lhs ":" rhs ";" ;
    grammar = { rule } ;

    |   alternation
    ()  grouping
    []  option (0 or 1 times)
    {}  repetition (0 to n times)

## Encoding

Programs are encoded using UTF-8 when transmitted and are Unicode documents. Comments and string literals may contain arbitrary characters subject to the grammar. Keywords and identifier names are limited to the ASCII subset of Unicode.

## Comments

Single line comments are prefixed with `//` (C++/Java style) and continue to the end of the line. They may begin on a line with other tokens.

Multi-line comments begin with `/*` and end with `*/` as in Java.

Parsing MAY skip comments when parsing to an Abstract Syntax Tree (AST). Engines SHOULD preserve comments if the program will be serialized back to a source code representation. Engines which parse, modify, and re-serialize programs SHOULD preserve comment locality in the original program when possible.

## Whitespace

Whitespace separates tokens. Any sequence of whitespace is equivalent to a single whitespace. Whitespace includes space, tab, and newline. 

## Identifiers and names

    ID  : ('a'..'z'|'A'..'Z'|'_') { 'a'..'z'|'A'..'Z'|'0'..'9'|'_'|':' };

Identifiers start with a letter or underscore and contain ASCII letters, numbers, underscore, or colon.

    ident : ID | 'select' | 'table' | 'delete' | 'into' | 'values' 
          | 'limit' | 'offset' | 'where' | 'order' | 'by' | 'desc' 
          | 'asc' | 'merge' | 'left' | 'join' | 'on' | 'output' 
          | 'program' | 'view' | 'create' | 'require' | 'update' 
          | 'delete' | 'values' | 'insert' | 'set';

    namespaced_name :   ident  { '.' ident }

Some parts of the grammar permit several keywords to be used as identifiers when the meaning will be unambiguous. Those points are identified by the use of `ident` rather than `ID`.

A `namespaced_name` is a dot-separated name used to identify things within namespaces. For example, references to sources within modules.

## Literals

    INT : '0'..'9' { '0'..'9' }

    FLOAT
        :   DIGIT { DIGIT } '.' { '0'..'9' } [ EXPONENT ]
        |   '.' DIGIT { DIGIT } [ EXPONENT ]
        |   DIGIT { DIGIT } [ EXPONENT ]
        ;
 
    EXPONENT : ('e'|'E') [ '+'|'-' ] DIGIT { DIGIT } ;

    DIGIT : '0'..'9'

    LETTER  : 'a'..'z'
            | 'A'..'Z'

    DQ : '"'
    SQ : "'"

    STRING  :  DQ { ESC_SEQ | ~('\\'| DQ| '\n' | '\r') } DQ
            |  SQ { ESC_SEQ | ~('\\' | SQ | '\n' | '\r') } SQ
            ;

    HEX_DIGIT : ('0'..'9'|'a'..'f') ;


    ESC_SEQ
        :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\'|'/')
        |   UNICODE_ESC
        ;

    UNICODE_ESC
        :   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT

    integer 
       : INT [ 'b' | 'L' ] // b indicates byte, L indicates int64
       ;

    boolean
       : 'true' 
       | 'false'
       ;

    double
       : FLOAT
       ;

    scalar
       : boolean
       | integer 
       | STRING
       | double
       ;

    map
        : '{' [ name_value { ',' name_value) } ] '}'

    name_value
        : (ident | STRING) ':' expression

    array : '[' [ expression { ',' expression } ] ']'

Literal strings are specified using either single or double quotes (as in JavaScript) and support familiar escape sequences for specifying values. Strings are Unicode values.

Literal maps and arrays are specified using JavaScript syntax. Trailing commas in map expressions (a comma after the last item but before the closing brace) are not permitted. When a map is used to represent a record the order of fields used in the map is preserved.

## Keywords

QL keywords are case insensitive. Keywords in statements are generally written in uppercase.

# Types

- byte, int16, int32, int64 -- 1, 2, 4, and 8 byte signed integers
- double -- 8-byte IEEE floating point value
- string -- Unicode string (UTF-8 when serialized)
- boolean -- true/false
- timestamp -- at least millisecond precision time since epoch
- array -- array of values; arrays may contain any type
- map -- string to value map
- record -- ordered collection of name, value
- expr -- expression fragment
- NULL -- nil value; the absence of a value, any type of value may also be NULL

# Programs
 

    program : [ program_arguments ';' ] { import_statement ';'} { ddl_statement ';' } { statement ';' } EOF

    import_statement : 'import' (ID | STRING) 'as' ID
                     | 'from' ( ID | STRING ) 'import' ID { ',' ID }
                     | 'import' ID
                     ;

    program_arguments  : 'program' '(' program_argument { ',' program_argument } ')'

    program_argument : '@' ident typename [ '=' expression ]

    typename
        : 'byte' | 'int16' | 'int32' | 'int64'   // integer types
        | 'double'                               // floating-point types
        | 'string' | 'boolean' | 'timestamp'     // other scalars
        | 'array' | 'map' | 'record'             // composite types
        | 'expr'                                 // expression fragment
        ;

    ddl_statement : view_statement
                  ;

    statement : output_statement
              | create_temp_statement
              | next_statement
              ;

    create_temp_statement : 'create' ('temp' | 'temporary') 'table' ident 'AS' '(' source_statement ')' ;

    output_statement : source_statement [ paged ] output_spec ;

    output_spec : 'output' ['count'] 'as' ident ;

    paged : 'paged' fixed_or_variable_reference ;

    next_statement : 'next' STRING output_spec;

## program arguments

Programs may have named, typed arguments. If a default value is supplied then an argument is optional. If a default value is not supplied then the caller must supply a value for the argument. The default for an argument must match its declared type.

## import statements

Modules may be identified via an ID or a STRING. The meaning and format of module identifiers is up to the implementation. Certain names are reserved by the language specification for defining standardized modules of functionality.

There are three forms of import statement:
- import (ID | STRING) as ID permits importing STRING or ID identified modules into a specified namespace.
- from (ID | STRING) import ID, ... permits importing specified identifiers from a module into the default namespace
- import ID permits importing a module identified with an ID in using the module name as the namespace

Import statements define dependencies of the program. Modules loaded via import statements may load sources, functions, or pipeline components. 

Import statements may not override names in a namespace. Importing different modules into the same namespace is a compile error. Using the `from ... import ...` form of import statement to import a name more than once is a compile error. 

Import statements MUST be idempotent and MUST NOT result in side effects other than loading and making a module available for use in a program.

Implementations MUST support a mechanism for loading CQL programs with DDL statements as modules to permit reuse of views. CQL programs imported in this way MUST NOT have any output or temporary table query statements.

## DDL statements

### CREATE VIEW statement

    view_statement : 'create' 'view' ID 'as' source_statement

A VIEW is a source created based on a source_statement and given a name. It permits reuse of complex queries. Using a view is equivalent to substituting the source statement in place. For example:

    CREATE VIEW view_name AS SELECT name, title FROM source_name;
    SELECT name FROM view_name;

    // equivalent to:
    SELECT name FROM ( SELECT name, title FROM source_name ) view_name;

The query planner is given the complete, view-expanded statement for planning.

## temporary table and output statements

A program is a series of statements. The output of a program is one or more named sequences generated by output_statements.

output_statements must name their output to ensure callers can recognize the different output resultsets regardless of the order they are returned.

'create temporary table' statements can create a local memory-resident table for later queries to use. They persist only as long as a program is executing.

Output statements implicitly define a temporary table with the output name.

A temporary table or output name may only be used once per program.

QL 1.0 programs have no side effects as they contain only query statements. Effects on metrics systems and log files are not counted as "side effects" for this assertion.

The QL planner MAY execute statements in parallel and in order based on dependencies between statements rather than as defined in the program. The QL planner SHOULD provide a means for describing the execution plan for a given program and configuration. Statements which are not required to satisfy a query to be output MAY not be executed. For example, in this program:

    CREATE TEMPORARY TABLE foo AS (SELECT * FROM source1);
    SELECT name, title FROM source2 OUTPUT title_list;

The first statement may not be executed as it produces no output and the resulting temporary table is not used anywhere.

## `paged`

The `paged` keyword on an output statement requests paginated results while specifying the size of the page. When this is used, the return data includes a opaque continuation value to request further results for a resultset. The next page for a given paged resultset may be requested via the `next` statement. 

Pagination requires the support of the sources that compute a resultset. If pagination for a given resultset is not supported then an error MUST be returned which indicates which source lacks support.

# Source and Query statements

    source_statement : query_statement { '|' pipeline_step }

    query_statement :  merge_statement
                    |  select_statement
                    |  insert_statement
                    |  update_statement
                    |  delete_statement

    merge_statement : merge_component 'merge' merge_component { 'merge' merge_component }

    merge_component : select_statement
                    | '(' source_statement ')'

    pipeline_step : namespaced_name [ arguments ]

A query statement produces a sequence.

## MERGE statement

MERGE statements merge two or more select_statement results into a single sequence. To impose order or other constraints on the output of a MERGE use the sequence as the input to a query.

Results of different select_statements may be interleaved or they may be concatenated. The order of items within a resultset MUST be preserved.

## SELECT statement

    select_statement :  'select' select_field_spec select_source [where] [orderby] [limit] [offset] [timeout] [fallback]

    select_field_spec
            :   field_def { ',' field_def }
            |   '*'

    select_source :   'from' ( multi_source | source_spec { join_spec source_spec 'on' join_expression } )

    multi_source : 'sources' ('*' | source_list)

    source_list : namespaced_name { ',' namespaced_name }

    join_spec : 'left' 'join'
              | [ 'inner' ] 'join'

    source_spec :  data_source [ alias_def ]
                |  '(' source_statement ')' alias_def

        
    alias_def : ['as'] ID

    join_expression : namespaced_name '=' namespaced_name

    data_source
            :   namespaced_name  [ arguments ]
            |   variable_reference

    orderby :  'order by' orderby_fields 
        
    orderby_fields
           :   orderby_field (COMMA orderby_field)* 
        
    orderby_field
           :   expression ('desc' | 'asc')?

    limit  : 'limit' fixed_or_variable_reference 

    offset : 'offset' fixed_or_variable_reference

    where  : 'where' expression

    field_def
           :   expression alias_def?
           |   structured_join alias_def

    fallback : 'fallback' select_statement

    timeout :  'timeout' fixed_or_variable_reference

SELECT statements are the interface to sources. They permit specifying projections (via select_field_spec), joins, timeouts, sorting (via orderby), slices (via offset/limit)

### projections

Projections are specified similarly to SQL -- a list of expressions with specified or generated names. Output field names are generated based on the expression if they are not specified. A project may be omitted using '*' which will return the entire record.

### source_list

For some systems it may make sense to support specifying a query against multiple or all sources. Search backends frequently work this way -- matching queries against many backends by executing the querying and examining which backends returned relevant results.

This syntax provides for a way to specify multiple or 'all' sources. This is mutually exclusive with specifying a source with joins.

### Source specifications

Sources may be named using a `namespaced_name` with optionally specified arguments. Source implementations may support behavior modification via arguments or they may require arguments to be invoked.

Sources may also be a variable reference to refer to a previously defined named resultset.

Finally, sources can be an in-line source statement with a specified alias.

### Joins

Joins between two sources may be specified using the JOIN and LEFT JOIN clauses. The "left" side of a join is the source immediately before the JOIN clause and the "right" side of a join is the new source specified in the join clause. A join expression must specify an equality of two field references and must reference (only) a single field from each side of the join. Joins in QL are intended to enable a relatively narrow set of use cases where the left side of a join is usually a relatively small number of records that is the output of a selection process (e.g. querying an index) and the right side of joins usually are against a data source with supplemental data about the items from the left side of the join. 

For example, querying a content index with a subset of fields for each content item and then joining against a complete content serving store indexed by content item ID and further joining against a "social" data store. See [social example](#social_example).

When JOINs are specified all field references in projections, ORDER BY and WHERE clauses MUST include the source alias in order to identify which source a given field is to be read from.

In SQL this is only required when field names are ambiguous, but in QL schemas on sources are optional so unambiguous field references are always required. Aliases are optional when once source is visible.

### `where` - filtering

The WHERE clause specifies a filter for the output resultset as a boolean expression. A WHERE clause may refer to projected fields, any source field, and available functions.

Available filtering options may vary by source. Unsupported operations MUST be identified at program compile time.

### `order by` - specifying output order

The ORDER BY clause specifies an output order as a function of the projected fields, source fields, or available functions. It is specified as one or more clauses which are applied as a compound sort key. The details of ordering and the support for sorting are source implementation details.

### `limit` and `offset`

Use `limit` to limit the number of results from a query. `offset` should be used only for inspection and development. Requesting paginated results should be done with the `paged` keyword.

### `timeout`

`timeout` specifies a timeout in clock milliseconds for this query starting from the start of program execution. If a query has dependencies that must be executed before a query can be started then that time will be deducted from the available time for the source.

### `fallback`

Fallback clauses specify a select_statement to use in the event the left statement fails for any reason (timeout or error). The execution engine SHOULD enforce the requirement that the fallback statement is cachable and is always available. Use of the fallback clause SHOULD NOT incur the full execution time of the fallback clause because the results should be retrieved or cached in parallel with the primary query.

Configurable automatic degrading behavior (backing off given repeated failures of a source) by source SHOULD be provided by the execution engine. 

Configurable rate limiting for remote sources SHOULD be provided by the execution engine.

## <a name="insert_statement">INSERT statement</a>

    insert_statement
        : 'insert' insert_source insert_values [returning]
        ;
        
    insert_source : 'into' write_data_source ;

    write_data_source : namespaced_name ;

    insert_values
        : field_names_spec 'values' field_values_spec { ',' field_values_spec }
        | query_statement
        ;

    field_names_spec : '(' field_def { ',' field_def } ')' ;

    field_values_spec : '(' parameter { ',' parameter } ')' ;

    returning : 'returning' select_field_spec ;
    
The INSERT statement inserts one or more records into the specified program source.

If accessed over HTTP, programs containing INSERT statements are restricted to PUT and POST methods. Any attempt to access such programs via any other HTTP method MUST be denied and result in a 400 response.

### `returning` - projection

The optional RETURNING clause performs projection on the inserted record.

## <a name="update_statement">UPDATE statement</a>

    update_statement
        : 'update' update_source 'set' update_values [where] [returning]
        ;

    update_source : write_data_source ;

    update_values
        : field_names_spec '=' field_values_spec
        | field_def { ',' field_def }
        ;
    
The UPDATE statement updates one or more records in the specified program source.

If accessed over HTTP, programs containing UPDATE statements are restricted to PUT and POST methods. Any attempt to access such programs via any other HTTP method MUST be denied and result in a 400 response.

### `where` - filtering

The optional WHERE clause specifies which record(s) should be updated. If it is missing, all records will be updated.

### `returning` - projection

The optional RETURNING clause performs projection on the updated record(s).

### `output count as`

If only the number of records that were updated is of interest, the OUTPUT COUNT AS clause may be used, which writes to the program output. The name uniqueness constraints for OUTPUT COUNT AS are the same as for OUTPUT AS.

## <a name="delete_statement">DELETE statement</a>

    delete_statement
        : 'delete' delete_source [where] [returning_spec]
        ;

    delete_source : 'from' write_data_source ;
        
The DELETE statement deletes one or more records from the specified program source.

If accessed over HTTP, programs containing DELETE statements are restricted to DELETE methods. Any attempt to access such programs via any other HTTP method MUST be denied and result in a 400 response.

### `where` - filtering

The optional WHERE clause specifies which record(s) should be deleted. If it is missing, all records will be deleted.

### `returning` - projection

The optional RETURNING clause performs projection on the deleted record(s).

### `output count as`

If only the number of records that were deleted is of interest, the OUTPUT COUNT AS clause may be used, which writes to the program output. The name uniqueness constraints for OUTPUT COUNT AS are the same as for OUTPUT AS.

# Expressions

Expressions are used in filtering (WHERE), sorting (ORDER BY), and projection clauses.

    expression : [ annotation ] logical_or_expression

    annotation : '[' map ']'

Annotations may be attached to any expression and are a literal, constant map. These are permit specifying additional, engine-specific options for the interpretation of an expression.

    logical_or_expression 
        : logical_and_expression { 'or' logical_and_expression }
                
    logical_and_expression 
        : equality_expression { 'and' equality_expression }

`and` and `or` boolean operators.

    equality_expression 
        : relational_expression [ ( ['not'] 'in' in_expression)
                                 | ( 'is' ['not'] 'null' )
                                 | (op_equal relational_expression) ]

    op_equal 
            :   ('=' | '!=' | 'like' | 'not like' | 'matches' | 'not matches' | 'contains')

    // select_statement in in_expression must yield records with a single field
    in_expression
        : '(' ( select_statement | constant_scalar_list | variable_reference | namespaced_name ) ')'

    constant_scalar_list
           : scalar { ',' scalar } 

Boolean equality expressions. `=` and `!=` test equality and inequality. `like` and `not like` do pattern matching (see below). `matches` and `not matches` test regular expression matches. `contains` does full text token matching.

`in` and `not in` test for membership in a set. The set may be specified as a constant list, a previously defined variable or reference, or a select_statement. If a variable is used, it must either be an array or it must refer to a resultset with exactly one field. If a SELECT statement is used it must specify a projection with exactly one field. The name of the field does not matter. 


    relational_expression
            : additive_expression { op_rel additive_expression }

    op_rel
           :    ('<' | '>'| '<=' | '>=')

Relational expressions test for inequalities and have higher precedence than equality tests.
        
    additive_expression
            : multiplicativeExpression [ op_add additive_expression ]
        
    op_add:   '+' | '-'

    multiplicativeExpression
            : unary_expression [ op_mult multiplicativeExpression ]

    op_mult  :  '*' |  '/' | '%'

Arithmetic expressions for addition, subtraction, multiplication, division, and modulus. Note that in a WHERE clause the expression must evaluate to a boolean value.

    op_unary : '-' | '!'

    unary_expression
        : dereference_expression
        | op_unary dereference_expression

Arithmetic (`-`) and boolean (`!`) negation.

    dereference_expression
           :     primary_expression
             {
                 ('[' expression ']')   // index expression (arrays, maps)
                 | ('.' ID)             // dereference expression
             }

Dereferencing into structured values, arrays, and maps.

    primary_expression
        : namespaced_name arguments // function call
        | variable_reference
        | namespaced_name           // field reference
        | scalar
        | array
        | map
        | '(' expression ')'
        | '`' expression '`'        // quoted expression fragment

    arguments
            :  '(' [ expression { ',' expression } ] ')'

    variable_reference 
           : AT ident

A quoted expression fragment creates an `expr` value which can then be passed into functions.

## Advanced grouping

Advanced grouping is implemented using pipeline components supplied by the container. It is not part of the 1.0 language specification.

# Reserved words

The following keywords are reserved for future use:

- perform
- execute
- procedure
- graph
- environment

# Variable capabilities

The specific filters supported by a given source are defined by a source.

Support for pagination is defined by the source.

Source discovery tools SHOULD provide a way to identify the types of supported queries for a source.

A program which requests unsupported behavior from a source SHOULD fail at compile time rather than execution time.

# Explain

The tools and development environment for QL MUST provide a way to generate both human and machine readable query plans for a given program including all timeouts, remote requests, dependencies, and the execution dependency graph.

# Trace

The program execution MUST provide a way to generate a trace of an actual program execution. Similar to explain but including actual times and waterfall execution information.

# Differences from SQL

## Vespa extensions

These are extensions to faciliate expressing Vespa use cases:

- Annotations (constant key/value additions to expressions) 
- SOURCES *|source_list as a way of specifying the same query across multiple sources. The implementation determines how this is handled.

## Orchestration

Parallel execution of multiple independent query trees is an extension to SQL. SQL would execute queries in order.

The `var` construct to represent a query resultset is an extension. It is similar but different from CREATE TEMPORARY TABLE in that 


# Examples

## <a name="social_example">Social content join example</a>

Query a serving system for article IDs to show, join against a content store for details about the items, and further join against a "social content" store to get social details. Assume the content store is faster than the social store, and arrange for the content data (necessary to display results) is returned as fast as possible.

    CREATE TEMPORARY TABLE my_articles AS (SELECT id, score FROM content_index);
    SELECT a.id, a.score, c.href, c.title, c.image 
      FROM my_articles a
      JOIN content_store c ON a.id = c.id 
      ORDER BY a.score DESC  
    FALLBACK 
        SELECT id, 0.0 score, href, title, image FROM static_content ORDER BY published_date DESC
    OUTPUT AS articles;

    SELECT a.id, s.like_count, s.share_count, s.friends_like, s.friends_shared
       FROM my_articles a
       JOIN social_store s ON a.id = s.id
    OUTPUT AS article_social;

In this example, the planner SHOULD execute the content index query and then start both the content_store and social_store queries. The client can then start rendering as soon as the content_store query returns and augment the rendered view with the social data when it is available (assuming the content_store query returns first; if the social_store query returned first then the client would be able to render the full view once the content store results returned).

This program also includes a fallback example. static_content is assumed to be a local or reliable data source. If either the dynamic query to content_index or the query to content_store fails then the static content would be. The social query would either not be run (if it was the content_index query that failed) or would return inapplicable results (if it was the content_store query that failed).

## <a name="expr_fragment">Expression fragments</a>

    PROGRAM (@filter expr = `true`);

    SELECT id, title, href 
      FROM content
     WHERE @filter AND type = 'article';

Query the `content` source and a permit the optional augmentation of the filter with a passed-in expression fragment.

## <a name="write_statements">Write statements</a>
            
Insert a movie record:

    PROGRAM (@uuid string, @title string, @category string, @prodDate string, @duration int32, @reviews array<string>);
    INSERT INTO movies (uuid, title, category, prodDate, duration, reviews) VALUES (@uuid, @title, @category, @prod_date, @duration, @reviews) OUTPUT AS inserted_record ;

Insert a movie record and output just its `uuid` field that was auto-generated by the program source:

    PROGRAM (@title string, @category string, @prodDate string, @duration int32, @reviews array<string>);
    INSERT INTO movies (title, category, prodDate, duration, reviews) VALUES (@title, @category, @prodDate, @duration, @reviews) RETURNING uuid OUTPUT AS inserted_record_uuid ;

Update the `category` field of the movie records with the given `uuids`:

    PROGRAM (@category string);
    UPDATE movies SET category = @category WHERE uuid IN ('1234', '5678') OUTPUT AS updated_records ;

Update the `title` and `category` fields of the movie records with the given `uuids`:

    PROGRAM (@title string, @category string);
    UPDATE movies SET title = @title, category = @category  WHERE uuid IN ('1234', '5678') OUTPUT AS updated_records ;

The above UPDATE statement is equivalent to the following:

    UPDATE movies SET (title, category) = (@title, @category) WHERE uuid = @uuid OUTPUT AS updated_records ;

Delete the movie record whose `uuid` field matches the `uuid` program argument:

    PROGRAM (@uuid string);
    DELETE FROM movies WHERE uuid = @uuid OUTPUT AS deleted_record ;

Delete all movie records and return the `uuid` fields of the ones with a duration greater than 4 hours:

    PROGRAM ();
    CREATE TEMPORARY TABLE deleted_records AS (DELETE FROM movies) ;
    SELECT uuid FROM deleted_records WHERE duration > 4 OUTPUT AS deleted_records_with_long_duration ;

Delete all movie records and output the total count:

    PROGRAM ();
    DELETE FROM movies OUTPUT COUNT AS num_deleted_records ;
 

