grammar yqlplus;

options {
  superClass = ParserBase;
  language = Java;
}

@header {
  import java.util.Stack;
  import com.yahoo.yqlplus.language.internal.parser.*;
}

@parser::members {
	protected static class expression_scope {
        boolean in_select;
    }
    protected Stack<expression_scope> expression_stack = new Stack();    
}

// tokens for command syntax
  CREATE : 'create';
  SELECT : 'select';
  INSERT : 'insert';
  UPDATE : 'update';
  EVALUATE : 'evaluate';
  SET : 'set';
  VIEW : 'view';
  TABLE  : 'table';
  DELETE : 'delete';
  INTO   : 'into';
  VALUES : 'values';
  IMPORT : 'import';
  NEXT : 'next';
  PAGED : 'paged';
  FALLBACK : 'fallback';

  LIMIT : 'limit';
  OFFSET : 'offset';
  WHERE : 'where';
  ORDERBY : 'order by';
  DESC : 'desc';
  ASC :;
  FROM : 'from';
  SOURCES : 'sources';
  AS : 'as';
  MERGE : 'merge';
  LEFT : 'left';
  JOIN : 'join';
  
  ON : 'on';
  COMMA : ',';
  OUTPUT : 'output';
  COUNT : 'count';
  RETURNING : 'returning';
  APPLY : 'apply';
  CAST : 'cast';

  BEGIN : 'begin';
  END : 'end';

  // type-related
  TYPE_BYTE : 'byte';
  TYPE_INT16 : 'int16';
  TYPE_INT32 : 'int32';
  TYPE_INT64 : 'int64';
  TYPE_STRING : 'string';
  TYPE_DOUBLE : 'double';
  TYPE_TIMESTAMP : 'timestamp';
  TYPE_BOOLEAN : 'boolean';
  TYPE_ARRAY : 'array';
  TYPE_MAP : 'map';

  // token literals
  TRUE : 'true';
  FALSE : 'false';

  // brackets and other tokens in literals

  LPAREN : '(';
  RPAREN : ')';
  LBRACKET : '[';
  RBRACKET : ']';
  LBRACE : '{';
  RBRACE : '}';
  COLON : ':';
  PIPE : '|';

  // operators
  AND : 'and';
  OR : 'or';
  NOT_IN : 'not in';
  IN : 'in';
  QUERY_ARRAY :;

  LT : '<';
  GT : '>';
  LTEQ : '<=';
  GTEQ : '>=';
  NEQ : '!=';
  STAR : '*';
  EQ : '=';
  LIKE : 'like';
  CONTAINS : 'contains';
  NOTLIKE : 'not like';
  MATCHES : 'matches';
  NOTMATCHES : 'not matches';

  // effectively unary operators
  IS_NULL : 'is null';
  IS_NOT_NULL : 'is not null';

  // dereference
  DOT : '.';
  AT : '@';

  // quotes
  SQ : '\'';
  DQ : '"';

  // statement delimiter
  SEMI : ';';

  PROGRAM : 'program';
  TIMEOUT : 'timeout';

/*------------------------------------------------------------------
 * LEXER RULES
 *------------------------------------------------------------------*/

ID  :	('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_'|':')*
    ;

LONG_INT :	'-'?'0'..'9'+ ('L'|'l')
    ;

INT :	'-'?'0'..'9'+
    ;

FLOAT
    :   ('-')?('0'..'9')+ '.' ('0'..'9')* EXPONENT?
    |   ('-')?'.' ('0'..'9')+ EXPONENT?
    |   ('-')?('0'..'9')+ EXPONENT
    ;

fragment
EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;


fragment
DIGIT : '0'..'9'
    ;

fragment
LETTER  : 'a'..'z'
    	| 'A'..'Z'
    	;

STRING  :  '"' ( ESC_SEQ | ~('\\'| '"') )* '"'
        |  '\'' ( ESC_SEQ | ~('\\' | '\'') )* '\''
	;

/////////////////////////////
fragment
HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;
	

fragment
ESC_SEQ
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\'|'/')
    |   UNICODE_ESC
    ;

fragment
UNICODE_ESC
    :   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;

WS  :   ( ' '
        | '\t'
        | '\r'
        | '\n'
  	) -> channel(HIDDEN)
    ;

COMMENT
    :  ( ('//') ~('\n'|'\r')* '\r'? '\n'? 
    | '/*' .*? '*/'
    )
    -> channel(HIDDEN)
    ;

TEMP_TABLE
	: ('temp' | 'temporary') WS+ TABLE
	;
	
/*------------------------------------LPAREN! select_statement RPAREN!------------------------------
 * PARSER RULES
 *------------------------------------------------------------------*/

ident
   : keyword_as_ident //{addChild(new TerminalNodeImpl(keyword_as_ident.getText()));}
   		//{return ID<IDNode>[$keyword_as_ident.text];}
   | ID
   ;

keyword_as_ident
   : SELECT | TABLE | DELETE | INTO | VALUES | LIMIT | OFFSET | WHERE | 'order' | 'by' | DESC | MERGE | LEFT | JOIN
   | ON | OUTPUT | COUNT | BEGIN | END | APPLY | TYPE_BYTE | TYPE_INT16 | TYPE_INT32 | TYPE_INT64 | TYPE_BOOLEAN | TYPE_TIMESTAMP | TYPE_DOUBLE | TYPE_STRING | TYPE_ARRAY | TYPE_MAP
   | VIEW | CREATE | EVALUATE | IMPORT | PROGRAM | NEXT | PAGED | SOURCES | SET | MATCHES | CAST
   ;

program : params? (import_statement SEMI)* (ddl SEMI)* (statement SEMI)* EOF
	;

params
     : PROGRAM LPAREN program_arglist? RPAREN SEMI
     ;

import_statement
      : IMPORT moduleName AS moduleId
      | IMPORT moduleId               
      | FROM moduleName IMPORT import_list
      ;

import_list
      : moduleId (',' moduleId)*
      ;

moduleId
    :  ID
    ;
    
moduleName
	: literalString
	| namespaced_name
	;

ddl
      : view
      ;

view  : CREATE VIEW ID AS source_statement
      ;

program_arglist
      : procedure_argument (',' procedure_argument)*
      ;

procedure_argument
      :
      	array_eq_argument
      | AT (ident typename ('=' expression[false])? )
      ;

array_eq_argument
      : AT (ident TYPE_ARRAY LT typename GTEQ (expression[false])? )
      ;

statement
      : output_statement
	  | selectvar_statement
	  | next_statement
	  ;

output_statement
      : source_statement paged_clause? output_spec?
      ;

paged_clause
      : PAGED fixed_or_parameter
      ;

next_statement
	  : NEXT literalString OUTPUT AS ident
	  ;

source_statement
      : query_statement (PIPE pipeline_step)*
      ;

pipeline_step
      : namespaced_name arguments[false]?
      ;

selectvar_statement
      : CREATE TEMP_TABLE ident AS LPAREN source_statement RPAREN
      ;

typename
      : TYPE_BYTE | TYPE_INT16 | TYPE_INT32 | TYPE_INT64 | TYPE_STRING | TYPE_BOOLEAN | TYPE_TIMESTAMP
      | arrayType | mapType | TYPE_DOUBLE
      ;

arrayType
      : TYPE_ARRAY LT typename GT
      ;

mapType
      : TYPE_MAP LT typename GT
      ;

output_spec
      : (OUTPUT AS ident)
      | (OUTPUT COUNT AS ident)
      ;


query_statement
    : merge_statement
    | select_statement
    | execute_statement
    | insert_statement
    | delete_statement
    | update_statement
    ;

// This does not use the UNION / UNION ALL from SQL because the semantics are different than SQL UNION
//   - no set operation is implied (no DISTINCT)
merge_statement
	:	merge_component (MERGE merge_component)+
	;

merge_component
	: select_statement
	| LPAREN source_statement RPAREN
	;

select_statement
	:	SELECT select_field_spec select_source? where? orderby? limit? offset? timeout? fallback?
    ;

execute_statement
    : EVALUATE expression[false]
    ;

select_field_spec
	:	project_spec
	| 	STAR
	;
	
project_spec
	: field_def (COMMA  field_def)*
	;

fallback
    : FALLBACK select_statement
    ;

timeout
	:  TIMEOUT fixed_or_parameter
	;
	
select_source
    :   select_source_all
    |   select_source_multi
	|	select_source_join
	;

select_source_all
	:   FROM SOURCES STAR
	;

select_source_multi
 	: 	FROM SOURCES source_list
 	;
 	
select_source_join
	:	FROM source_spec join_expr*
	;

source_list
    : namespaced_name (COMMA namespaced_name )*
    ;

join_expr
    : (join_spec source_spec ON joinExpression)
    ;

join_spec
	: LEFT JOIN
	| 'inner'? JOIN
	;

source_spec
	:	( data_source (alias_def { ($data_source.ctx).addChild($alias_def.ctx); })? )
	;

alias_def
	:	(AS? ID)
	;

data_source
	:	call_source
	|	LPAREN source_statement RPAREN
	|   sequence_source
	;
	
call_source
	: namespaced_name arguments[true]?
	;
	
sequence_source
	: AT ident
	;

namespaced_name
    :   (ident (DOT ident)* (DOT STAR)?)
    ;

orderby
	:	ORDERBY orderby_fields
	;
	
orderby_fields
	:	orderby_field (COMMA orderby_field)*
	;	
	
orderby_field
	:   expression[true] DESC
	|	expression[true] ('asc')?
	;	

limit
	:	LIMIT expression[true]
	;

offset
    : OFFSET expression[true]
	;

where
	: WHERE expression[true]
	;

field_def
	: expression[true] alias_def?
	;

mapExpression
    : LBRACE propertyNameAndValue? (COMMA propertyNameAndValue)* RBRACE
    ;

constantMapExpression
    : LBRACE constantPropertyNameAndValue? (COMMA constantPropertyNameAndValue)* RBRACE
    ;

arguments[boolean in_select]
	:  LPAREN RPAREN                                                    
	|  LPAREN (argument[$in_select] (COMMA argument[$in_select])*) RPAREN
	;

argument[boolean in_select]
    : expression[$in_select]
    ;

// -------- join expressions ------------

// Limit expression syntax for joins to AND and field equality

joinExpression
    : joinClause (AND joinClause)*
	;

joinClause
    : joinDereferencedExpression EQ joinDereferencedExpression
    ;

joinDereferencedExpression
	:	 namespaced_name
	;

// --------- expressions ------------

expression [boolean select] 
@init {
	expression_stack.push(new expression_scope());
    expression_stack.peek().in_select = select;
}
@after {
	expression_stack.pop();	
}
	: annotateExpression
	| logicalORExpression
	| nullOperator
	;
	
nullOperator
	: 'null'
	;

annotateExpression
	: annotation logicalORExpression
	;
annotation
    : LBRACKET constantMapExpression RBRACKET
    ;

logicalORExpression
	: logicalANDExpression (OR logicalANDExpression)+
	| logicalANDExpression
	;

logicalANDExpression
	: equalityExpression (AND equalityExpression)*
	;

equalityExpression //changed for parsing literal tests
	: relationalExpression (  ((IN | NOT_IN) inNotInTarget)
	                         | (IS_NULL | IS_NOT_NULL)
	                         | (equalityOp relationalExpression) )
	 | relationalExpression
	;

inNotInTarget
    : {expression_stack.peek().in_select}? LPAREN select_statement RPAREN
    | literal_list
    ;

equalityOp 
	:	(EQ | NEQ | LIKE | NOTLIKE | MATCHES | NOTMATCHES | CONTAINS)
	;

relationalExpression
	: additiveExpression (relationalOp additiveExpression)?
	;

relationalOp
	:	(LT | GT | LTEQ | GTEQ)
	;

additiveExpression
	: multiplicativeExpression (additiveOp additiveExpression)?
	;

additiveOp
	:	'+'
	|   '-'
	;	

multiplicativeExpression
	: unaryExpression (multOp multiplicativeExpression)?
	;

multOp	
	:	'*'
	|   '/'
	|   '%'
	;
	
unaryOp
    : '-'
    | '!'
	;	

unaryExpression
    : dereferencedExpression
    | unaryOp dereferencedExpression
	;

dereferencedExpression
@init{
	boolean	in_select = expression_stack.peek().in_select;	
}
	:	 primaryExpression
	     (
	        indexref[in_select]
          | propertyref
	     )*
	;
	
indexref[boolean in_select]
	:	LBRACKET idx=expression[in_select] RBRACKET
	;
propertyref
	: 	DOT nm=ID
	;

operatorCall
@init{
	boolean	in_select = expression_stack.peek().in_select;	
}
    : multOp arguments[in_select]
    | additiveOp arguments[in_select]
    | AND arguments[in_select]
    | OR arguments[in_select]
    ;


primaryExpression
@init {
    boolean in_select = expression_stack.peek().in_select;
}
	: callExpresion[in_select]
	| parameter
	| fieldref
	| scalar_literal
	| arrayLiteral
	| mapExpression
	| LPAREN expression[in_select] RPAREN
	;
	
callExpresion[boolean in_select]
	: namespaced_name arguments[in_select]
	;
	
fieldref
	: namespaced_name
	;
arrayLiteral
@init {
	boolean in_select = expression_stack.peek().in_select;
}
    : LBRACKET expression[in_select]? (COMMA expression[in_select])* RBRACKET
	;

// a parameter is an argument from outside the script/program
parameter 
	: AT ident
	;

propertyNameAndValue
	: propertyName ':' expression[{expression_stack.peek().in_select}] //{return (PROPERTY propertyName expression);}
	;

constantPropertyNameAndValue
	: propertyName ':' constantExpression
	;

propertyName
	: ID 
	| literalString
	;

constantExpression
    : scalar_literal
    | constantMapExpression
    | constantArray
    ;

constantArray
    : LBRACKET i+=constantExpression? (COMMA i+=constantExpression)* RBRACKET
    ;

// TODO: fix INT L and INT b parsing -- should not permit whitespace between them
scalar_literal
	: TRUE    
	| FALSE 
	| STRING
	| LONG_INT
//	| INT 'b' -> INT<LiteralNode>[$INT, Byte.parseByte($INT.text)]
	| INT     
	| FLOAT
	;

literalString
	: STRING
	;

// array literal for IN/NOT_IN using SQL syntax
literal_list
	: LPAREN literal_element (COMMA literal_element)* RPAREN //{return ^(ARRAY_LITERAL literal_element+);}
	;
	
literal_element
    : scalar_literal
    | parameter
    ;

fixed_or_parameter
    : INT
	| parameter
	;


// INSERT

insert_statement
    : INSERT insert_source insert_values returning_spec?
    ;

insert_source
    : INTO write_data_source
    ;

write_data_source
    :	namespaced_name
    ;
    
insert_values
    : field_names_spec VALUES field_values_group_spec (COMMA field_values_group_spec)*
    | query_statement
    ;

field_names_spec
    : LPAREN field_def (COMMA field_def)* RPAREN
    ;

field_values_spec
    : LPAREN expression[true] (COMMA expression[true])* RPAREN
    ;

field_values_group_spec
    : LPAREN expression[true] (COMMA expression[true])* RPAREN
    ;
    
returning_spec
    : RETURNING select_field_spec
    ;

// DELETE

delete_statement
    : DELETE delete_source where? returning_spec?
    ;

delete_source
    : FROM write_data_source
    ;

// UPDATE

update_statement
    : UPDATE update_source SET update_values where? returning_spec?
    ;

update_source
    : write_data_source
    ;

update_values
    : field_names_spec EQ field_values_spec
    | field_def (COMMA field_def)*
    ;

   


