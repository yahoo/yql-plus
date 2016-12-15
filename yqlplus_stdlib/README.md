# YQL+ standard UDFs

## string

   import string;

Scalar functions for string manipulation.

<table>
  <thead>
    <tr>
      <td>function</td>
      <td>description</td>
    </tr>
  </thead>
  <tr>
    <td>substr(@text string, @start int, @length int) : string</td>
    <td>Take a substring of @text</td>
  </tr>
  <tr>
    <td>split(@text string, @delim string) : array</td>
    <td>Split string by delim. Multiple copies of the delim string are handled as if they have empty values between them.</td>
  </tr>
  
</table>

## timestamp

   IMPORT timestamp;

Scalar functions for parsing and formatting timestamps.

All of these functions always parse and format as UTC. Only the specific form of ISO8601 date time string is supported.

<table>
  <thead>
    <tr>
      <td>function</td>
      <td>description</td>
    </tr>
  </thead>
  <tr>
    <td>iso8601_parse(@datetime string) : timestamp</td>
    <td>Turn an ISO8601 datetime of the form YYYY-MM-DDThh:mm:ss[.ss]Z into a timestamp.</td>
  </tr>
  <tr>
    <td>iso8601_string(@ts timestamp) : string</td>
    <td>Format an ISO8601 datetime string of the form YYYY-MM-DDThh:mm:ss[.ss]Z from a timestamp.</td>
  </tr>

</table>

## sequence


   IMPORT sequence;

Functions for manipulating and producing sequences.


### sequence.one

Pipeline element to select the first result of a query and return it as a scalar value. Produces an error if the input sequence does not have at least one element.

    IMPORT sequence;
    SELECT * FROM source | sequence.one OUTPUT AS result;

### sequence.unique

`sequence.unique(@key expr)` 

Using the expression fragment @key consume rows and emit the first row with each key. For example:

    SELECT id, cluster_id, title FROM article_index ORDER BY score DESC | sequence.unique(`row.cluster_id`)

would return the first article (by descending score) with each cluster id.

### sequence.extract

`sequence.unique(@value expr)` 

Extract an expression from a sequence and emit a sequence of that value. 

    SELECT id, cluster_id, title FROM article_index ORDER BY score DESC | sequence.extract(`row.id`)

would produce an array of row.id values.