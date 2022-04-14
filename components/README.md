# Transformation Components


### [SQL Query (Table Input)](./table-input-sql-query.go) 

   1. Input is a SQL statement.
   2. Output rows to a channel of `map[string]interface{}`, where the map keys are the SQL column names.

  ![Image Stream Lookup](./table-input-sql-query.png?raw=true "Stream Lookup")


### [SQL Query with Arguments (Table Input)](./table-input-sql-query.go)

   1. Input is a SQL statement with bind variables populated by another (prior) SQL statement.
   2. Output rows to a channel of `map[string]interface{}`, where the map keys are the SQL column names.

  ![Image Stream Lookup](./table-input-sql-query-with-args.png?raw=true "Stream Lookup")


### [Stream Lookup - Join Tables A & B (1:n)](./stream-lookup.go)

  1. Input is two SQL row channels that can join by common field(s), where the cardinality of parent-child data
  is `1:n`. Take the child rows and build a `[]map[string]interface{}` before adding it to the parent row map.
  2. The channels must be sorted by the common join field(s) for this to work.  E.g. use SQL `order by` to achieve this.
  3. Output results to a channel of `map[string]interface{}` so further processing can be performed by other steps.
  4. The map keys are the column names and these are case sensitive.

  ![Image Stream Lookup](./stream-lookup.png?raw=true "Stream Lookup")


### [Table Diff / Merge Diff](./merge-diff.go)

  1. Input is two channels containing an ordered stream of records of type `map[string]interface{}`: 
  one with old data, one with new data.
  Use the table input steps above as input.
  2. Output `map[string]interface{}` per row with an added flag field showing whether a record is
  NEW, CHANGED or DELETED or IDENTICAL. 
  This output can feed into the Table Sync or Merge step below. 
  Output of IDENTICAL rows is optional.

  ![Image Merge Diff](./merge-diff.png?raw=true "Merge Diff")


### [Table Sync (Table Output)](./table-output-sync.go)

  1. Input is one channel of records containing both table data fields and
   a flag field from the Table Diff / Merge Diff step above.
  2. Output is database writes for the field changes to a RDBMS table 
  where NEW rows cause INSERTs, 
  CHANGED rows cause UPDATEs 
  and DELETED rows cause DELETEs. 
  IDENTICAL rows are ignored.
  Transaction size is configurable.
  
  ![Image Stream Lookup](./table-output-sync.png?raw=true "Stream Lookup")


### [Table Merge (Table Output)](./table-output-merge.go)

  1. Input is one channel of records containing table data fields. 
  E.g. Data from the Table Input step above.
  2. Output is SQL MERGE statements executed.

  ![Image Stream Lookup](_table-output-merge.png?raw=true "Stream Lookup")

