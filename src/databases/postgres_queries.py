postgres_queries = {}

postgres_queries[
    "get_table_ddl"
] = """
SELECT 
    table_schema
  , table_name
  , column_name
  , data_type
  , character_maximum_length
  , numeric_precision
  , numeric_scale
  , is_nullable
  , ordinal_position
FROM
    INFORMATION_SCHEMA.COLUMNS
WHERE 
    UPPER(table_schema) = (%s)
AND UPPER(table_name)   = (%s)
"""

postgres_queries[
    "get_table_ddl_another"
] = """
WITH temp AS (
    <temp_placeholder>
)
SELECT 
    UPPER(a.table_schema) AS table_schema
  , UPPER(a.table_name)  AS table_name
  , UPPER(a.column_name) AS column_name
  , a.data_type
  , a.character_maximum_length
  , a.numeric_precision
  , a.numeric_scale
  , a.is_nullable
  , a.ordinal_position
FROM
    INFORMATION_SCHEMA.COLUMNS a
  , temp  
WHERE 
    UPPER(a.table_schema) = UPPER(temp.table_schema)
AND UPPER(a.table_name)   = UPPER(temp.table_name)
"""
