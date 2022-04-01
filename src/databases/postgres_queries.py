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
