oracle_queries = {}

oracle_queries['get_table_ddl'] = """
SELECT 
    OWNER
  , TABLE_NAME
  , COLUMN_NAME
  , DATA_TYPE
  , DATA_LENGTH
  , DATA_PRECISION
  , DATA_SCALE
  , NULLABLE
  , COLUMN_ID
FROM
    ALL_TAB_COLS
WHERE
    OWNER = :schema
AND TABLE_NAME = :table_name  
ORDER BY
    COLUMN_ID
"""
