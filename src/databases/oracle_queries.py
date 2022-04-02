oracle_queries = {}

oracle_queries[
    "get_table_ddl"
] = """
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

# Get Primary key
oracle_queries[
    "get_primary_key"
] = """
SELECT 
     cols.column_name
FROM 
     all_constraints cons
   , all_cons_columns cols
WHERE 
    cons.owner = :schema
AND cons.owner = cols.owner
AND cols.table_name = :table_name
AND cons.constraint_type = 'P'
AND cons.constraint_name = cols.constraint_name
AND cons.status = 'ENABLED'
ORDER BY 
    cols.position
"""
