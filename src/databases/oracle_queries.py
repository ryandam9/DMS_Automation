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

oracle_queries[
    "get_table_ddl_another"
] = """
WITH temp AS (
    <temp_placeholder>
)
SELECT 
    a.owner
  , a.table_name
  , a.column_name
  , a.data_type
  , a.data_length
  , a.data_precision
  , a.data_scale
  , a.nullable
  , a.column_id
FROM
    all_tab_cols a
  , temp  
WHERE
    UPPER(a.owner) = UPPER(temp.owner)
and UPPER(a.table_name) = UPPER(temp.table_name)
ORDER BY
    a.owner
  , a.table_name
  , a.column_id
"""

# Get Primary key
oracle_queries[
    "get_primary_key"
] = """
WITH temp AS (
    <temp_placeholder>
)
SELECT 
     cols.owner
   , cols.table_name
   , cols.column_name
FROM 
     all_constraints cons
   , all_cons_columns cols
   , temp
WHERE 
    UPPER(cons.owner) = UPPER(temp.owner)
AND UPPER(cols.owner) = UPPER(temp.owner)
AND UPPER(cols.table_name) = UPPER(temp.table_name)
AND cons.constraint_type = 'P'
AND cons.constraint_name = cols.constraint_name
AND cons.status = 'ENABLED'
ORDER BY 
    1, 2, 3
"""

oracle_queries['get_tables_in_a_schema'] = """
SELECT
    owner
  , table_name
FROM
    ALL_TABLES
WHERE
    UPPER(OWNER) = UPPER(:schema)
"""
