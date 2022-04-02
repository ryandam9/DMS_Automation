import os

import pandas as pd
from sql_formatter.core import format_sql

from config import (DATA_VALIDATION_REC_COUNT, csv_files_location,
                    show_generated_queries)
from databases.oracle import oracle_execute_query, oracle_table_to_df
from databases.oracle_queries import oracle_queries
from databases.postgres import postgres_table_to_df
from utils import print_messages


def get_tables_to_validate():
    tables_migrated = []

    # Read the Input CSV Files & gather a list of Schemas and tables
    # that are being migrated.
    for file in os.listdir(csv_files_location):
        file_full_path = os.path.join(csv_files_location, file)

        if file.startswith("include"):
            print(f"-> Reading {file}")

            with open(file_full_path, "r") as f:
                for line in f:
                    schema, table = line.split(",")[0], line.split(",")[1]

                    schema = schema.strip().upper()
                    table = table.strip().upper()

                    tables_migrated.append({"schema": schema, "table": table})

    return tables_migrated


def data_validation(src_config, tgt_config):
    """
    For the tables that're migrated, this function compares the data
    between source & target tables.

    Steps:
    ------
    1. Get the list of tables that are being migrated
    2. Use DB Metadata, identify primary key columns for each table from source DB.
    3. For each table, extract X random rows from source table
    4. Capture the primary key data for these tables.
    5. Prepare a query to fetch the data from target DB.
    6. Get the data from target table using the primary key data.
    7. Compare the data from source & target tables.
    """
    # Step 1: Get the list of tables that are being migrated
    tables_migrated = get_tables_to_validate()

    # Step 2: Use DB Metadata, identify primary key columns for each
    # table from source DB.
    primary_keys_query = oracle_queries["get_primary_key"]

    # This dictionary will hold the primary key data for each table
    primary_keys = {}

    for table in tables_migrated:
        schema = table["schema"]
        table = table["table"]

        result_set = oracle_table_to_df(src_config, primary_keys_query, [schema, table])

        # A primary key can have multiple columns.
        multiple_cols = []

        for row in result_set.values.tolist():
            multiple_cols.append(row[0].lower())

        primary_keys[table] = multiple_cols

    # Step 3: For each table, extract X random rows from source table
    for table in tables_migrated:
        schema = table["schema"]
        table = table["table"]

        query = (
            f"SELECT * FROM {schema}.{table} WHERE ROWNUM < {DATA_VALIDATION_REC_COUNT}"
        )
        source_df = oracle_table_to_df(src_config, query, None)

        # Step 4: Capture the primary key data for these tables.
        pk_list = primary_keys[table]  # [c1, c2, c3]
        sample_pk_values = source_df[pk_list].values.tolist()

        no_pk_cols = len(pk_list)

        # At this point, we are not validating tables that don't have
        # primary keys.
        if no_pk_cols == 0:
            continue

        # Step 5: Prepare a query to fetch the data from target DB.
        query = "WITH temp AS ("

        for index, sample_pk_value in enumerate(sample_pk_values):
            if index > 0:
                query += " UNION "

            # If it is a composite primary key, after zipping, the value looks like
            # this:  [ (c1, v1), (c2, v2), (c3, v3) ]
            sinle_pk_entry = zip(pk_list, sample_pk_value)

            q = "SELECT "
            i = 0
            for col in sinle_pk_entry:
                i += 1
                k, v = col[0], col[1]

                if i > 1:
                    q += ", "

                if type(v) == str:
                    q += f"'{v}' AS {k}"
                else:
                    q += f"{v} AS {k}"

            query += q

        query += ")"
        query += f"SELECT a.* FROM {schema}.{table} a, temp WHERE "

        for i in range(no_pk_cols):
            if i > 0:
                query += " AND "

            query += f"a.{pk_list[i]} = temp.{pk_list[i]}"

        if show_generated_queries:
            print_messages([[f"Target Query for {schema}.{table}"]], ["Query"])
            print(format_sql(query))
            print("\n")

        # Step 6: Get the data from target table using the primary key data.
        target_df = postgres_table_to_df(tgt_config, query, None)

        # Step 7: Compare the data between source & target tables.
        # We're going to combine the Source & Target Dataframes now.
        # column names cannot be same. So, we'll rename the columns in the
        # target DB.
        columns = target_df.columns

        new_columns = []
        [new_columns.append("tgt_" + col) for col in columns]

        target_df.columns = new_columns

        # Now, we'll combine the Source & Target Dataframes.
        target_table_pk = []
        [target_table_pk.append("tgt_" + col.lower()) for col in pk_list]

        combined_df = pd.merge(
            source_df, target_df, how="left", left_on=pk_list, right_on=target_table_pk
        )

        excel_file_location = f"../data_validation/{schema}_{table}.xlsx"

        combined_df.to_excel(
            excel_file_location, sheet_name=f"{schema}_{table}", index=False
        )
