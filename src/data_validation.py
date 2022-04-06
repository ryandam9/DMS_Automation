import os
import threading

import numpy as np
import pandas as pd
from sql_formatter.core import format_sql

from config import (
    DATA_VALIDATION_REC_COUNT,
    DEBUG_DATA_VALIDATION,
    PARALLEL_THREADS,
    csv_files_location,
    show_generated_queries,
)
from databases.oracle import oracle_execute_query, oracle_table_to_df
from databases.oracle_queries import oracle_queries
from databases.postgres import postgres_table_to_df
from generate_html_reports import generate_data_validation_report
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

    tables_migrated.sort(key=lambda x: x["schema"] + x["table"])
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
    print(f"-> Tables have been identified. Count: {len(tables_migrated)}")

    # Step 2: Use DB Metadata, identify primary key columns for each
    # table from source DB.
    primary_keys_query = oracle_queries["get_primary_key"]
    inline_view = ""

    for index, table in enumerate(tables_migrated):
        if index > 0:
            inline_view += " UNION "

        inline_view += f"SELECT '{table['schema']}' AS owner, '{table['table']}' AS table_name FROM DUAL "

    query = primary_keys_query.replace("<temp_placeholder>", inline_view)

    # Execute the query
    df = oracle_table_to_df(src_config, query, None)

    # This dictionary will hold the primary key data for each table
    primary_keys = {}

    for row in df.values.tolist():
        schema, table, pk = row[0], row[1], row[2]

        if table not in primary_keys.keys():
            primary_keys[table] = []

        primary_keys[table].append(pk)

    print(f"-> Primary keys have been identified.")

    no_tables = len(tables_migrated)

    # Perform data validation in parallel rather sequentially to get better performance.
    i = 0
    no_threads_per_cycle = PARALLEL_THREADS

    if no_threads_per_cycle > no_tables:
        no_threads_per_cycle = no_tables

    while True:
        threads = []

        for j in range(no_threads_per_cycle):
            table_id = i + j
            schema = tables_migrated[table_id]["schema"]
            table = tables_migrated[table_id]["table"]

            t = threading.Thread(
                target=data_validation_single_table,
                args=(
                    schema,
                    table,
                    primary_keys[table],
                    src_config,
                    tgt_config,
                ),
            )
            t.start()

            threads.append(t)

        # Wait for the threads to complete
        for t in threads:
            t.join()

        i += no_threads_per_cycle

        if (no_tables - i) < no_threads_per_cycle:
            no_threads_per_cycle = no_tables - i

        if i >= no_tables:
            print(f"-> All tables [{no_tables}] have been processed.")
            break
        else:
            print(
                f"-> {i} tables have been processed. Remaining tables: {no_tables - i}"
            )

    print(
        f"-> Results have been written to this location: {os.path.abspath('../data_validation')}"
    )

    if DEBUG_DATA_VALIDATION:
        print(
            f"-> Column level differences have been captured @ {os.path.abspath('../logs')}"
        )

    # Generate a HTML report
    html_rows = []

    for file in os.listdir("../logs"):
        file_full_path = os.path.join("../logs", file)

        if "summary" in file:
            with open(file_full_path, "r") as f:
                for line in f:
                    html_rows.append(line)

    generate_data_validation_report(html_rows)


def data_validation_single_table(schema, table, primary_key, src_config, tgt_config):
    """
    Performs Data validation for a single table

        - Connects to the source DB & extracts data.
        - Then, connects to the target DB, extracts data.
        - Compares data from both sources.
        - Finally, writes the result to a spreadsheet.
    """
    query = f"SELECT * FROM {schema}.{table} WHERE ROWNUM < {DATA_VALIDATION_REC_COUNT}"
    source_df = oracle_table_to_df(src_config, query, None)
    primary_key = [x.lower() for x in primary_key]

    # Step 4: Capture the primary key data.
    pk_values = source_df[primary_key].values.tolist()
    no_pk_cols = len(primary_key)

    # At this point, we are not validating tables that don't have
    # primary keys.
    if no_pk_cols == 0:
        print("Table does not have primary keys, skipping data validation!")
        return

    # Step 5: Prepare a query to fetch the data from target DB.
    query = "WITH temp AS ("
    for index, sample_pk_value in enumerate(pk_values):
        if index > 0:
            query += " UNION "

        # If it is a composite primary key, after zipping, the value looks like
        # this:  [ (c1, v1), (c2, v2), (c3, v3) ]
        sinle_pk_entry = zip(primary_key, sample_pk_value)
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

        query += f"a.{primary_key[i]} = temp.{primary_key[i]}"

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
    [target_table_pk.append("tgt_" + col.lower()) for col in primary_key]

    combined_df = pd.merge(
        source_df, target_df, how="left", left_on=primary_key, right_on=target_table_pk
    )

    combined_df = combined_df.replace({np.nan: None})

    # Now that, we have Source & Target DB data in a single Dataframe
    # Compare the records and check if they're same or not.
    formatted_df = compare_data(combined_df, schema, table, columns, primary_key)

    excel_file_location = f"../data_validation/{schema}_{table}.xlsx"

    formatted_df.to_excel(
        excel_file_location, sheet_name=f"{schema}_{table}", index=False
    )


def compare_data(df, schema, table, columns, primary_key):
    """ """
    no_cols_to_compare = len(columns)
    differences = {}
    no_recs_having_differences = 0

    primary_key_indexes = []

    for k in primary_key:
        i = columns.tolist().index(k)
        primary_key_indexes.append(i)

    formatted_recs = []
    columns_having_differences = set()

    # Loop through each row in the Dataframe.
    for index, rec in enumerate(df.values.tolist()):
        no_cols_having_differences = 0

        pk = ""

        for i in primary_key_indexes:
            pk += f"{columns[i]} = {rec[i]}"

        decision = "MATCH"

        # Compare all the columns in the record.
        for i in range(no_cols_to_compare):
            source_cell = rec[i]
            target_cell = rec[i + no_cols_to_compare]

            if (
                (source_cell is None and target_cell is not None)
                or (source_cell is not None and target_cell is None)
                or (
                    source_cell is not None
                    and target_cell is not None
                    and source_cell != target_cell
                )
            ):
                no_cols_having_differences += 1

                if index not in differences.keys():
                    differences[index] = []

                differences[index].append(
                    {
                        "primary_key": pk,
                        "column": columns[i],
                        "source_value": source_cell,
                        "target_value": target_cell,
                    }
                )

                columns_having_differences.add(columns[i])
                decision = "NO MATCH"

        # If there are differences in a record, increment the counter.
        if no_cols_having_differences > 0:
            no_recs_having_differences += 1

        rec.append(decision)
        formatted_recs.append(rec)

    # Create a new dataframe with the result of comparison
    formatted_df_cols = df.columns.tolist()
    formatted_df_cols.append("result")
    formatted_df = pd.DataFrame(formatted_recs, columns=formatted_df_cols)

    print(
        f"-> {schema:>30s} {table:>30s} {str(no_recs_having_differences):>10s} differences found"
    )

    if DEBUG_DATA_VALIDATION:
        log_file = open(f"../logs/{schema}_{table}_data_validation.log", "w")

        for row_no, col_diff_list in differences.items():
            rec = f"Index: {row_no + 1} Primary Key: {col_diff_list[0]['primary_key']}"
            log_file.write(rec + "\n")

            col_diff_list.sort(key=lambda x: x["column"])

            for col_diff in col_diff_list:
                log_file.write(
                    f"{col_diff['column']:>30s}: {str(col_diff['source_value']):>30s} : {str(col_diff['target_value']):>30s}\n"
                )

        log_file.close()

    # Generate summary file
    summary_file = open(f"../logs/{schema}_{table}_data_validation_summary.log", "w")

    # Table, no. of records validated, no. of records having differences, Columns having differences
    line1 = f"{schema}:{table}:{len(df)}:{no_recs_having_differences}:{','.join(list(columns_having_differences))}"
    summary_file.write(line1 + "\n")

    return formatted_df


def generate_db_specific_inline_view(db_engine, tables):
    """

    :param db_engine: Specifies a database engine (Oracle, PostgreSQL, etc.)
    :param tables: A list of tables to be included in the inline view. Each table is a map
    with the following keys: schema, table.

    :return: A string containing the inline view.
    """
    inline_view = ""

    for index, table in enumerate(tables):
        if index > 0:
            inline_view += " UNION "

        if db_engine == "Oracle":
            inline_view += f"SELECT '{table['schema']}' AS owner, '{table['table']}' AS table_name FROM DUAL "
        elif db_engine == "PostgreSQL":
            inline_view += f"SELECT '{table['schema']}' AS owner, '{table['table']}' AS table_name "

        return inline_view
