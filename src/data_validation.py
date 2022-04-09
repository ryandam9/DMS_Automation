import os
import sys
import threading

import numpy as np
import pandas as pd
from sql_formatter.core import format_sql
from sqlalchemy.exc import SQLAlchemyError

from config import DATA_VALIDATION_REC_COUNT, DEBUG_DATA_VALIDATION, PARALLEL_THREADS
from databases.oracle import oracle_execute_query, oracle_table_to_df
from databases.oracle_queries import oracle_queries
from databases.postgres import postgres_table_to_df
from generate_html_reports import generate_data_validation_report
from utils import get_tables_to_validate, print_messages


def data_validation(src_config, tgt_config):
    """
    This is the Driver function that controls all data validation activity.

    For the tables that're migrated, this function compares the data
    between source & target tables.

    src_config: Source DB config. A Dictionary that has all details needed to
                connect to the source database. It has the following keys:

                - db_engine
                - host
                - port
                - service
                - user
                - password

    tgt_config: Target DB config. A Dictionary that has all details needed to
                connect to the target database.

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
    for file in os.listdir("../logs"):
        os.remove(os.path.join("../logs", file))

    # Step 1: Get the list of tables that are being migrated
    tables = get_tables_to_validate()
    print(f"-> Tables have been identified. Count: {len(tables)}")

    # Step 2: Using DB catalog tables, identify primary key columns for each
    # table from source DB.
    df = fetch_primary_key_column_names(src_config, tables)

    # This dictionary will hold the primary key data for each table
    #  - Key: Table name
    #  - Value: A list of primary key column names
    primary_keys = {}

    for row in df.values.tolist():
        schema, table, pk = row[0], row[1], row[2]

        if table not in primary_keys.keys():
            primary_keys[table] = []

        primary_keys[table].append(pk)

    print(f"-> Primary keys have been identified.")

    # [print(f"{t:>30} : {k}") for t, k in primary_keys.items()]

    no_tables = len(tables)

    # Perform data validation in parallel rather sequentially to get better performance.
    i = 0
    no_threads_per_cycle = PARALLEL_THREADS

    if no_threads_per_cycle > no_tables:
        no_threads_per_cycle = no_tables

    while True:
        threads = []

        for j in range(no_threads_per_cycle):
            table_id = i + j
            schema = tables[table_id]["schema"]
            table = tables[table_id]["table"]

            t = threading.Thread(
                target=data_validation_single_table,
                args=(
                    schema,
                    table,
                    primary_keys[table] if table in primary_keys.keys() else [],
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
                lines = f.readlines()

                if len(lines) == 0:
                    print(f"File {file_full_path} is empty.")
                else:
                    html_rows.append(lines[-1].strip("\n"))

    generate_data_validation_report(html_rows)


def data_validation_single_table(schema, table, primary_key, src_config, tgt_config):
    """
    Performs Data validation for a single table

        - Connects to the source DB & extracts data.
        - Then, connects to the target DB, extracts data.
        - Compares data from both sources.
        - Finally, writes the result to a spreadsheet.
    """
    # Generate summary file
    summary_file = open(f"../logs/{schema}_{table}_data_validation_summary.log", "w")

    no_pk_cols = len(primary_key)

    # At this point, we are not validating tables that don't have
    # primary keys.
    if no_pk_cols == 0:
        msg = f"{schema}~{table}~0~0~~:{schema}.{table} does not have primary keys, skipping data validation!"
        write_log_entry(summary_file, msg, True)
        return

    # Read source table
    try:
        source_df = read_data_from_source_db(src_config, schema, table)
    except SQLAlchemyError as e:
        error = str(e.__dict__["orig"])
        msg = f"{schema}~{table}~0~0~~{error}"
        write_log_entry(summary_file, msg, True)
        return

    if len(source_df) == 0:
        msg = f"{schema}~{table}~0~0~~{schema}.{table} does not have data in source DB, skipping data validation!"
        write_log_entry(summary_file, msg, True)
        return

    # Step 4: Capture the primary key data.
    try:
        primary_key = [x.lower() for x in primary_key]
        pk_values = source_df[primary_key].values.tolist()
    except Exception as err:
        err_str = str(err)
        msg = f"{schema}~{table}~0~0~~{err_str}"
        write_log_entry(summary_file, msg, True)
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

    query += ") "
    query += f"SELECT a.* FROM {schema}.{table} a, temp WHERE "

    for i in range(no_pk_cols):
        if i > 0:
            query += " AND "

        query += f"a.{primary_key[i]} = temp.{primary_key[i]}"

    msg = f"{schema}~{table}~0~0~~{format_sql(query)}"
    write_log_entry(summary_file, msg, False)

    # Step 6: Get the data from target table using the primary key data.
    try:
        target_df = read_data_from_target_db(tgt_config, query)
    except SQLAlchemyError as e:
        error = str(e.__dict__["orig"])
        msg = f"{schema}~{table}~0~0~~{error}"
        write_log_entry(summary_file, msg, True)
        return

    if len(target_df) == 0:
        msg = f"{schema}~{table}~0~0~~No data found in target DB, skipping data validation!"
        write_log_entry(summary_file, msg, True)
        return

    # Step 7: Compare the data between source & target tables.
    # We're going to combine the Source & Target Dataframes now.
    # column names cannot be same. So, we'll rename the columns in the
    # target DB.
    try:
        columns = target_df.columns
        new_columns = []
        [new_columns.append("tgt_" + col) for col in columns]
        target_df.columns = new_columns

        # Now, we'll combine the Source & Target Dataframes.
        target_table_pk = []
        [target_table_pk.append("tgt_" + col.lower()) for col in primary_key]

        combined_df = pd.merge(
            source_df,
            target_df,
            how="left",
            left_on=primary_key,
            right_on=target_table_pk,
        )

        combined_df = combined_df.replace({np.nan: None})

        # Now that, we have Source & Target DB data in a single Dataframe
        # Compare the records and check if they're same or not.
        formatted_df = compare_data(
            combined_df, schema, table, columns, primary_key, summary_file
        )

        excel_file_location = f"../data_validation/{schema}_{table}.xlsx"

        formatted_df.to_excel(
            excel_file_location, sheet_name=f"{schema}_{table}", index=False
        )
    except Exception as err:
        error = str(err)
        msg = f"{schema}~{table}~0~0~~{error}"
        write_log_entry(summary_file, msg, True)
        return


def compare_data(df, schema, table, columns, primary_key, summary_file):
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

    try:
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
    except Exception as err:
        error = str(err)
        msg = f"{schema}~{table}~0~0~~{error}"
        write_log_entry(summary_file, msg, True)
        return

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

    # Table, no. of records validated, no. of records having differences, Columns having differences
    line1 = f"{schema}~{table}~{len(df)}~{no_recs_having_differences}~{','.join(list(columns_having_differences))}~VALIDATION COMPLETED"
    write_log_entry(summary_file, line1, True)

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


def fetch_primary_key_column_names(src_config, tables):
    """ """
    db_engine = src_config["db_engine"]

    if db_engine == "Oracle":
        primary_keys_query = oracle_queries["get_primary_key"]
        inline_view = generate_db_specific_inline_view(db_engine, tables)
        query = primary_keys_query.replace("<temp_placeholder>", inline_view)

        # Execute the query
        df = oracle_table_to_df(src_config, query, None)

        return df

    print("Primary key fetching not supported for this database engine.")
    sys.exit(1)


def read_data_from_source_db(src_config, schema, table):
    """ """
    db_engine = src_config["db_engine"]

    if db_engine == "Oracle":
        try:
            query = f"SELECT * FROM {schema}.{table} WHERE ROWNUM < {DATA_VALIDATION_REC_COUNT}"
            source_df = oracle_table_to_df(src_config, query, None)

            return source_df
        except SQLAlchemyError as e:
            raise e


def read_data_from_target_db(tgt_config, query):
    """ """
    db_engine = tgt_config["db_engine"]

    if db_engine == "PostgreSQL":
        try:
            target_df = postgres_table_to_df(tgt_config, query, None)
            return target_df
        except SQLAlchemyError as e:
            raise e


def write_log_entry(file, entry, close_file):
    """ """
    file.write(entry + "\n")

    if close_file:
        file.close()
