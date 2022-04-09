import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from databases.oracle import oracle_table_to_df
from databases.oracle_queries import oracle_queries
from databases.postgres import postgres_table_to_df
from databases.postgres_queries import postgres_queries
from generate_html_reports import generate_table_metadata_compare_report
from utils import print_messages


def oracle_get_table_metadata(tables, db_config):
    """

    :param tables: A list of dictionaries containing the table name and schema name.
    """
    # Generate a query to be executed on Oracle
    sub_query = ""

    for index, table in enumerate(tables):
        if index > 0:
            sub_query += " UNION "

        sub_query += f"SELECT '{table['schema']}' AS owner, '{table['table']}' AS table_name FROM DUAL "

    query = oracle_queries["get_table_ddl_another"]
    query = query.replace("<temp_placeholder>", sub_query)

    # Execute the query
    try:
        df = oracle_table_to_df(db_config, query, None)
        return df
    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        print_messages([[error]], ["Error connecting to Source DB"])
        sys.exit(1)


def postgres_get_table_metadata(tables, db_config):
    """

    :param tables: A list of dictionaries containing the table name and schema name.
    """
    # Generate a query to be executed on Oracle
    sub_query = ""

    for index, table in enumerate(tables):
        if index > 0:
            sub_query += " UNION "

        sub_query += f"SELECT '{table['schema']}' AS table_schema, '{table['table']}' AS table_name "

    query = postgres_queries["get_table_ddl_another"]
    query = query.replace("<temp_placeholder>", sub_query)

    # Execute the query
    try:
        df = postgres_table_to_df(db_config, query, None)
        return df
    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        print_messages([[error]], ["Error connecting to Target DB"])
        sys.exit(1)


def validate_table_structure(tables, src_db_config, tgt_db_config):
    """ """
    if src_db_config["db_engine"] == "Oracle":
        src_df = oracle_get_table_metadata(tables, src_db_config)
    elif src_db_config["db_engine"] == "PostgreSQL":
        src_df = postgres_get_table_metadata(tables, tgt_db_config)

    print("-> Metadata gathered from Source DB")
    print(f"-> Size: {len(src_df)}")

    if tgt_db_config["db_engine"] == "Oracle":
        tgt_df = oracle_get_table_metadata(tables, src_db_config)
    elif tgt_db_config["db_engine"] == "PostgreSQL":
        tgt_df = postgres_get_table_metadata(tables, tgt_db_config)

    # Change Dataframe column names, so that, we don't have to depend on
    # database specific names.
    src_df_cols = [
        "src_schema",
        "src_table",
        "src_column",
        "src_data_type",
        "src_max_length",
        "src_precision",
        "src_scale",
        "src_is_nullable",
        "src_col_position",
    ]

    src_df.columns = src_df_cols

    # Change Dataframe column names, so that, we don't have to depend on
    # database specific names.
    tgt_df_cols = [
        "tgt_schema",
        "tgt_table",
        "tgt_column",
        "tgt_data_type",
        "tgt_max_length",
        "tgt_precision",
        "tgt_scale",
        "tgt_is_nullable",
        "tgt_col_position",
    ]

    tgt_df.columns = tgt_df_cols

    print("-> Metadata gathered from Target DB")
    print(f"-> Size: {len(tgt_df)}")

    # Join the two dataframes on the schema, table & column names.
    src_join_keys = ["src_schema", "src_table", "src_column"]
    tgt_join_keys = ["tgt_schema", "tgt_table", "tgt_column"]

    combined_df = pd.merge(
        src_df, tgt_df, how="left", left_on=src_join_keys, right_on=tgt_join_keys
    )

    combined_df = combined_df.replace({np.nan: None})

    # Write to a CSV file
    current_time = (
        datetime.now().strftime("%Y_%m_%d %H:%M").replace(" ", "_").replace(":", "_")
    )

    csv_file = (
        "../table_structure_validation/structure_comparison"
        + "_"
        + current_time
        + ".xlsx"
    )

    combined_df.to_excel(
        csv_file, sheet_name="Structure comparison", index=False)

    print(f"-> CSV report generated: {os.path.abspath(csv_file)}")

    # Generate a HTML report
    generate_table_metadata_compare_report(
        combined_df, src_db_config, tgt_db_config)
