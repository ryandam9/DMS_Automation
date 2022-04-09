import os
import platform
import sys
import warnings

import cx_Oracle
import pandas as pd
import sqlalchemy
from click import echo
from config import SHOW_CONNECTION_STRING, SQL_ALCHEMY_ECHO_MODE
from sqlalchemy import exc as sa_exc
from sqlalchemy.exc import SQLAlchemyError
from tabulate import tabulate
from utils import print_messages

from .oracle_queries import oracle_queries


def oracle_get_connection(config):
    host = config["host"]
    port = config["port"]
    service = config["service"]
    user = config["user"]
    password = config["password"]

    try:
        dsn = cx_Oracle.makedsn(host, port, service_name=service)
        connection = cx_Oracle.connect(user, password, dsn=dsn)
        return connection
    except Exception as exception:
        msg1 = "Error getting DB connection: {}".format(exception)
        msg2 = f"{host}:{port}/{service}:{user}"

        print_messages([[msg1], [msg2]], ["Error"])
        sys.exit(1)


def oracle_table_to_df(config, query, params):
    host = config["host"]
    port = config["port"]
    service = config["service"]
    user = config["user"]
    password = config["password"]

    if 'windows' in platform.system().lower():
        client_entry = os.environ['PATH'].split(";")[0]
    else:
        client_entry = os.environ['PATH'].split(":")[0]

    if SQL_ALCHEMY_ECHO_MODE:
        print(f"-> Inside oracle_table_to_df(): PATH: {client_entry}")

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=sa_exc.SAWarning)

            engine = sqlalchemy.create_engine(
                f"oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service}",
                arraysize=1000,
                echo=SQL_ALCHEMY_ECHO_MODE
            )

            if params is None:
                df = pd.read_sql(query, engine)
                return df
            else:
                df = pd.read_sql(query, engine, params=params)
                return df
    except SQLAlchemyError as e:
        if SHOW_CONNECTION_STRING:
            msg1 = "VERIFY IF THE CONNECTION STRING IS CORRECT OR NOT !!!"
            msg2 = f"USER: {user}, PASSWORD: {password}, HOST: {host}, PORT: {port}, SERVICE: {service}"
            msg3 = f"Client path: {client_entry}"
            print_messages([[msg1], [msg2], [msg3]], ["Error"])
            sys.exit(1)

        raise e


def oracle_execute_query(config, query, parameters):
    """
    Executes given SQL query.
    Every time a query is executed, a new DB connection is acquired and closed after the query is executed.
    This is not really a good idea, but there is a reason for doing this way.
    Using Connection pooling might be a good idea (However, I am not sure how to use them). Query requests from
    client come at any time and they're are concurrent.
    :return  A List of lists, where each list is a record. First list is the column names.
    """
    connection = oracle_get_connection(config)

    records = []  # Query results
    try:
        cur = connection.cursor()

        if parameters is None:
            cur.execute(query)
        else:
            cur.execute(query, parameters)

        # Get Column names
        field_names = [i[0].upper() for i in cur.description]
        records.append(field_names)

        # Process all the records
        for record in cur:
            rec = []

            # SQL Query result can contain different types of data - Int, Char, Decimal, CLOB, etc.
            # Converting all types to Char so that, when preparing JSON format, there won't be any
            # errors.
            for i in range(len(record)):
                string_value = ""
                try:
                    string_value = str(
                        record[i]) if record[i] is not None else ""
                except Exception as error:
                    print(
                        "Unable to convert value to String; value: {}".format(
                            record[i])
                    )

                rec.append(string_value)
            records.append(rec)
        cur.close()
    except Exception as err:
        # A SQL Query could fail due to any number of reasons. Syntax could be wrong, Database could be down,
        # the User may not have required privileges to execute the query, No Temporary table space, no CPU
        # availability, to name a few. In such cases, Return the Exception.
        print(err)
        records.append("Exception: " + str(err))
    finally:
        connection.close()

    return records


def oracle_table_metadata(config, schema, table, print_result=False):
    query = oracle_queries["get_table_ddl"]
    query_result = oracle_execute_query(
        config, query, parameters=[schema, table])

    if print_result:
        print(
            tabulate(query_result[1:],
                     headers=query_result[0], tablefmt="fancy_grid")
        )

    return query_result


def oracle_tables(config, schema):
    query = oracle_queries["get_tables_in_a_schema"]
    query_result = oracle_table_to_df(config, query, [schema])

    return query_result
