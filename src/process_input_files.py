import collections
import json
import os

from config import csv_files_location, json_files_location
from utils import (convert_columns_to_lowercase, convert_schemas_to_lowercase,
                   convert_tables_to_lowercase)

# ------------------------------------------------------------------------------------------------#
# Create named tuples to hold Table, and filter attributes                                        #
# ------------------------------------------------------------------------------------------------#
# Each table should be associated with a schema. Table will have filters applied to it.
Table = collections.namedtuple("Table", "schema, table, filters, auto_partitioned")

# Each filter is composed of three attributes
#  1. Column name
#  2. Operator name (eq, ste, gte, between)
#  3. Filter value (Note - In case of between, two values are needed. They should be separated with "~")
Filter = collections.namedtuple("Filter", "column, operator, value")

# holds tables that have filter conditions
filter_tables = []

# This is a map with key as schema name.this map holds all the tables under a
# schema.
non_filter_tables = {}


def delete_json_files():
    """
    Deletes json files in "json_files" directory.
    """
    for file in os.listdir(json_files_location):
        os.remove(os.path.join(json_files_location, file))
        print(f"File {file} deleted")


def process_input_files():
    print("-" * 100)

    # Identify the CSV files and process them
    for file in os.listdir(csv_files_location):
        file_full_path = os.path.join(csv_files_location, file)

        if file.startswith("include"):
            process_csv_file(file_full_path, "include")
        elif file.startswith("exclude"):
            process_csv_file(file_full_path, "exclude")

    print("All CSV files have been read.")
    print("-" * 100)

    delete_json_files()

    # Generate JSON Files
    create_tasks_for_filter_tables(filter_tables)

    create_tasks_for_no_filter_tables(non_filter_tables)

    print("JSON files have been generated.")
    print("-" * 100)


def process_csv_file(csv_file, action):
    """
    Reads Input "csv" file(s) and segregates tables into (a) tables that have no filter conditions
    (b) tables that have filter conditions.

    It is assumed that tables with filter conditions are huge. As a result, they should have a dedicated DMS
    task created for them. On the other hand, all tables with no filter conditions under a schema should be handled by
    a single DMS task.
    """
    print(f"Processing file: {csv_file}")
    counter = 0

    with open(csv_file, "r") as in_file:
        for line in in_file:
            counter += 1
            decision = ""

            # Following cases fall into this category.
            #  1. Table with no filter conditions
            #  2. All tables in a schema (E.g., HR,%)
            if len(line.split(",")) == 2:
                schema, table = line.split(",")

                # Remove any special chars
                schema = schema.strip()
                table = table.strip("\n").strip()
                table_obj = Table(
                    schema=schema, table=table, filters=[], auto_partitioned=False
                )

                add_to_non_filter_tables(schema, table_obj)
                decsion = "No Filter conditions"

            # These are the tables with filter conditions.
            if len(line.split(",")) > 3:
                cols = line.split(",")
                schema, table = (
                    cols[0],
                    cols[1],
                )  # First two positions have schema, table respectively.

                schema = schema.strip()
                table = table.strip()

                # Identify filter count. Each filter is a set of 3 columns.
                filter_count = int(len(cols) - 2) / 3
                index = 2

                # holds all filter conditions of a given table.
                filters = list()

                for i in range(0, int(filter_count)):
                    column, operator, value = (
                        cols[index],
                        cols[index + 1],
                        cols[index + 2],
                    )

                    column = column.strip()
                    operator = operator.strip()
                    value = value.strip("\n").strip()

                    # Store the filter details in a named table.
                    filter_condition = Filter(
                        column=column, operator=operator, value=value
                    )
                    filters.append(filter_condition)

                    # Add the index to process next filter condition.
                    index += 3

                # Create a Table object.
                table_obj = Table(
                    schema=schema, table=table, filters=filters, auto_partitioned=False
                )

                filter_tables.append(table_obj)
                decsion = "Filter conditions"

            # If an entry has exactly 3 columns, at this point, it is assumed that the 3rd column
            # specifies "partition-auto" specified. This condition needs to be revisited in case more
            # scenarios need to be handled in future.
            if len(line.split(",")) == 3:
                schema, table, auto_partition_flag = line.split(",")
                table_obj = Table(
                    schema=schema, table=table, filters=[], auto_partitioned=True
                )
                add_to_non_filter_tables(schema, table_obj)

                decsion = "No Filter conditions & Auto Partition"

            print(f"{counter} - {line.strip()} - {decsion}")


def add_to_non_filter_tables(schema, obj):
    """
    Adds a table object to the dict.
    """

    # Create an entry for the schema.
    if schema not in non_filter_tables.keys():
        non_filter_tables[schema] = []

    # Append the table object.
    non_filter_tables[schema].append(obj)


def create_tasks_for_no_filter_tables(tables):
    """
    Creates JSON files for tables that DO NOT have any filter conditions. Following tables fall under this
    case.
        1. Tables with no filter conditions (E.g., HR.EMPLOYEE)
        2. Schema with all tables (E.g., HR,%)
        3. Tables with "partitions-auto" specified (E.g., HR,EMPLOYEE,partitions-auto)

    Our intention is to create a single DMS task to process all tables that belong a single schema.
    As a result, a single JSON file will be created for a single schema.
    """
    schemas = tables.keys()
    index = 5

    for schema in schemas:
        data = dict()
        data["rules"] = []
        file_name = schema.lower() + ".all_tables.json"

        for table in tables[schema]:
            print("Processing table: {}.{}".format(table.schema, table.table))
            index += 1

            entry = {
                "rule-type": "selection",
                "rule-id": index,
                "rule-name": index,
                "object-locator": {
                    "schema-name": table.schema,
                    "table-name": table.table,
                },
                "rule-action": "include",
            }

            # If the table is specified to have have "partitions-auto" in the input csv file
            # create this entry.
            if table.auto_partitioned:
                entry["parallel-load"] = {"type": "partitions-auto"}

            data["rules"].append(entry)

        # Add a Transformation
        data["rules"].append(convert_schemas_to_lowercase())
        data["rules"].append(convert_tables_to_lowercase())
        data["rules"].append(convert_columns_to_lowercase())

        with open(os.path.join(json_files_location, file_name), "w") as fp:
            json.dump(data, fp)


def create_tasks_for_filter_tables(tables):
    """
    Creates JSON files for tables that DO HAVE any filter conditions.

    One JSON file will be created for each table/condition.
    """
    index = 5

    for table in tables:
        data = dict()
        data["rules"] = []

        print("Processing table: {}.{}".format(table.schema, table.table))
        index += 1

        entry = {
            "rule-type": "selection",
            "rule-id": index,
            "rule-name": index,
            "object-locator": {"schema-name": table.schema, "table-name": table.table},
            "rule-action": "include",
        }

        part_of_filename = ""

        # Generate filter conditions
        filter_conditions = []
        for fil in table.filters:
            column = fil.column
            operator = fil.operator.lower()
            value = fil.value

            condition = {}

            if operator == "between":
                lower, upper = value.split("~")
                upper = upper.strip("\n").strip()

                condition = {
                    "filter-operator": operator,
                    "start-value": lower,
                    "end-value": upper,
                }

                if len(part_of_filename) == 0:
                    part_of_filename = lower + "-" + upper
            else:
                condition = {"filter-operator": operator, "value": value}

                if len(part_of_filename) == 0:
                    part_of_filename = value

            filter_condition = {
                "filter-type": "source",
                "column-name": column,
                "filter-conditions": [condition],
            }

            filter_conditions.append(filter_condition)

        entry["filters"] = filter_conditions
        data["rules"].append(entry)

        # Add a Transformation
        data["rules"].append(convert_schemas_to_lowercase())
        data["rules"].append(convert_tables_to_lowercase())
        data["rules"].append(convert_columns_to_lowercase())

        file_name = "f{table.schema}-{table.table}-{part_of_filename}.json"
        file_name = file_name.replace("_", "-").lower()

        with open(os.path.join(json_files_location, file_name), "w") as fp:
            json.dump(data, fp)
