import base64
import json
import os
import sys
import textwrap
from datetime import datetime
from pathlib import Path

import boto3
import openpyxl
from botocore.exceptions import ClientError
from openpyxl.styles.borders import Border, Side
from tabulate import tabulate

from config import SECRET_MANAGER_SECRET_NAME, csv_files_location


def convert_schemas_to_lowercase():
    return {
        "rule-type": "transformation",
        "rule-id": "3",
        "rule-name": "convert-schemas-to-lower",
        "rule-action": "convert-lowercase",
        "rule-target": "schema",
        "object-locator": {"schema-name": "%"},
    }


def convert_tables_to_lowercase():
    return {
        "rule-type": "transformation",
        "rule-id": "4",
        "rule-name": "convert-tables-to-lower",
        "rule-action": "convert-lowercase",
        "rule-target": "table",
        "object-locator": {"schema-name": "%", "table-name": "%"},
    }


def convert_columns_to_lowercase():
    return {
        "rule-type": "transformation",
        "rule-id": "5",
        "rule-name": "convert-columns-to-lowercase",
        "rule-action": "convert-lowercase",
        "rule-target": "column",
        "object-locator": {"schema-name": "%", "table-name": "%", "column-name": "%"},
    }


def get_aws_cli_profile():
    # ----------------------------------------------------------------------------------------------#
    # Look for AWS CLI profiles in ~/.aws/config
    # ----------------------------------------------------------------------------------------------#
    aws_config_file_path = os.path.join(Path.home(), ".aws", "config")
    profiles = []

    if not os.path.exists(aws_config_file_path):
        return profiles

    with open(aws_config_file_path, "r") as aws_cli_profile_file:
        aws_cli_profile_list = aws_cli_profile_file.readlines()

        for line in aws_cli_profile_list:
            if "[" in line:
                aws_cli_profile_name = line.split("[")[1].split("]")[0]
                aws_cli_profile_name = aws_cli_profile_name.replace(
                    "profile", ""
                ).strip()
                profiles.append(aws_cli_profile_name)

    return profiles


def write_to_excel_file(list1, list2):
    """
    Writes the input M x N matrices to an Excel file.

    Assumption is that, both the lists are of the same size (i.e, each have
    M rows, and there are N cells in each row).

    :param list1: M x N matrix
    :param list2: M x N matrix

    :return: None
    """
    no_cells_in_each_row_in_list1 = set([len(row) for row in list1])
    no_cells_in_each_row_in_list2 = set([len(row) for row in list2])

    if (
        len(list1) != len(list2)
        or no_cells_in_each_row_in_list1 != no_cells_in_each_row_in_list2
    ):
        print("Input lists are not of the same size.")

        print(
            tabulate(
                list1[1:],
                headers=list1[0],
                tablefmt="fancy_grid",
            )
        )

        print(
            tabulate(
                list2[1:],
                headers=list2[0],
                tablefmt="fancy_grid",
            )
        )

        return

    wb = openpyxl.Workbook()
    sheet = wb["Sheet"]  # Default sheet name is 'Sheet'
    sheet.title = "structure_comparison"
    sheet.sheet_properties.tabColor = "1072BA"

    thin_border = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin"),
    )

    current_time = (
        datetime.now().strftime("%Y_%m_%d %H:%M").replace(" ", "_").replace(":", "_")
    )

    target_file = (
        "../table_structure_validation/structure_comparison"
        + "_"
        + current_time
        + ".xlsx"
    )

    for i in range(len(list1)):
        for j in range(len(list1[i])):
            sheet.cell(row=i + 1, column=j + 1).value = list1[i][j]
            sheet.cell(row=i + 1, column=j + 1).border = thin_border

        k = len(list1[i]) + 1

        for j in range(len(list2[i])):
            sheet.cell(row=i + 1, column=k + j + 1).value = list2[i][j]
            sheet.cell(row=i + 1, column=k + j + 1).border = thin_border

    wb.save(target_file)

    print(f"-> Data written to excel file: {target_file}")


def print_messages(messages, headers):
    """
    Prints the messages in the input list.

    :param messages: List of Lists
    :param headers: List of strings

    :return: None
    """
    wrapped_messages = []

    for message in messages:
        wrapped_message = "\n".join(
            textwrap.wrap(message[0], width=180, replace_whitespace=False)
        )

        wrapped_messages.append([wrapped_message])

    print(
        tabulate(
            wrapped_messages,
            headers=headers,
            tablefmt="fancy_grid",
        )
    )


def read_secret(profile, region, secret_key):
    secret_name = SECRET_MANAGER_SECRET_NAME

    # Create a Secrets Manager client
    session = boto3.Session(profile_name=profile, region_name=region)

    client = session.client(
        service_name='secretsmanager'
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )

    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            print(e)
            sys.exit(1)
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            print(e)
            sys.exit(1)
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            print(e)
            sys.exit(1)
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            print(e)
            sys.exit(1)
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            msg1 = str(e)
            msg2 = f"Secret not found: {secret_name}"
            print_messages([[msg1], [msg2]], ['Error'])
            sys.exit(1)
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']

            # Convert to a map
            try:
                secret = json.loads(secret)

                if secret_key in secret.keys():
                    return secret[secret_key]
                else:
                    msg1 = f"Secret key [{secret_key}] does not exist!! Did you supply the right name in config.py file?"
                    print_messages([[msg1]], ["Error"])
                    sys.exit(1)
            except Exception as error:
                msg1 = f"Unable to convert secret to JSON: {str(error)}"
                msg2 = f"Secret: {secret}"
                msg3 = "The secrets have to be stored in Key/Value format in Secrets manager!"
                print_messages([[msg1], [msg2], [msg3]], ["Error"])
                sys.exit(1)
        else:
            msg1 = "[SecretString] field is not populated in the response."
            msg2 = get_secret_value_response
            print_messages([[msg1], [msg2]], ["Error"])
            sys.exit(1)


def get_tables_to_validate():
    """
    This function reads the "INCLUDE" files in config folder and returns a list of tables to validate.

    :return: A list of tables to validate. Each table is a map with keys:
        'schema' and 'table'
    """
    tables = []

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

                    tables.append({"schema": schema, "table": table})

    tables.sort(key=lambda x: x["schema"] + x["table"])
    return tables


def get_current_time():
    return (
        datetime.now().strftime("%Y_%m_%d %H:%M").replace(" ", "_").replace(":", "_")
    )
