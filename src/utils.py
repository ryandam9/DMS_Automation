import os
from pathlib import Path


def convert_schemas_to_lowercase():
    return {
        "rule-type": "transformation",
        "rule-id": "3",
        "rule-name": "convert-schemas-to-lower",
        "rule-action": "convert-lowercase",
        "rule-target": "schema",
        "object-locator": {
            "schema-name": "%"
        }
    }


def convert_tables_to_lowercase():
    return {
        "rule-type": "transformation",
        "rule-id": "4",
        "rule-name": "convert-tables-to-lower",
        "rule-action": "convert-lowercase",
        "rule-target": "table",
        "object-locator": {
            "schema-name": "%",
            "table-name": "%"
        }
    }


def convert_columns_to_lowercase():
    return {
        "rule-type": "transformation",
        "rule-id": "5",
        "rule-name": "convert-columns-to-lowercase",
        "rule-action": "convert-lowercase",
        "rule-target": "column",
        "object-locator": {
            "schema-name": "%",
            "table-name": "%",
            "column-name": "%"
        }
    }


def get_aws_cli_profile():
    #----------------------------------------------------------------------------------------------#
    # Look for AWS CLI profiles in ~/.aws/config
    #----------------------------------------------------------------------------------------------#
    aws_config_file_path = os.path.join(Path.home(), '.aws', 'config')
    profiles = []

    if not os.path.exists(aws_config_file_path):
        return profiles

    with open(aws_config_file_path, 'r') as aws_cli_profile_file:
        aws_cli_profile_list = aws_cli_profile_file.readlines()

        for line in aws_cli_profile_list:
            if "[" in line:
                aws_cli_profile_name = line.split('[')[1].split(']')[0]
                aws_cli_profile_name = aws_cli_profile_name.replace("profile", "").strip()
                profiles.append(aws_cli_profile_name)

    return profiles
