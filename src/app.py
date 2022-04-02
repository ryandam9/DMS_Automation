import argparse
import sys
from pathlib import Path

from config import DEFAULT_REGION
from dms import (create_dms_tasks, create_iam_role_for_dms_cloudwatch_logs,
                 delete_dms_tasks, describe_db_log_files, describe_endpoints,
                 describe_table_statistics, fetch_cloudwatch_logs_for_a_task,
                 list_dms_tasks, start_dms_tasks, test_db_connection,
                 validate_source_target_data,
                 validate_source_target_structures,
                 validate_source_target_structures_all)
from process_input_files import process_input_files
from utils import get_aws_cli_profile, print_messages

# --------------------------------------------------------------------------------------------------#
# Main section                                                                                      #
# --------------------------------------------------------------------------------------------------#
parser = argparse.ArgumentParser()
parser.add_argument("--profile", help="AWS CLI Profile to be used", type=str)
parser.add_argument("--region", help="Region", type=str)

actions = [
    "generate_json_files",
    "create_dms_tasks",
    "list_dms_tasks",
    "delete_dms_tasks",
    "start_dms_tasks",
    "test_db_connection_from_replication_instance",
    "describe_table_statistics",
    "create_iam_role_for_dms_cloudwatch_logs",
    "fetch_cloudwatch_logs_for_a_task",
    "describe_endpoints",
    "describe_db_log_files",
    "validate_source_target_structures",
    "validate_source_target_data",
]

parser.add_argument(
    "--action", help="Specify the action to be performed", type=str, choices=actions
)

parser.add_argument("--task_arn", help="Specify the task arn", type=str)
parser.add_argument("--table_name", help="Specify schema & table name", type=str)

args = parser.parse_args()

# See if any CLI profiles are already configured
profiles = get_aws_cli_profile()

icon = "->"

# --------------------------------------------------------------------------------------------------#
# Is action passed?                                                                                 #
# --------------------------------------------------------------------------------------------------#
if args.action is None:
    print_messages(
        [[f"{icon} No action specified. Please specify the action to be performed."]],
        ["Error"],
    )
    parser.print_help(sys.stderr)
    sys.exit(1)

# --------------------------------------------------------------------------------------------------#
# Which profile to use?                                                                             #
# --------------------------------------------------------------------------------------------------#
if args.profile:
    print(f"{icon} Profile specified: {args.profile}")

    if args.profile not in profiles:
        print_messages(
            [[f"{icon} Profile {args.profile} not found in ~/.aws/config"]], ["Error"]
        )
        sys.exit(1)
    elif args.profile in profiles:
        print(f"{icon} Profile {args.profile} found in ~/.aws/config")
else:
    if "default" in profiles:
        print(f"{icon} Default profile found. Using it.")
        args.profile = "default"
    else:
        print_messages(
            [
                [
                    f"{icon} No 'default' profile found in ~/.aws/config. Please specify the profile to be used."
                ]
            ],
            ["Error"],
        )
        sys.exit(1)

# --------------------------------------------------------------------------------------------------#
# What's the region are we dealing with?                                                           #
# --------------------------------------------------------------------------------------------------#
if args.region:
    print(f"{icon} Region specified: {args.region}")
else:
    args.region = DEFAULT_REGION
    print(
        f"{icon} No region specified. Using the default region specified in the config file: {args.region}"
    )

# --------------------------------------------------------------------------------------------------#
# Process the Input CSV Files & generate JSON Configurations                                        #
# --------------------------------------------------------------------------------------------------#
if args.action == "generate_json_files":
    process_input_files()

# --------------------------------------------------------------------------------------------------#
# Create DMS tasks                                                                                  #
# --------------------------------------------------------------------------------------------------#
if args.action == "create_dms_tasks":
    create_dms_tasks(args.profile, args.region)

# --------------------------------------------------------------------------------------------------#
# List DMS tasks                                                                                    #
# --------------------------------------------------------------------------------------------------#
if args.action == "list_dms_tasks":
    list_dms_tasks(args.profile, args.region)

# --------------------------------------------------------------------------------------------------#
# Start DMS tasks                                                                                   #
# --------------------------------------------------------------------------------------------------#
if args.action == "start_dms_tasks":
    start_dms_tasks(args.profile, args.region)

# --------------------------------------------------------------------------------------------------#
# Delete DMS tasks                                                                                  #
# --------------------------------------------------------------------------------------------------#
if args.action == "delete_dms_tasks":
    delete_dms_tasks(args.profile, args.region)

# --------------------------------------------------------------------------------------------------#
# Test DB Connection from Replication Instance.                                                     #
# --------------------------------------------------------------------------------------------------#
if args.action == "test_db_connection_from_replication_instance":
    test_db_connection(args.profile, args.region)

# --------------------------------------------------------------------------------------------------#
# Describe table statistics                                                                         #
# --------------------------------------------------------------------------------------------------#
if args.action == "describe_table_statistics":
    describe_table_statistics(args.profile, args.region)

# --------------------------------------------------------------------------------------------------#
# Create IAM Role required for DMS Service to create CloudWatch logs.                               #
# --------------------------------------------------------------------------------------------------#
if args.action == "create_iam_role_for_dms_cloudwatch_logs":
    create_iam_role_for_dms_cloudwatch_logs(args.profile, args.region)

# --------------------------------------------------------------------------------------------------#
# Fetch CloudWatch logs for a DMS task.                                                             #
# --------------------------------------------------------------------------------------------------#
if args.action == "fetch_cloudwatch_logs_for_a_task":
    if args.task_arn is None:
        msg1 = "Please specify a task arn"
        msg2 = "Usage: python app.py --action fetch_cloudwatch_logs_for_a_task --task_arn <task arn>"
        print_messages([[msg1], [msg2]], ["Error"])
    else:
        fetch_cloudwatch_logs_for_a_task(args.profile, args.region, args.task_arn)

# --------------------------------------------------------------------------------------------------#
# Describe DMS End points                                                                           #
# --------------------------------------------------------------------------------------------------#
if args.action == "describe_endpoints":
    describe_endpoints(args.profile, args.region, print_result=True)

# --------------------------------------------------------------------------------------------------#
# Get log files from a database                                                                     #
# --------------------------------------------------------------------------------------------------#
if args.action == "describe_db_log_files":
    describe_db_log_files(args.profile, args.region)

# --------------------------------------------------------------------------------------------------#
# Validate table structures between SOURCE & TARGET DBs                                             #
# --------------------------------------------------------------------------------------------------#
if args.action == "validate_source_target_structures":
    if args.table_name is None:
        msg1 = "Please specify a table name in <SCHEMA.TABLE NAME> format"
        msg2 = "Usage: python app.py --action validate_source_target_structures --table_name <schema>.<table>"
        print_messages([[msg1], [msg2]], ["Error"])
    else:
        if args.table_name == "all":
            validate_source_target_structures_all(args.profile, args.region)
        else:
            validate_source_target_structures(
                args.profile, args.region, args.table_name
            )
# --------------------------------------------------------------------------------------------------#
# Validate data between SOURCE & TARGET DBs                                                         #
# --------------------------------------------------------------------------------------------------#
if args.action == "validate_source_target_data":
    validate_source_target_data(args.profile, args.region)
