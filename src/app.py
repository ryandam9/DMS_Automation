import argparse
import sys
from pathlib import Path

from config import DEFAULT_REGION
from dms import (create_dms_tasks, create_iam_role_for_dms_cloudwatch_logs,
                 delete_dms_tasks, describe_db_log_files, describe_endpoints,
                 describe_table_statistics, fetch_cloudwatch_logs_for_a_task,
                 list_dms_tasks, start_dms_tasks, test_db_connection)
from process_input_files import process_input_files
from utils import get_aws_cli_profile

# ---------------------------------------------------------------------------------------------------#
# Main section
# ---------------------------------------------------------------------------------------------------#
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
]

parser.add_argument(
    "--action", help="Specify the action to be performed", type=str, choices=actions
)

parser.add_argument("--task_arn", help="Specify the task arn", type=str)

args = parser.parse_args()

# See if any CLI profiles are already configured
profiles = get_aws_cli_profile()

icon = "->"


## PROFILE
if args.profile:
    print(f"{icon} Profile specified: {args.profile}")

    if args.profile not in profiles:
        print(f"{icon} Profile {args.profile} not found in ~/.aws/config")
        sys.exit(1)
    elif args.profile in profiles:
        print(f"{icon} Profile {args.profile} found in ~/.aws/config")
else:
    if "default" in profiles:
        print(f"{icon} Default profile found. Using it.")
        args.profile = "default"
    else:
        print(
            f"{icon} No 'default' profile found in ~/.aws/config. Please specify the profile to be used."
        )
        sys.exit(1)

## REGION
if args.region:
    print(f"{icon} Region specified: {args.region}")
else:
    args.region = DEFAULT_REGION
    print(
        f"{icon} No region specified. Using the default region specified in the config file: {args.region}"
    )

## ACTION
if args.action is None:
    print(f"{icon} No action specified. Please specify the action to be performed.")

    parser.print_help(sys.stderr)
    sys.exit(1)

if args.action == "generate_json_files":
    ## Process the Input CSV Files & generate JSON Configurations
    process_input_files()

if args.action == "create_dms_tasks":
    create_dms_tasks(args.profile, args.region)

if args.action == "list_dms_tasks":
    list_dms_tasks(args.profile, args.region)

if args.action == "start_dms_tasks":
    start_dms_tasks(args.profile, args.region)

if args.action == "delete_dms_tasks":
    delete_dms_tasks(args.profile, args.region)

if args.action == "test_db_connection_from_replication_instance":
    test_db_connection(args.profile, args.region)

if args.action == "describe_table_statistics":
    describe_table_statistics(args.profile, args.region)

if args.action == "create_iam_role_for_dms_cloudwatch_logs":
    create_iam_role_for_dms_cloudwatch_logs(args.profile, args.region)

if args.action == "fetch_cloudwatch_logs_for_a_task":
    if args.task_arn is None:
        print("** Please specify the task arn **")
        print("... --action fetch_cloudwatch_logs_for_a_task --task_arn XXX")
    else:    
        fetch_cloudwatch_logs_for_a_task(args.profile, args.region, args.task_arn)

if args.action == "describe_endpoints":
    describe_endpoints(args.profile, args.region)

if args.action == "describe_db_log_files":
    describe_db_log_files(args.profile, args.region)