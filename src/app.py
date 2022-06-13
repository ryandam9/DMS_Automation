import argparse
import os
import sys

from config import DEFAULT_REGION
from dms import (create_dms_tasks, create_iam_role_for_dms_cloudwatch_logs,
                 delete_all_dms_tasks, delete_dms_tasks, describe_db_log_files,
                 describe_endpoints, describe_table_statistics,
                 fetch_cloudwatch_logs_for_a_task, list_dms_tasks,
                 run_dms_tasks, test_db_connection)
from process_input_files import process_input_files
from utils import get_aws_cli_profile, print_messages

# --------------------------------------------------------------------------------------------------#
# Main section                                                                                      #
# --------------------------------------------------------------------------------------------------#
parser = argparse.ArgumentParser()
parser.add_argument("--profile", help="AWS CLI Profile to be used", type=str)
parser.add_argument("--region", help="Region", type=str)

actions = [
    "[1] generate_json_files",
    "[2] create_dms_tasks",
    "[3] list_dms_tasks",
    "[4] delete_dms_tasks",
    "[5] run_dms_tasks",
    "[6] test_db_connection_from_replication_instance",
    "[7] describe_table_statistics",
    "[8] create_iam_role_for_dms_cloudwatch_logs",
    "[9] fetch_cloudwatch_logs_for_a_task",
    "[10] describe_endpoints",
    "[11] describe_db_log_files",
    "[12] delete_all_dms_tasks",
]

parser.add_argument(
    "--action",
    help="Specify the action to be performed " + ", ".join(actions),
    metavar="",
)

parser.add_argument("--task_arn", help="Specify the task arn", type=str)

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
# Create following directories
# --------------------------------------------------------------------------------------------------#
if not os.path.exists("../logs"):
    os.mkdir("../logs")

if not os.path.exists("../json_files"):
    os.mkdir("../json_files")

# --------------------------------------------------------------------------------------------------#
# Process the Input CSV Files & generate JSON Configurations                                        #
# --------------------------------------------------------------------------------------------------#
if args.action == "generate_json_files" or args.action == "1":
    process_input_files()

# --------------------------------------------------------------------------------------------------#
# Create DMS tasks                                                                                  #
# --------------------------------------------------------------------------------------------------#
if args.action == "create_dms_tasks" or args.action == "2":
    create_dms_tasks(args.profile, args.region)

# --------------------------------------------------------------------------------------------------#
# List DMS tasks                                                                                    #
# --------------------------------------------------------------------------------------------------#
if args.action == "list_dms_tasks" or args.action == "3":
    list_dms_tasks(args.profile, args.region, display_result=True)

# --------------------------------------------------------------------------------------------------#
# Delete DMS tasks                                                                                  #
# --------------------------------------------------------------------------------------------------#
if args.action == "delete_dms_tasks" or args.action == "4":
    delete_dms_tasks(args.profile, args.region)

# --------------------------------------------------------------------------------------------------#
# Start DMS tasks                                                                                   #
# --------------------------------------------------------------------------------------------------#
if args.action == "run_dms_tasks" or args.action == "5":
    run_dms_tasks(args.profile, args.region)

# --------------------------------------------------------------------------------------------------#
# Test DB Connection from Replication Instance.                                                     #
# --------------------------------------------------------------------------------------------------#
if args.action == "test_db_connection_from_replication_instance" or args.action == "6":
    test_db_connection(args.profile, args.region)

# --------------------------------------------------------------------------------------------------#
# Describe table statistics                                                                         #
# --------------------------------------------------------------------------------------------------#
if args.action == "describe_table_statistics" or args.action == "7":
    describe_table_statistics(args.profile, args.region)

# --------------------------------------------------------------------------------------------------#
# Create IAM Role required for DMS Service to create CloudWatch logs.                               #
# --------------------------------------------------------------------------------------------------#
if args.action == "create_iam_role_for_dms_cloudwatch_logs" or args.action == "8":
    create_iam_role_for_dms_cloudwatch_logs(args.profile, args.region)

# --------------------------------------------------------------------------------------------------#
# Fetch CloudWatch logs for a DMS task.                                                             #
# --------------------------------------------------------------------------------------------------#
if args.action == "fetch_cloudwatch_logs_for_a_task" or args.action == "9":
    if args.task_arn is None:
        msg1 = "Please specify a task arn"
        msg2 = "Usage: python app.py --action fetch_cloudwatch_logs_for_a_task --task_arn <task arn>"
        print_messages([[msg1], [msg2]], ["Error"])
    else:
        fetch_cloudwatch_logs_for_a_task(
            args.profile, args.region, args.task_arn)

# --------------------------------------------------------------------------------------------------#
# Describe DMS End points                                                                           #
# --------------------------------------------------------------------------------------------------#
if args.action == "describe_endpoints" or args.action == "10":
    describe_endpoints(args.profile, args.region, print_result=True)

# --------------------------------------------------------------------------------------------------#
# Get log files from a database                                                                     #
# --------------------------------------------------------------------------------------------------#
if args.action == "describe_db_log_files" or args.action == "11":
    describe_db_log_files(args.profile, args.region)

# --------------------------------------------------------------------------------------------------#
# Delete all DMS Tasks                                                                              #
# --------------------------------------------------------------------------------------------------#
if args.action == "delete_all_dms_tasks" or args.action == "12":
    delete_all_dms_tasks(args.profile, args.region)
