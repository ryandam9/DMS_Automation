import argparse
import os
import platform
import sys
from dis import dis
from pathlib import Path

from config import DEFAULT_REGION, oracle_instance_client_path
from dms import (create_dms_tasks, create_iam_role_for_dms_cloudwatch_logs,
                 delete_all_dms_tasks, delete_dms_tasks, describe_db_log_files,
                 describe_endpoints, describe_table_statistics,
                 fetch_cloudwatch_logs_for_a_task, list_dms_tasks,
                 prepare_include_file_for_a_schema, run_dms_tasks,
                 test_db_connection, validate_source_target_data,
                 validate_table_structure_single_table,
                 validate_table_structures_all)
from process_input_files import process_input_files
from utils import get_aws_cli_profile, print_messages, read_secret

current_platform = platform.system().lower()

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
    "[12] validate_table_structures",
    "[13] validate_data",
    "[14] prepare_include_file_for_a_schema",
    "[15] delete_all_dms_tasks",
]

parser.add_argument(
    "--action",
    help="Specify the action to be performed " + ", ".join(actions),
    metavar="",
)

parser.add_argument("--task_arn", help="Specify the task arn", type=str)
parser.add_argument("--table_name", help="Specify schema & table name", type=str)
parser.add_argument("--schema", help="Specify a schema", type=str)

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

if not os.path.exists("../table_structure_validation"):
    os.mkdir("../table_structure_validation")

if not os.path.exists("../data_validation"):
    os.mkdir("../data_validation")

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
        fetch_cloudwatch_logs_for_a_task(args.profile, args.region, args.task_arn)

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
# Validate table structures between SOURCE & TARGET DBs                                             #
# --------------------------------------------------------------------------------------------------#
if (
    args.action == "validate_table_structures"
    or args.action == "validate_data"
    or args.action == "12"
    or args.action == "13"
):
    # Using cx_Oracle requires Oracle Client libraries to be installed. These provide the necessary
    # network connectivity allowing cx_Oracle to access an Oracle Database instance.
    if "windows" in current_platform:
        current_path = os.environ["PATH"]
        updated_path = oracle_instance_client_path + ";" + current_path
        os.environ["PATH"] = updated_path

        client_path = updated_path.split(";")[0]
        print(f"PATH is set to: {client_path}")

        msg1 = f"Please make sure that Oracle Instant Client is available at {oracle_instance_client_path}."
        msg2 = "Download it from https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html"
        msg3 = "Follow the instructions from https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html#wininstall"
        print_messages([[msg1], [msg2], [msg3]], ["INFO"])

        if not os.path.exists(f"{oracle_instance_client_path}/oci.dll"):
            msg1 = f"Oracle client path {oracle_instance_client_path} seems incorrect!!"
            msg2 = "Does the specified path contain oci.dll, and other files ??"
            msg3 = "Update correct path @ " + os.path.abspath("config.py")
            print_messages([[msg1], [msg2], [msg3]], ["ERROR"])
            sys.exit(1)


if args.action == "validate_table_structures" or args.action == "12":
    if args.table_name is None:
        args.table_name = "all"

    msg1 = "The 'validate_table_structures' action currently supports the following databases:"
    msg2 = "SOURCE DB: Oracle"
    msg3 = "TARGET DB: Postgres"
    print_messages([[msg1], [msg2], [msg3]], ["INFO"])

    if args.table_name == "all":
        validate_table_structures_all(args.profile, args.region)
    else:
        validate_table_structure_single_table(
            args.profile,
            args.region,
            args.table_name,
            True,
        )
# --------------------------------------------------------------------------------------------------#
# Validate data between SOURCE & TARGET DBs                                                         #
# --------------------------------------------------------------------------------------------------#
if args.action == "validate_data" or args.action == "13":
    msg1 = "The 'validate_data' action currently supports the following databases:"
    msg2 = "SOURCE DB: Oracle"
    msg3 = "TARGET DB: Postgres"
    msg4 = "Update PARALLEL_THREADS for faster comparison to 5 or more. Default is 1."
    msg5 = "Update DATA_VALIDATION_REC_COUNT to change the number of records to be compared. Default is 1000."
    print_messages([[msg1], [msg2], [msg3], [msg4], [msg5]], ["INFO"])

    validate_source_target_data(args.profile, args.region)


# --------------------------------------------------------------------------------------------------#
# Prepare Include file for a given schema                                                           #
# --------------------------------------------------------------------------------------------------#
if args.action == "prepare_include_file_for_a_schema" or args.action == "14":
    if args.schema is None:
         msg1 = "Please specify a schema"
         msg2 = "Sample: python app.py --action 14 --schema SYS"
         print_messages([[msg1], [msg2]], ["Error"])
         sys.exit(1)

    prepare_include_file_for_a_schema(args.profile, args.region, args.schema)

# --------------------------------------------------------------------------------------------------#
# Delete all DMS Tasks                                                                              #
# --------------------------------------------------------------------------------------------------#
if args.action == "delete_all_dms_tasks" or args.action == "15":
    delete_all_dms_tasks(args.profile, args.region)
