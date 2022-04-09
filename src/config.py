import os
from pathlib import Path

replication_instance_arn = (
    "arn:aws:dms:us-east-2:317460704610:rep:BF25BSHXHC7DQ73MXESH2LVUASATCYADAIS25BY"
)
source_endpoint_arn = "arn:aws:dms:us-east-2:317460704610:endpoint:TZGVBMPKWFKIL473B6RINNYJPUTXBVLAV2GSHLQ"
target_endpoint_arn = "arn:aws:dms:us-east-2:317460704610:endpoint:4I6ADULFGTZXZOBIZBDQYFGITJHR7ST5E3AMYWQ"
# sns_topic_arn = 'arn:aws:sns:us-east-2:317460704610:DMSTASK'
sns_topic_arn = ""

DEFAULT_REGION = "us-east-2"

# These are referenced when fetching log files from the DBs.
SOURCE_DB_ID = "database-1"
TARGET_DB_ID = "demo"

# Secret manager Value
# Store the secret in the secret manager as a Key/Value pair.
SOURCE_DB_SECRET_KEY = "oracle_1"
TARGET_DB_SECRET_KEY = "postgres_1"

# !!! WARNING !!!
# Storing passwords in code is NOT a good practice.
# Use Secrets Manager instead.
# But, if you still want to keep passwords in the code, use
# the following variables.
# If these two attributes are empty, this tool uses try to
# get passwords from Secrets Manager.
SOURCE_DB_PWD = ""
TARGET_DB_PWD = ""

# This variable controls how many RDS DB log files to be fetched
DB_LOG_FILE_COUNT = 1

# How many records to be validated between source & target tables
DATA_VALIDATION_REC_COUNT = 1000

# How many data validation threads can run at the same time?
PARALLEL_THREADS = 50

# When true, the data validation comparison will be logged.
DEBUG_DATA_VALIDATION = False

# When set to true, interaction with DB will be logged.
SQL_ALCHEMY_ECHO_MODE = False

# Enable this when it is failing to connect to the Database.
# It shows the connecting string including the password.
SHOW_CONNECTION_STRING = True

# Show generated queries or not
show_generated_queries = False

MAX_TASKS_PER_PAGE = 100

# Update this variable to point to the Oracle Instant Client.
oracle_instance_client_path = r"C:\Users\ravis\Desktop\ravi\instantclient_21_3"

#-------------------------------------------------------------------------------------------------#
# DO NOT CHANGE THE FOLLOWING LINES
#-------------------------------------------------------------------------------------------------#
csv_files_location = "../config"
json_files_location = "../json_files"
task_arn_file = "../config/task_arn_file.txt"
