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

# This variable controls how many RDS DB log files to be fetched
DB_LOG_FILE_COUNT = 1

# How many records to be validated between source & target tables
DATA_VALIDATION_REC_COUNT = 1000

# How many data validation threads can run at the same time?
PARALLEL_THREADS = 5

# When true, the data validation comparison will be logged.
DEBUG_DATA_VALIDATION = True

# Show generated queries or not
show_generated_queries = False

MAX_TASKS_PER_PAGE = 100

csv_files_location = "../config"
json_files_location = "../json_files"
task_arn_file = "../config/task_arn_file.txt"

# Update this variable to point to the Oracle Instant Client.
oracle_instance_client_path = os.path.join(
    str(Path.home()), "desktop", "instant_client"
)

# r"c:\Users\pavanzdb\desktop\instant_client"
