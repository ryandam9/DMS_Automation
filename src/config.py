replication_instance_arn = 'arn:aws:dms:us-east-2:317460704610:rep:BF25BSHXHC7DQ73MXESH2LVUASATCYADAIS25BY'
source_endpoint_arn = 'arn:aws:dms:us-east-2:317460704610:endpoint:TZGVBMPKWFKIL473B6RINNYJPUTXBVLAV2GSHLQ'
target_endpoint_arn = 'arn:aws:dms:us-east-2:317460704610:endpoint:4I6ADULFGTZXZOBIZBDQYFGITJHR7ST5E3AMYWQ'
# sns_topic_arn = 'arn:aws:sns:us-east-2:317460704610:DMSTASK'
sns_topic_arn = ''

DEFAULT_REGION = "us-east-2"
MAX_TASKS_PER_PAGE = 100
SOURCE_DB_ID = "database-1"
TARGET_DB_ID = "demo"

# This variable controls how many RDS DB log files to be fetched
DB_LOG_FILE_COUNT = 1

csv_files_location = '../config'
json_files_location = '../json_files'

task_arn_file = "../config/task_arn_file.txt"
