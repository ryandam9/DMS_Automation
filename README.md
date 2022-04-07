# Automating AWS Data Migration Service (DMS) Tasks

## Overview
Main objective of this script is to automate DMS task creation by reading a CSV file that has got table configurations. The script generates required JSON configuration files that will be fed to DMS service. so that, manual task creation using Web console can be avoided, thus saves time and effort. 

## `Configuration`

### `config.py`
- First configure `config.py` file with ARNs of Source end point, Target endpoint, and Replication Instance.
- The script can optionally publish messages to given SNS topic after tasks have been created, and deleted. If an SNS topic is created, specify the topic's ARN in `sns_topic_arn` attribute. This is optional feature though. If this notification feature is not required, simply leave the empty quotes.  

```python
csv_files_location = '.'
replication_task_settings = 'task_settings.json'
replication_instance_arn = ''
source_endpoint_arn = ''
target_endpoint_arn = ''
sns_topic_arn = ''
```

### `include.csv`
```shell script
ADMIN,EMPLOYEES,HIREDATE,GTE,1995-05-01,JOB_ID,EQ,PU_CLERK
ADMIN,JOBS
ADMIN,LOCATIONS
ADMIN,REGIONS
ADMIN,JOB_HISTORY,START_DATE,BETWEEN,1998-01-01~1999-12-31,JOB_ID,EQ,ST_CLERK
```

****
## Sample Execution

```shell script
cd DMS_Automation/src
python app.py -h
```

```
usage: app.py [-h] [--profile PROFILE] [--region REGION]
              [--action {
                  generate_json_files,
                  create_dms_tasks,
                  list_dms_tasks,
                  delete_dms_tasks,
                  start_dms_tasks,
                  test_db_connection_from_replication_instance,
                  describe_table_statistics,
                  create_iam_role_for_dms_cloudwatch_logs,
                  fetch_cloudwatch_logs_for_a_task,
                  describe_endpoints,
                  describe_db_log_files,
                  validate_table_structures,
                  validate_data
                  }
              ]
              [--task_arn TASK_ARN] [--table_name TABLE_NAME]
```

### Actions
Following are different types of actions the script supports.

Action | Description | Sample|
--- | --- | --- | 
`generate_json_files`|Generate DMS JSON Files|`python app.py --action generate_json_files`|
`create_dms_tasks`|Create JSON Files, and then creates DMS tasks|
`list_dms_tasks`|List DMS Tasks|
`delete_dms_tasks`|Delete DMS tasks|
`run_dms_tasks`|Run DMS tasks|
`test_db_connection_from_replication_instance`|Test DB Connections
`describe_table_statistics`|Describe Table stats
`create_iam_role_for_dms_cloudwatch_logs`|Creates IAM role needed by DMS to log
`fetch_cloudwatch_logs_for_a_task`|Fetch Cloudwatch logs for the task|`python app.py --action fetch_cloudwatch_logs_for_a_task --task_arn <task_run>`
`describe_endpoints`|Describe DMS Endpoints|
`describe_db_log_files`|Fetch DB Log files|
`validate_table_structures`|Compares table structures of the Source & Target DB|1. `python app.py --profile test-env --action validate_table_structures --table_name OT.WAREHOUSES`.<br><hr>2. `python app.py --profile test-env --action validate_table_structures --table_name all`
`validate_data`|Compares data in the Source & Target DB|
****

#### For Quick run

```sh
python app.py --action generate_json_files
python app.py --action create_dms_tasks
python app.py --action list_dms_tasks
python app.py --action delete_dms_tasks
python app.py --action run_dms_tasks
python app.py --action test_db_connection_from_replication_instance
python app.py --action describe_table_statistics
python app.py --action create_iam_role_for_dms_cloudwatch_logs
python app.py --action fetch_cloudwatch_logs_for_a_task --task_arn <task_arn>
python app.py --action describe_endpoints
python app.py --action describe_db_log_files
python app.py --action validate_table_structures --table_name <schema.table>
python app.py --action validate_table_structures --table_name all
python app.py --action validate_data
```

## To perform Data validation for Oracle Databases

### For Windows
- Download Oracle Instance client from `Basic Package`  from `https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html`.

****
## Other Python packages required

```sh
cd DMS_Automation/config
pip install -r requirements.txt
```
****