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
                  validate_data,
                  prepare_include_file_for_a_schema,
                  delete_all_dms_tasks
                  }
              ]
              [--task_arn TASK_ARN] [--table_name TABLE_NAME]
```

### Actions
Following are different types of actions the script supports.

Action ID|Action | Description|
--- |--- | --- | 
`1`|`generate_json_files`|Generate DMS JSON Files
`2`|`create_dms_tasks`|Create JSON Files, and then creates DMS tasks
`3`|`list_dms_tasks`|List DMS Tasks
`4`|`delete_dms_tasks`|Delete DMS tasks
`5`|`run_dms_tasks`|Run DMS tasks
`6`|`test_db_connection_from_replication_instance`|Test DB Connections
`7`|`describe_table_statistics`|Describe Table stats
`8`|`create_iam_role_for_dms_cloudwatch_logs`|Creates IAM role needed by DMS to log
`9`|`fetch_cloudwatch_logs_for_a_task`|Fetch Cloudwatch logs for the task
`10`|`describe_endpoints`|Describe DMS Endpoints
`11`|`describe_db_log_files`|Fetch DB Log files
`12`|`validate_table_structures`|Compares table structures of the Source & Target DB
`13`|`validate_data`|Compares data in the Source & Target DB|
`14`|`prepare_include_file_for_a_schema`|Creates a file with all the tables in a schema (Currently suppports Oracle)
`15`|`delete_all_dms_tasks`|Delete all DMS tasks.
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
python app.py --action prepare_include_file_for_a_schema
python app.py --action delete_all_dms_tasks
```
Rather than passing text based actions, the tool supports numefic IDs dedicated to each action.

```sh
python app.py --action 1
python app.py --action 2
python app.py --action 3
python app.py --action 4
python app.py --action 5
python app.py --action 6
python app.py --action 7
python app.py --action 8
python app.py --action 9
python app.py --action 10
python app.py --action 11
python app.py --action 12
python app.py --action 13
python app.py --action 14
python app.py --action 15
```

## To perform Data validation for Oracle Databases

### For Windows
- Download Oracle Instance client from `Basic Package`  from `https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html`.

- Help
    - https://oracle.github.io/odpi/doc/installation.html#windows

### For MacOS

1. Downloaded Basic Package from this link:
   `https://www.oracle.com/au/database/technologies/instant-client/macos-intel-x86-downloads.html`

2. Unzipped the files

3. MacOS does not allow to open them. So, changed their permissions (for the files that gave an error):

   ```sh
   xattr -d com.apple.quarantine libclntsh.dylib.19.1
   xattr -d com.apple.quarantine libnnz19.dylib
   ...
   ```

4. Created a symbolic link:

   ```sh
   ln -s $HOME/Downloads/instantclient_19_8/libclntsh.dylib /Users/rk/anaconda/anaconda3/lib/python3.8/site-packages
   ```

5. Verified using this:

   ```
   python
   >> import cx_Oracle
   >> cx_Oracle.init_oracle_client(lib_dir=r"/Users/rk/Downloads/instantclient_19_8")
   ```

   It did not throw an error.

6. This link is helpful
   `https://stackoverflow.com/questions/69165050/python-dpi-1047-cannot-locate-dlopenlibclntsh-dylib-on-macos/69169723#69169723`

****
## Other Python packages required

```sh
cd DMS_Automation/config
pip install -r requirements.txt
```
****
## Using this tool
Before running the script:

- Configure DMS Endpoints using AWS Console
- Update the `src/config.py` file
   - `oracle_instance_client_path` - Update this to point to Oracle client path.
   - Update the following:
      - `replication_instance_arn`
      - `source_endpoint_arn`
      - `target_endpoint_arn`

### Setting up passwords
The tool needs to connect to databases for the following actions. 
- `12. validate_table_structures`
- `13. validate_data`
- `14. prepare_include_file_for_a_schema`

In general, to connect to a database, following properties are needed:
   - Host
   - Port
   - User
   - Database
   - password

The tool fetches these attributes (Except password) from the End points configured in DMS. 
DB Password can be supplied in two ways:

1. Harding in the config file (By populating `SOURCE_DB_PWD` & `TARGET_DB_PWD`)
2. By storing them in AWS Secrets Manager. Store them as `Key/Value`. 
   Configure the following parameters in `config.py`:

   - `SECRET_MANAGER_SECRET_NAME` (Should be same as the one used in AWS Secrets Manager)
   - `SOURCE_DB_SECRET_KEY`  (Should be same as the one used in AWS Secrets Manager)
   - `TARGET_DB_SECRET_KEY` (Should be same as the one used in AWS Secrets Manager)
****