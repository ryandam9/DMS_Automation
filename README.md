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
─
╰─○ cd DMS_Automation

─
╰─○ ls -l
-rw-rw-r-- 1 ryandam ryandam   419 Jul 10 22:06 config.py
-rw-rw-r-- 1 ryandam ryandam 18862 Jul 11 10:54 dms_task_creator.py
-rw-rw-r-- 1 ryandam ryandam   178 Jul  9 23:07 include.csv
-rw-rw-r-- 1 ryandam ryandam  2973 Jul 11 11:23 README.md
-rw-rw-r-- 1 ryandam ryandam  5428 Jun 20 14:20 task_settings.py
```

### 
```sh
─
╰─○ python3 dms_task_creator.py 
Pass required parameter
Usage: python dms_task_creator.py [--create-tasks | --run-tasks | --delete-tasks | --list-tasks]
``` 

```sh
─
╰─○ python3 dms_task_creator.py --create-tasks
File ADMIN-JOB-HISTORY-1998-01-01-1999-12-31.json deleted
File ADMIN-EMPLOYEES-1995-05-01.json deleted
File ADMIN.all_tables.json deleted
DMS task created for file: ADMIN-JOB-HISTORY-1998-01-01-1999-12-31.json
DMS task created for file: ADMIN-EMPLOYEES-1995-05-01.json
DMS task created for file: ADMIN.all_tables.json
3 tasks have been created and ready
```

```sh
─
╰─○ python3 dms_task_creator.py --list-tasks  
arn:aws:dms:us-east-2:999999999999:task:YYY ready                                    
arn:aws:dms:us-east-2:999999999999:task:ZZZ ready                                    
arn:aws:dms:us-east-2:999999999999:task:XXX ready
```

```sh
─
╰─○ python3 dms_task_creator.py --run-tasks  
Task: arn:aws:dms:us-east-2:999999999999:task:XXX has been started
Task: arn:aws:dms:us-east-2:999999999999:task:ZZZ has been started
Task: arn:aws:dms:us-east-2:999999999999:task:YYY has been started
3 tasks have been started
```



```sh
─
╰─○ python3 dms_task_creator.py --list-tasks 
arn:aws:dms:us-east-2:999999999999:task:YYY starting                                 
arn:aws:dms:us-east-2:999999999999:task:ZZZ starting                                 
arn:aws:dms:us-east-2:999999999999:task:XXX starting                                 
```
```shell script
─
╰─○  python3 dms_task_creator.py --list-tasks
arn:aws:dms:us-east-2:999999999999:task:YYY stopped                                  
arn:aws:dms:us-east-2:999999999999:task:ZZZ stopped                                  
arn:aws:dms:us-east-2:999999999999:task:XXX stopped                                  
```

```shell script
─
╰─○  python3 dms_task_creator.py --delete-tasks
Task: arn:aws:dms:us-east-2:999999999999:task:XXX deletion in progress...
Task: arn:aws:dms:us-east-2:999999999999:task:ZZZ deletion in progress...
Task: arn:aws:dms:us-east-2:999999999999:task:YYY deletion in progress...
3 tasks have been deleted!
```
****
