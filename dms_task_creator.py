import collections
import json
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

import boto3

from config import *
from task_settings import task_settings

session = boto3.Session(profile_name='pavan')
client = session.client('dms')

# ----------------------------------------------------------------------------------------------------------------------#
# Initial setup
# ----------------------------------------------------------------------------------------------------------------------#
# noinspection PyBroadException
try:
    # Create logs, json_files if don't exist
    if not os.path.exists('logs'):
        os.mkdir('logs')

    if not os.path.exists('json_files'):
        os.mkdir('json_files')
except Exception as error:
    print('Something went wrong while creating directories logs, json_files')
    sys.exit(1)

# Create file to store task ARNs
task_arn_file = 'task_arn.txt'

# ----------------------------------------------------------------------------------------------------------------------#
# Setup logging
# ----------------------------------------------------------------------------------------------------------------------#
logfile_location = 'logs/dms_automation.log'
log_level = logging.DEBUG
log_format = '%(asctime)s %(levelname)s: [in %(filename)s:%(lineno)d] : %(message)s'

logger = logging.getLogger()
handler = RotatingFileHandler(logfile_location, maxBytes=100000, backupCount=10)
formatter = logging.Formatter(log_format)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(log_level)

# ----------------------------------------------------------------------------------------------------------------------#
# Create a named tuple
# ----------------------------------------------------------------------------------------------------------------------#
Table = collections.namedtuple('Table',
                               'schema, table, partition_column, operator, lower_bound, upper_bound, auto_partition, action')

partitioned_tables = {}
non_partitioned_tables = {}


def delete_json_files():
    # Delete existing json files
    for file in os.listdir('./json_files'):
        os.remove(os.path.join('.', 'json_files', file))
        print('File {} deleted'.format(file))


def add_to_non_partitioned_map(schema, obj):
    # Create an entry for the schema if it does not exist yet.
    if schema not in non_partitioned_tables.keys():
        non_partitioned_tables[schema] = []

    non_partitioned_tables[schema].append(obj)


def add_to_partitioned_map(schema, obj):
    # Create an entry for the schema if it does not exist yet.
    if schema not in partitioned_tables.keys():
        partitioned_tables[schema] = []

    partitioned_tables[schema].append(obj)


def process_csv_file(csv_file, action):
    """
    Reads Input CSV file that has got Schema and table details and create a dictionary where key
    is 'Schema' and value is 'List of Table names'.

    This function updates 'partitioned_tables',  'non_partitioned_tables'
    """
    with open(csv_file, 'r') as in_file:
        for line in in_file:
            # If line has only 2 fields, it is a non-partitioned table
            if len(line.split(',')) == 2:
                schema, table = line.split(',')

                # Remove any special chars
                schema = schema.strip()
                table = table.strip('\n').strip()

                table_obj = Table(schema=schema, table=table, partition_column=None, operator=None, lower_bound=None,
                                  upper_bound=None, auto_partition=None, action=action)

                add_to_non_partitioned_map(schema, table_obj)

            else:
                if len(line.split(',')) == 6:
                    # This is partitioned table.
                    schema, table, partition_column, operator, lower_bound, upper_bound = line.split(',')

                    # Remove Spaces around this.
                    schema = schema.strip()
                    table = table.strip()
                    partition_column = partition_column.strip()
                    operator = operator.strip().lower()
                    lower_bound = lower_bound.strip()
                    upper_bound = upper_bound.strip()

                    if len(upper_bound) == 0:
                        upper_bound = None

                    table_obj = Table(schema=schema, table=table, partition_column=partition_column, operator=operator,
                                      lower_bound=lower_bound, upper_bound=upper_bound, auto_partition=None,
                                      action=action)

                    # Create an entry for the schema if it does not exist yet.
                    if schema not in partitioned_tables.keys():
                        partitioned_tables[schema] = []

                    partitioned_tables[schema].append(table_obj)
                else:
                    if len(line.split(',')) == 3:
                        schema, table, _ = line.split(',')

                        # Remove any special chars
                        schema = schema.strip()
                        table = table.strip('\n').strip()

                        table_obj = Table(schema=schema, table=table, partition_column=None, operator=None,
                                          lower_bound=None,
                                          upper_bound=None, auto_partition=True, action=action)

                        add_to_partitioned_map(schema, table_obj)
                    else:
                        logging.error('This line does not have either 2 or 5 values. Check it.')
                        logging.error(line)


def convert_schemas_to_lowercase():
    return {
        "rule-type": "transformation",
        "rule-id": "3",
        "rule-name": "convert-schemas-to-lower",
        "rule-action": "convert-lowercase",
        "rule-target": "schema",
        "object-locator": {
            "schema-name": "%"
        }
    }


def convert_tables_to_lowercase():
    return {
        "rule-type": "transformation",
        "rule-id": "4",
        "rule-name": "convert-tables-to-lower",
        "rule-action": "convert-lowercase",
        "rule-target": "table",
        "object-locator": {
            "schema-name": "%",
            "table-name": "%"
        }
    }


def convert_columns_to_lowercase():
    return {
        "rule-type": "transformation",
        "rule-id": "5",
        "rule-name": "convert-columns-to-lowercase",
        "rule-action": "convert-lowercase",
        "rule-target": "column",
        "object-locator": {
            "schema-name": "%",
            "table-name": "%",
            "column-name": "%"
        }
    }


def create_tasks(all_tables):
    """
    Creates JSON files that are needed to create Replication tasks
    """
    schemas = all_tables.keys()
    index = 5

    for schema in schemas:
        for table in all_tables[schema]:
            logger.debug('Processing table: {}.{}'.format(table.schema, table.table))
            index += 1
            data = dict()

            file_name = table.schema

            if table.table == '%':
                file_name += '.all_tables'
            else:
                file_name += '.' + table.table

            if table.lower_bound is not None and table.upper_bound is not None:
                file_name += '.' + table.lower_bound + '.' + table.upper_bound
            else:
                if table.lower_bound is not None:
                    file_name += '.' + table.operator + '.' + table.lower_bound

            file_name += '.json'

            data['rules'] = []

            # If it is a non-partitioned table
            if table.partition_column is None and table.auto_partition is None:
                data['rules'].append({
                    "rule-type": "selection",
                    "rule-id": index,
                    "rule-name": index,
                    "object-locator": {
                        "schema-name": table.schema,
                        "table-name": table.table
                    },
                    "rule-action": table.action,
                })

            # If 'auto-partition' is specified,
            if table.partition_column is None and table.auto_partition is not None:
                data['rules'].append({
                    "rule-type": "selection",
                    "rule-id": index,
                    "rule-name": index,
                    "object-locator": {
                        "schema-name": table.schema,
                        "table-name": table.table
                    },
                    "rule-action": table.action
                })

                data['rules'].append({
                    "rule-type": "table-settings",
                    "rule-id": index + 1,
                    "rule-name": index + 1,
                    "object-locator": {
                        "schema-name": table.schema,
                        "table-name": table.table
                    },
                    "parallel-load": {
                        "type": "partitions-auto"
                    }
                })

            # If partition column is specified,
            if table.partition_column is not None:
                condition = {}
                if table.operator == 'between':
                    condition = {
                        "filter-operator": table.operator,
                        "start-value": table.lower_bound,
                        "end-value": table.upper_bound
                    }
                else:
                    condition = {
                        "filter-operator": table.operator,
                        "value": table.lower_bound
                    }

                data['rules'].append({
                    "rule-type": "selection",
                    "rule-id": index,
                    "rule-name": index,
                    "object-locator": {
                        "schema-name": table.schema,
                        "table-name": table.table
                    },
                    "rule-action": table.action,
                    "filters":
                        [
                            {
                                "filter-type": "source",
                                "column-name": table.partition_column,
                                "filter-conditions": [
                                    condition
                                ]
                            }
                        ]
                })

            # Add a Transformation
            data['rules'].append(convert_schemas_to_lowercase())
            data['rules'].append(convert_tables_to_lowercase())
            data['rules'].append(convert_columns_to_lowercase())

            with open(os.path.join('json_files', file_name), 'w') as fp:
                json.dump(data, fp)


def print_tables():
    logger.debug('All partitioned tables')
    schemas = partitioned_tables.keys()
    for schema in schemas:
        for table in partitioned_tables[schema]:
            logger.debug(table)

    logger.debug('All non partitioned tables')
    schemas = non_partitioned_tables.keys()
    for schema in schemas:
        for table in non_partitioned_tables[schema]:
            logger.debug(table)


def create_dms_task(task_id, table_mapping):
    """
    Creates AWS Data Migration Task and returns the ARN of created task
    """
    task_arn = ''
    try:
        response = client.create_replication_task(
            ReplicationTaskIdentifier=task_id,
            SourceEndpointArn=source_endpoint_arn,
            TargetEndpointArn=target_endpoint_arn,
            ReplicationInstanceArn=replication_instance_arn,
            MigrationType='full-load',
            TableMappings=table_mapping,
            ReplicationTaskSettings=task_settings
        )
        task_arn = response['ReplicationTask']['ReplicationTaskArn']

    except Exception as error:
        logger.error('Something went wrong while creating Replication task for task_id: {}'.format(task_id))
        logger.error(error)

    return task_arn


def process_json_files():
    """
    Reads all the json files and generates DMS tasks
    """
    arn_list = []
    count = 0

    # Create tasks for all JSON files
    for json_file in os.listdir('json_files'):
        file_handler = open(os.path.join('json_files', json_file), 'r')
        table_mapping = json.dumps((json.load(file_handler)))
        file_handler.close()

        task_id = json_file

        # Replace special chars, otherwise AWS will complain.
        task_id = task_id.replace('.json', '').replace('_', '-').replace('.', '-').strip()
        task_arn = create_dms_task(task_id, table_mapping)

        if task_arn != '':
            print('DMS task created for file: {}'.format(json_file))
            arn_list.append(task_arn)
        else:
            count += 1

    # Persist the ARNs in a file.
    with open(task_arn_file, 'w') as file_handle:
        [file_handle.write('%s\n' % arn) for arn in arn_list]

    if count > 0:
        print('{} errors encountered while creating DMS tasks. Check the log file.'.format(count))


def start_dms_tasks():
    count = 0
    with open(task_arn_file, 'r') as arn_file:
        for arn in arn_file:
            arn = arn.strip('\n')
            try:
                response = client.start_replication_task(
                    ReplicationTaskArn=arn,
                    StartReplicationTaskType='reload-target'
                )
                print('Task: {} has been started'.format(arn))
            except Exception as error:
                count += 1
                logger.error('Error starting task with ARN: {}'.format(arn))

    if count > 0:
        print('{} errors encountered while starting DMS tasks. Check the log file.'.format(count))


def delete_dms_tasks():
    count = 0
    with open(task_arn_file, 'r') as arn_file:
        for arn in arn_file:
            arn = arn.strip('\n')
            try:
                response = client.delete_replication_task(
                    ReplicationTaskArn=arn
                )
                print('Task: {} has been deleted'.format(arn))
            except Exception as error:
                count += 1
                logger.error('Error deleting task with ARN: {}'.format(arn))

    if count > 0:
        print('{} errors encountered while deleting DMS tasks. Check the log file.'.format(count))


def create_dms_tasks():
    delete_json_files()

    # Identify the CSV files and process them
    for file in os.listdir(csv_files_location):
        if file.startswith("include"):
            process_csv_file(file, "include")
        elif file.startswith("exclude"):
            process_csv_file(file, "exclude")

    # Print all tables
    print_tables()

    # Create tasks in JSON form
    create_tasks(partitioned_tables)
    create_tasks(non_partitioned_tables)

    logger.debug('JSON files have been created')

    # Create Replication tasks
    process_json_files()


def list_dms_tasks():
    response = client.describe_replication_tasks(
        MaxRecords=100
    )

    for task in response['ReplicationTasks']:
        err_msg = ''

        if 'LastFailureMessage' in task.keys():
            err_msg = task['LastFailureMessage']

        print('{0:50} {1:10} {2:30}'.format(task['ReplicationTaskArn'], task['Status'], err_msg))


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == '--run-tasks':
        start_dms_tasks()
        sys.exit(0)

    if len(sys.argv) == 2 and sys.argv[1] == '--delete-tasks':
        delete_dms_tasks()
        sys.exit(0)

    if len(sys.argv) == 2 and sys.argv[1] == '--list-tasks':
        list_dms_tasks()
        sys.exit(0)

    if len(sys.argv) == 2 and sys.argv[1] == '--create-tasks':
        logger.debug('{0:25} : {1:40}'.format('CSV File', csv_files_location))
        logger.debug('{0:25} : {1:40}'.format('replication_task_settings', replication_task_settings))
        logger.debug('{0:25} : {1:40}'.format('replication_instance_arn', replication_instance_arn))
        logger.debug('{0:25} : {1:40}'.format('source_endpoint_arn', source_endpoint_arn))
        logger.debug('{0:25} : {1:40}'.format('target_endpoint_arn', target_endpoint_arn))

        create_dms_tasks()
        sys.exit(0)

    print('Pass required parameter')
    print('Usage: python dms_task_creator.py [--create-tasks | --run-tasks | --delete-tasks | --list-tasks]')
    sys.exit(0)
