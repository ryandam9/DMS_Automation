import collections
import json
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

import boto3

from config import *
from task_settings import task_settings

client = boto3.client('dms')

# noinspection PyBroadException
try:
    if not os.path.exists('logs'):
        os.mkdir('logs')

    if not os.path.exists('json_files'):
        os.mkdir('json_files')
except Exception as error:
    print('Something went wrong while creating directories logs, json_files')
    sys.exit(1)


# Setup logging
logfile_location = 'logs/dms_automation.log'
log_level = logging.DEBUG
log_format = '%(asctime)s %(levelname)s: [in %(filename)s:%(lineno)d] : %(message)s'

logger = logging.getLogger()
handler = RotatingFileHandler(logfile_location, maxBytes=100000, backupCount=10)
formatter = logging.Formatter(log_format)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(log_level)

# Create a named tuple
Table = collections.namedtuple('Table', 'schema, table, partition_column, lower_bound, upper_bound, action')

partitioned_tables = {}
non_partitioned_tables = {}


def process_csv_file(csv_file, action):
    """
    Reads Input CSV file that has got Schema and table details and create a dictionary where key
    is 'Schema' and value is 'List of Table names'.

    - The CSV file can contain tables from multiple schemas.
    - A table can be Partitioned or Non partitioned.
    - Sample file
         schema1, table1
         schema1, table2, partition_column, value1, value2
         schema1, table3
         schema2, table4
         schema2, table5, partition_column, value1, value2
         schema2, table6, partition_column, value1, value2

    - This function updates 'partitioned_tables',  'non_partitioned_tables'
    """
    with open(csv_file, 'r') as in_file:
        for line in in_file:
            # If line has only 2 fields, it is a non-partitioned table
            if len(line.split(',')) == 2:
                schema, table = line.split(',')

                # Remove any special chars
                schema = schema.strip()
                table = table.strip('\n').strip()

                table_obj = Table(schema=schema, table=table, partition_column=None, lower_bound=None, upper_bound=None,
                                  action=action)

                # Create an entry for the schema if it does not exist yet.
                if schema not in non_partitioned_tables.keys():
                    non_partitioned_tables[schema] = []

                non_partitioned_tables[schema].append(table_obj)
            else:
                # This is partitioned table.
                schema, table, partition_column, lower_bound, upper_bound = line.split(',')

                # Remove Spaces around this.
                schema = schema.strip()
                table = table.strip()
                partition_column = partition_column.strip()
                lower_bound = lower_bound.strip()
                upper_bound = upper_bound.strip('\n').strip()

                table_obj = Table(schema=schema, table=table, partition_column=partition_column,
                                  lower_bound=lower_bound, upper_bound=upper_bound, action=action)

                # Create an entry for the schema if it does not exist yet.
                if schema not in partitioned_tables.keys():
                    partitioned_tables[schema] = []

                partitioned_tables[schema].append(table_obj)


def create_tasks(all_tables):
    """
    Creates JSON files that are needed to create Replication tasks
    """
    schemas = all_tables.keys()
    index = 0

    for schema in schemas:
        for table in all_tables[schema]:
            logger.debug('Processing table: {}.{}'.format(table.schema, table.table))
            index += 1
            data = dict()
            file_name = '{}.{}.{}.json'.format(table.schema, table.table, index)
            data['rules'] = []

            # If it is a non-partitioned table
            if table.partition_column is None:
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
            else:
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
                                    {
                                        "filter-operator": "between",
                                        "start-value": table.lower_bound,
                                        "end-value": table.upper_bound
                                    }
                                ]
                            }
                        ]
                })

            # Add a Transformation
            data['rules'].append({
                "rule-type": "transformation",
                "rule-id": "2",
                "rule-name": "convert-schemas-to-lower",
                "rule-action": "convert-lowercase",
                "rule-target": "schema",
                "object-locator": {
                    "schema-name": "%"
                }
            })

            # Add a Transformation
            data['rules'].append({
                "rule-type": "transformation",
                "rule-id": "3",
                "rule-name": "convert-tables-to-lower",
                "rule-action": "convert-lowercase",
                "rule-target": "table",
                "object-locator": {
                    "schema-name": "%",
                    "table-name": "%"
                }
            })

            # Add a Transformation
            data['rules'].append(
                {
                    "rule-type": "transformation",
                    "rule-id": "4",
                    "rule-name": "convert-columns-to-lowercase",
                    "rule-action": "convert-lowercase",
                    "rule-target": "column",
                    "object-locator": {
                        "schema-name": "%",
                        "table-name": "%",
                        "column-name": "%"
                    }
                })

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


def create_dms_tasks(schema, table, index, table_mapping):
    """
    Creates AWS Data Migration Task
    """
    try:
        response = client.create_replication_task(
            ReplicationTaskIdentifier='{}-{}-{}'.format(schema, table, index).replace('_', '-'),
            SourceEndpointArn=source_endpoint_arn,
            TargetEndpointArn=target_endpoint_arn,
            ReplicationInstanceArn=replication_instance_arn,
            MigrationType='full-load',
            TableMappings=table_mapping,
            ReplicationTaskSettings=task_settings,
            Tags=[
                {
                    'Key': 'schema',
                    'Value': schema,
                },
                {
                    'Key': 'table',
                    'Value': table,
                },
                {
                    'Key': 'index',
                    'Value': index,
                },
            ],
        )
    except Exception as error:
        logger.error('Something went wrong while creating Replication task for: [{}.{}]'.format(schema, table))
        logger.error(error)


def process_json_files():
    for json_file in os.listdir('json_files'):
        file_handler = open(os.path.join('json_files', json_file), 'r')
        table_mapping = json.dumps((json.load(file_handler)))
        file_handler.close()

        schema, table, index, _ = json_file.split('.')
        create_dms_tasks(schema, table, index, table_mapping)


if __name__ == "__main__":
    logger.debug('{0:25} : {1:40}'.format('CSV File', csv_files_location))
    logger.debug('{0:25} : {1:40}'.format('replication_task_settings', replication_task_settings))
    logger.debug('{0:25} : {1:40}'.format('replication_instance_arn', replication_instance_arn))
    logger.debug('{0:25} : {1:40}'.format('source_endpoint_arn', source_endpoint_arn))
    logger.debug('{0:25} : {1:40}'.format('target_endpoint_arn', target_endpoint_arn))

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
