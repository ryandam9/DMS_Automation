import os

from config import csv_files_location
from databases.oracle import oracle_execute_query, oracle_table_to_df
from databases.oracle_queries import oracle_queries
from databases.postgres import postgres_table_to_df


def get_tables_to_validate():
    tables_migrated = []

    # Read the Input CSV Files & gather a list of Schemas and tables
    # that are being migrated.
    for file in os.listdir(csv_files_location):
        file_full_path = os.path.join(csv_files_location, file)

        if file.startswith("include"):
            print(f"-> Reading {file}")

            with open(file_full_path, "r") as f:
                for line in f:
                    schema, table = line.split(",")[0], line.split(",")[1]

                    schema = schema.strip().upper()
                    table = table.strip().upper()

                    tables_migrated.append({"schema": schema, "table": table})    

    return tables_migrated


def data_validation_driver(src_config, tgt_config):
    tables_migrated = get_tables_to_validate()

    # Get Primary Keys
    primary_keys_query = oracle_queries["get_primary_key"]

    # Primary Keys
    primary_keys = {}

    for table in tables_migrated:
        result_set = oracle_table_to_df(src_config, primary_keys_query, [table["schema"], table["table"]])
        
        multiple_cols = [] 

        for row in result_set.values.tolist():
            multiple_cols.append(row[0].lower())

        primary_keys[table['table']] = multiple_cols

    # Validate the data in the tables
    for table in tables_migrated:
        schema = table["schema"]
        table = table["table"]

        query = f"SELECT * FROM {schema}.{table} WHERE ROWNUM < 100"
        source_df = oracle_table_to_df(src_config, query, None)
        
        # Extract primary keys only from the DF
        pk_list = primary_keys[table]      # [c1, c2, c3]
        sample_pk_values = source_df[pk_list].values.tolist()
        
        no_pk_cols = len(pk_list)

        # Prepare a SELECT query using the primary keys to be executed
        # on the target DB.
        query = query = f"WITH temp AS ("
        
        for index, sample_pk_value in enumerate(sample_pk_values):
            if index > 0:
                query += " UNION "

            # If it is a composite primary key, after zipping, the value looks like 
            # this:  [ (c1, v1), (c2, v2), (c3, v3) ]
            sinle_pk_entry = zip(pk_list, sample_pk_value)

            q = "SELECT "
            i = 0 
            for col in sinle_pk_entry:
                i += 1
                k, v = col[0], col[1]

                if i > 1:
                    q += ", "

                if type(v) == str:
                    q += f"'{v}' AS {k}"
                else:
                    q += f"{v} AS {k}"    

            query += q

        query += ")"        
        
        query += f"SELECT * FROM {schema}.{table} a, temp WHERE "

        for i in range(no_pk_cols):
            if i > 0:
                query += " AND "

            query += f"a.{pk_list[i]} = temp.{pk_list[i]}"

        print(query)
        print("\n")

        # Execute the query on the target DB
        target_df = postgres_table_to_df(tgt_config, query, None)        
        print(target_df)
        print("\n")


