import os

# When I tried to execute this script from Windows CLI (Not Anaconda CLI)
# getting this exception:
#
#    ImportError: Unable to import required dependencies:
#    numpy: DLL load failed: The specified module could not be found.
#
# To fix it, I am changing the PATH to point to Anaconda library where
# the dlls are available. 

# This fix is applied for Windows.

module_path = [
    r'C:\Users\ravis\Anaconda3\Library\bin',
]

old_path = os.environ['PATH']
new_path = ";".join(module_path)
os.environ['PATH'] = f"{new_path};{old_path}"

import warnings
import pandas as pd
import sqlalchemy
from sqlalchemy import exc as sa_exc
from sqlalchemy.exc import SQLAlchemyError
import cx_Oracle

INSTANT_CLIENT_LOCATION = r"C:\Users\ravis\Desktop\ravi\instantclient_21_3"

# Use either of these.
#    1. Call init_oracle_client()  or
#    2. Set PATH  

# cx_Oracle.init_oracle_client(lib_dir=INSTANT_CLIENT_LOCATION)

os.environ['PATH'] = INSTANT_CLIENT_LOCATION + ";" + os.environ['PATH']

user = "admin"
password = ""
host = "database-1.c7bxy1mpyupz.us-east-2.rds.amazonaws.com"
port = "1521"
service = "ORCL"

try:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=sa_exc.SAWarning)

        engine = sqlalchemy.create_engine(
            f"oracle+cx_oracle://{user}:{password}@{host}:{port}/?service_name={service}",
            arraysize=1000,
            echo=True,
        )

        query = "SELECT * FROM V$VERSION"
        df = pd.read_sql(query, engine)
        print(df)

except SQLAlchemyError as e:
    error = str(e.__dict__["orig"])
    print(error)