import os
import platform

import cx_Oracle
import sys

# -------------------------------------------------------------------------------------------------#
# Check Oracle DB Connectivity                                                                     #
# -------------------------------------------------------------------------------------------------#

# Set this to instant client location
INSTANT_CLIENT_LOCATION = r"C:\Users\ravis\Desktop\ravi\instantclient_21_3"

print("Platform Architecture:", platform.architecture())
print(f"Files available @ {INSTANT_CLIENT_LOCATION}")

for name in os.listdir(INSTANT_CLIENT_LOCATION):
    print(name)

#os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]

## Upgrade cx_Oracle
## The init_oracle_client() function was introduced in 8.0
## python -m pip install --upgrade cx_Oracle
cx_Oracle.init_oracle_client(lib_dir=INSTANT_CLIENT_LOCATION)

user = "admin"
password = ""
host = "database-1.c7bxy1mpyupz.us-east-2.rds.amazonaws.com"
port = "1521"
service = "ORCL"

try:
    conn = cx_Oracle.connect(f"{user}/{password}@{host}:{port}/{service}")
    cur = conn.cursor()
    cur.execute("SELECT * FROM V$VERSION")

    for result in cur:
        print(result)

    cur.close()
    conn.close()

    print("You're able to talk to the DB")
except Exception as err:
    print("Error connecting to the DB.")
    print(err)
    sys.exit(1)
