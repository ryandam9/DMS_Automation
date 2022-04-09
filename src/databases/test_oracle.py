import os
import platform

import cx_Oracle

# -------------------------------------------------------------------------------------------------#
# Check Oracle DB Connectivity                                                                     #
# -------------------------------------------------------------------------------------------------#
# Set this to instant client location
LOCATION = r""

print("ARCH:", platform.architecture())
print("FILES AT LOCATION:")

for name in os.listdir(LOCATION):
    print(name)

os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]
cx_Oracle.init_oracle_client(lib_dir=LOCATION)

user = ""
password = ""
host = ""
port = ""
service = ""

conn = cx_Oracle.connect(f"{user}/{password}@{host}:{port}/{service}")
cur = conn.cursor()

cur.execute("SELECT * FROM V$VERSION")

for result in cur:
    print(result)

cur.close()
conn.close()
