import os
import platform

import cx_Oracle

LOCATION = r"C:\Users\pavan\Downloads\instantclient-basic-windows.x64-21.3.0.0.0\instantclient_21_3"
print("ARCH:", platform.architecture())
print("FILES AT LOCATION:")

for name in os.listdir(LOCATION):
    print(name)

os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]

cx_Oracle.init_oracle_client(lib_dir=LOCATION)
conn = cx_Oracle.connect(
    "admin/@database-1.c7bxy1mpyupz.us-east-2.rds.amazonaws.com:1521/ORCL")
