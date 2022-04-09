import warnings

import pandas as pd
import sqlalchemy
from sqlalchemy import exc as sa_exc
from sqlalchemy.exc import SQLAlchemyError

user = ""
password = ""
host = ""
port = ""
service = ""

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
    raise e
