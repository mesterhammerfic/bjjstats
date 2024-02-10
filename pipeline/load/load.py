# this script takes defines a function that takes in 3 dataframes
# and loads them into the database. The dataframes are the athlete,
# performance, and match tables.
# here's how you would invoke it from the command line
# DB_URL=[SECRET] python load.py athlete.csv performance.csv match.csv

import pandas as pd
import sqlalchemy as sa
import os
import sys

def upload_data(athlete_df: pd.DataFrame, performance_df: pd.DataFrame, match_df: pd.DataFrame, engine: sa.engine.Engine):
    with engine.connect() as con:
        print("deleting existing data")
        statement = sa.text("TRUNCATE TABLE athlete, performance, match CASCADE;")
        con.execute(statement)
        con.commit()

    print("uploading athlete data")
    athlete_df.to_sql("athlete", engine, if_exists="append", index=False)

    print("uploading match data")
    match_df.to_sql("match", engine, if_exists="append", index=False)

    print("uploading performance data")
    performance_df.to_sql("performance", engine, if_exists="append", index=False)
    print("upload complete")


if __name__ == "__main__":
    athlete_df = pd.read_csv(sys.argv[1])
    performance_df = pd.read_csv(sys.argv[2])
    match_df = pd.read_csv(sys.argv[3])
    DB_URL = os.getenv("DB_URL")
    if DB_URL is None:
        raise Exception("You must set the DB_URL environment variable")
    engine = sa.create_engine(DB_URL)
    upload_data(athlete_df, performance_df, match_df, engine)
    engine.dispose()
