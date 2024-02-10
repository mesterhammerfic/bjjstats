# this script takes defines a function that takes in 3 dataframes
# and loads them into the database. The dataframes are the athlete,
# performance, and match tables.
# here's how you would invoke it from the command line
# DB_URL=[SECRET] python load.py athlete.csv performance.csv match.csv
# or to load from s3 you would use the --s3 argument:
# DB_URL=[SECRET] python load.py --s3 2021-01-01

import pandas as pd
import sqlalchemy as sa
import os
import sys

CHUNKSIZE = 1000

def upload_data(
        athlete_df: pd.DataFrame,
        performance_df: pd.DataFrame,
        match_df: pd.DataFrame,
        engine: sa.engine.Engine
) -> None:
    """
    This function takes in 3 dataframes and an engine and loads the data into the database
    the following parameters match the tables in the target database
    :param athlete_df:
    :param performance_df:
    :param match_df:
    :param engine: the sqlalchemy engine to use
    """
    with engine.connect() as con:
        print("deleting existing data")
        statement = sa.text("DELETE FROM athlete;")
        con.execute(statement)
        con.commit()
        statement = sa.text("DELETE FROM performance;")
        con.execute(statement)
        con.commit()
        statement = sa.text("DELETE FROM match;")
        con.execute(statement)
        con.commit()

        print("uploading athlete data")
        athlete_df.to_sql(
            "athlete",
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=CHUNKSIZE,
        )
        print("uploading match data")
        match_df.to_sql(
            "match",
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=CHUNKSIZE,
        )
        print("uploading performance data")
        performance_df.to_sql(
            "performance",
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=CHUNKSIZE,
        )
        print("upload complete")


def upload_from_s3(
        s3_folder: str,
        engine: sa.engine.Engine
) -> None:
    """
    This function takes in an s3 folder and an engine and loads the data from the s3 folder into the database
    :param s3_folder: either test or a date string in the format YYYY-MM-DD
    :param engine: the sqlalchemy engine to use
    """
    athlete_df = pd.read_parquet(
        f"s3://bjjstats/bjjheroes-scrape-v1/{s3_folder}/athlete.parquet"
    )
    performance_df = pd.read_parquet(
        f"s3://bjjstats/bjjheroes-scrape-v1/{s3_folder}/performance.parquet"
    )
    match_df = pd.read_parquet(
        f"s3://bjjstats/bjjheroes-scrape-v1/{s3_folder}/match.parquet"
    )
    upload_data(athlete_df, performance_df, match_df, engine)


def lambda_handler(event, context):
    if event.get("s3_folder"):
        s3_folder = event["s3_folder"]
        DB_URL = os.getenv("DB_URL")
        if DB_URL is None:
            raise Exception("You must set the DB_URL environment variable")
        engine = sa.create_engine(DB_URL)
        upload_from_s3(s3_folder, engine)
        engine.dispose()
    else:
        raise Exception(
            "You must provide an s3_folder in the event"
        )


if __name__ == "__main__":
    DB_URL = os.getenv("DB_URL")
    if DB_URL is None:
        raise Exception("You must set the DB_URL environment variable")
    engine = sa.create_engine(DB_URL)
    if len(sys.argv) == 4:
        athlete_df = pd.read_csv(sys.argv[1])
        performance_df = pd.read_csv(sys.argv[2])
        match_df = pd.read_csv(sys.argv[3])
        upload_data(athlete_df, performance_df, match_df, engine)
    elif len(sys.argv) == 3 and sys.argv[1] == "--s3":
        s3_folder = sys.argv[2]
        upload_from_s3(s3_folder, engine)
        engine.dispose()
    else:
        raise Exception(
            "You must provide 3 csv files or use the --s3 argument to specify an s3 folder to load from"
        )