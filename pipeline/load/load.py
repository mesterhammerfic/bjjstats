"""
this script takes defines a function that takes in 3 dataframes
and loads them into the database. The dataframes are the athlete,
performance, and match tables.
here's how you would invoke it from the command line
DB_URL=[SECRET] python load.py directory_with_csv_files
or to load from s3 you would use the --s3 argument:
DB_URL=[SECRET] python load.py --s3 name_of_s3_folder
"""

from typing import Dict, Any

import pandas as pd
import sqlalchemy as sa
import os
import argparse

from aws_lambda_powertools.utilities.data_classes import ALBEvent
from aws_lambda_powertools.utilities.typing import LambdaContext
import awswrangler as wr

CHUNKSIZE = 1000


def upload_data(
    athlete_df: pd.DataFrame,
    performance_df: pd.DataFrame,
    match_df: pd.DataFrame,
    engine: sa.engine.Engine,
) -> None:
    """
    This function takes in 3 dataframes and an engine and loads the data into the database
    the following parameters match the tables in the target database
    :param athlete_df:
    :param performance_df:
    :param match_df:
    :param engine: the sqlalchemy engine to use
    """
    with engine.begin() as con:
        # here i check whether its a postgres or sqlite database
        if "sqlite" in engine.url.drivername:
            print("deleting existing data")
            statement = sa.text("DELETE FROM athlete;")
            con.execute(statement)
            statement = sa.text("DELETE FROM performance;")
            con.execute(statement)
            statement = sa.text("DELETE FROM match;")
            con.execute(statement)
        elif "postgres" in engine.url.drivername:
            # here i'll truncate all the tables in one go and reset the index
            print("deleting existing data")
            statement = sa.text(
                "TRUNCATE athlete, performance, match RESTART IDENTITY;"
            )
            con.execute(statement)
        print("loading data")
        match_df.to_sql("match", con, if_exists="append", index=False, method="multi")
        athlete_df.to_sql(
            "athlete", con, if_exists="append", index=False, method="multi"
        )
        performance_df.to_sql(
            "performance", con, if_exists="append", index=False, method="multi"
        )


def upload_from_s3(
    s3_folder: str, engine: sa.engine.Engine, region: str = "us-east-2"
) -> None:
    """
    This function takes in an s3 folder and an engine and loads the data from the s3 folder into the database
    :param s3_folder: either test or a date string in the format YYYY-MM-DD
    :param engine: the sqlalchemy engine to use
    """
    athlete_df = wr.pandas.read_parquet(
        path=f"s3://bjjstats/bjjheroes-scrape-v1/{s3_folder}/athlete.parquet?region={region}"
    )

    performance_df = wr.pandas.read_parquet(
        path=f"s3://bjjstats/bjjheroes-scrape-v1/{s3_folder}/performance.parquet?region={region}"
    )

    match_df = wr.pandas.read_parquet(
        path=f"s3://bjjstats/bjjheroes-scrape-v1/{s3_folder}/match.parquet?region={region}"
    )

    upload_data(athlete_df, performance_df, match_df, engine)


def lambda_handler(event: ALBEvent, context: LambdaContext) -> Dict[str, Any]:
    if event.get("s3_folder"):
        s3_folder = event["s3_folder"]
        DB_URL = os.getenv("DB_URL")
        if DB_URL is None:
            raise Exception("You must set the DB_URL environment variable")
        engine = sa.create_engine(DB_URL)
        upload_from_s3(s3_folder, engine)
        engine.dispose()
    else:
        raise Exception("You must provide an s3_folder in the event")
    return {"statusCode": 200, "body": "Data loaded"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load data into the database")
    parser.add_argument("input", type=str, help="the directory where the csv files are")
    parser.add_argument("--s3", action="store_true", help="whether to load from s3")
    args = parser.parse_args()
    DB_URL = os.getenv("DB_URL")
    if DB_URL is None:
        raise Exception("You must set the DB_URL environment variable")
    engine = sa.create_engine(DB_URL)
    if args.s3:
        upload_from_s3(args.input, engine)
    else:
        athlete_df = pd.read_csv(os.path.join(args.input, "athlete.csv"))
        performance_df = pd.read_csv(
            os.path.join(args.input, "performance.csv"), index_col=0
        )
        match_df = pd.read_csv(os.path.join(args.input, "match.csv"))
        upload_data(athlete_df, performance_df, match_df, engine)
    engine.dispose()
    print("data loaded")
