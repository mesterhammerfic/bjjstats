import os

import pytest
import pandas as pd

from sqlalchemy import create_engine, text, Engine
from alembic.config import Config
from alembic import command  # type: ignore

from pipeline.load.load import upload_data
from pipeline.extract.extract import (
    get_athletes_from_source,
    scrape_matches_and_performances,
)


@pytest.fixture  # type: ignore
def setup_database() -> Engine:

    engine = create_engine("sqlite:///test.db")
    config = Config("alembic.ini")
    command.upgrade(config, "head")
    yield engine
    engine.dispose()
    os.remove("test.db")


def test_upload_data(setup_database) -> None:  # type: ignore
    source_dir = os.path.dirname(__file__)
    athlete_df = pd.read_csv(
        os.path.join(source_dir, "fixtures", "athlete.csv"), index_col=0
    )
    performance_df = pd.read_csv(
        os.path.join(source_dir, "fixtures", "performance.csv"), index_col=0
    )
    match_df = pd.read_csv(
        os.path.join(source_dir, "fixtures", "match.csv"), index_col=0
    )

    upload_data(athlete_df, performance_df, match_df, setup_database)
    engine = setup_database
    with engine.connect() as con:
        statement = text("SELECT COUNT(*) FROM athlete;")
        result = con.execute(statement)
        assert result.scalar() == 3
        statement = text("SELECT COUNT(*) FROM performance;")
        result = con.execute(statement)
        assert result.scalar() == 6
        statement = text("SELECT COUNT(*) FROM match;")
        result = con.execute(statement)
        assert result.scalar() == 3


def test_get_athletes_from_source() -> None:
    source_dir = os.path.dirname(__file__)
    with open(os.path.join(source_dir, "fixtures", "athletes.html")) as f:
        # convert the file to a string
        html = f.read()
        # call the function
        df = get_athletes_from_source(html)
        # check the number of rows and columns
        assert df.shape == (2, 3)
        # check the column names
        assert list(df.columns) == ["name", "nickname", "url"]
        # check the values in the dataframe
        assert df.iloc[0]["name"] == "Aarae Alexander"
        assert df.iloc[0]["nickname"] == ""
        assert df.iloc[0]["url"] == "https://www.bjjheroes.com/?p=8141"
        assert df.iloc[1]["name"] == "Aaron Johnson"
        assert df.iloc[1]["nickname"] == "Tex"
        assert df.iloc[1]["url"] == "https://www.bjjheroes.com/?p=9246"


def test_scrape_matches_and_performances_0() -> None:
    """
    this tests if the athlete_0.html file is scraped correctly
    it is an example of an athlete with no matches
    the scraper should not fail and should return an empty dataframe
    """
    source = os.path.dirname(__file__)
    with open(os.path.join(source, "fixtures", "athlete_0.html")) as f:
        html = f.read()
        athlete_df = pd.DataFrame(
            {
                "name": ["Aarae Alexander"],
                "nickname": [""],
                "url": ["https://www.bjjheroes.com/?p=8141"],
            }
        )
        id_to_html = {0: html}
        performances_df, matches_df = scrape_matches_and_performances(
            athlete_df, id_to_html
        )
        assert performances_df.shape == (0, 5)
        assert matches_df.shape == (0, 3)


def test_scrape_matches_and_performances_1() -> None:
    """
    this tests if the athlete_1.html file is scraped correctly
    it is an example of an athlete with mtches
    the scraper should return a dataframe with 3 rows
    new athletes should be added to the df after it's scraped
    """
    source = os.path.dirname(__file__)
    with open(os.path.join(source, "fixtures", "athlete_1.html")) as f:
        html = f.read()
        athlete_df = pd.DataFrame(
            {
                "name": ["Aaron Johnson"],
                "nickname": ["Tex"],
                "url": ["https://www.bjjheroes.com/?p=9246"],
            }
        )
        lengeth_before = len(athlete_df)
        id_to_html = {0: html}
        performances_df, matches_df = scrape_matches_and_performances(
            athlete_df, id_to_html
        )
        assert performances_df.shape == (3, 5)
        assert matches_df.shape == (3, 3)
        assert len(athlete_df) == lengeth_before + 3
