import os

import pytest
import pandas as pd

from sqlalchemy import create_engine, text
from alembic.config import Config
from alembic import command

from pipeline.load.load import upload_data

@pytest.fixture
def setup_database():

    engine = create_engine("sqlite:///test.db")
    config = Config("alembic.ini")
    command.upgrade(config, "head")
    yield engine
    engine.dispose()
    os.remove("test.db")


def test_upload_data(setup_database):
    source_dir = os.path.dirname(__file__)
    athlete_df = pd.read_csv(os.path.join(source_dir, "fixtures", "athlete.csv"))
    performance_df = pd.read_csv(os.path.join(source_dir, "fixtures", "performance.csv"))
    match_df = pd.read_csv(os.path.join(source_dir, "fixtures", "match.csv"))

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

