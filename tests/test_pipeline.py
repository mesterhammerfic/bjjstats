import os

import pytest
import pandas as pd

from sqlalchemy import create_engine
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
        rs = con.execute("SELECT * FROM athlete")
        rows = rs.fetchall()
        assert len(rows) == 3
        rs = con.execute("SELECT * FROM performance")
        rows = rs.fetchall()
        assert len(rows) == 3
        rs = con.execute("SELECT * FROM match")
        rows = rs.fetchall()
        assert len(rows) == 3
