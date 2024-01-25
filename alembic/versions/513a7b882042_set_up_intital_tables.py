"""set up intital tables

Revision ID: 513a7b882042
Revises: 
Create Date: 2024-01-12 15:05:01.008968

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = '513a7b882042'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE athlete (
            id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            nickname VARCHAR
        );
        CREATE TABLE url (
            id SERIAL PRIMARY KEY,
            url VARCHAR NOT NULL,
            athlete_id INT REFERENCES athlete(id) ON DELETE CASCADE
        );
        CREATE TABLE match (
            id SERIAL PRIMARY KEY,
            year INTEGER,
            competition VARCHAR,
            method VARCHAR
        );
        CREATE TABLE performance (
            id SERIAL PRIMARY KEY,
            athlete_id INT REFERENCES athlete(id) ON DELETE CASCADE, 
            match_id INT REFERENCES match(id) ON DELETE CASCADE,
            result VARCHAR
        );
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE athlete CASCADE;
        DROP TABLE url CASCADE;
        DROP TABLE match CASCADE;
        DROP TABLE performance CASCADE;
        """

    )
