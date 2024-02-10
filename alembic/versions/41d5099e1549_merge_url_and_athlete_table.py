"""merge url and athlete table


Revision ID: 41d5099e1549
Revises: 72d11429849c
Create Date: 2024-02-10 12:51:20.803895

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '41d5099e1549'
down_revision: Union[str, None] = '72d11429849c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        (
            "ALTER TABLE athlete"
            " ADD COLUMN url VARCHAR"
        ),
    )
    op.execute(
        (
            "UPDATE athlete"
            " SET url = ("
            "     SELECT url"
            "     FROM url"
            "     WHERE athlete.id = url.athlete_id"
            " );"
        ),
    )
    op.execute(
        (
            "DROP TABLE url;"
        ),
    )


def downgrade() -> None:
    op.execute(
        (
            "CREATE TABLE url ("
            "    id SERIAL PRIMARY KEY,"
            "    url VARCHAR,"
            "    athlete_id INTEGER REFERENCES athlete(id)"
            ");"
        ),
    )
    op.execute(
        (
            "INSERT INTO url (url, athlete_id)"
            " SELECT url, id"
            " FROM athlete;"
        ),
    )
    op.execute(
        (
            "ALTER TABLE athlete"
            " DROP COLUMN url;"
        ),
    )


