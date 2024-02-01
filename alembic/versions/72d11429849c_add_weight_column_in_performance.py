"""add weight column in performance

Revision ID: 72d11429849c
Revises: 8b80bc681637
Create Date: 2024-02-01 17:03:21.272858

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '72d11429849c'
down_revision: Union[str, None] = '8b80bc681637'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE match 
        ADD COLUMN weight VARCHAR;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE match
        DROP COLUMN weight;
        """
    )
