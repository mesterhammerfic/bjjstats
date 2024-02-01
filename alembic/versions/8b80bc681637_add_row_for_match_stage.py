"""add row for match stage

Revision ID: 8b80bc681637
Revises: 513a7b882042
Create Date: 2024-01-31 17:12:25.688716

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8b80bc681637'
down_revision: Union[str, None] = '513a7b882042'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE match 
        ADD COLUMN stage VARCHAR;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE match
        DROP COLUMN stage;
        """
    )
