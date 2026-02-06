"""add is_internal to topics

Revision ID: 7b09d6ef0f6b
Revises: 3c1f5f7c2a1b
Create Date: 2025-12-16 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "7b09d6ef0f6b"
down_revision: Union[str, None] = "3c1f5f7c2a1b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "topics",
        sa.Column(
            "is_internal",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("topics", "is_internal")
