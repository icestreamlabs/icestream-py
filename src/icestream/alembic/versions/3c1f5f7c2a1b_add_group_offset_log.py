"""add group_offset_log table

Revision ID: 3c1f5f7c2a1b
Revises: 6c2f2db0c7b1
Create Date: 2025-12-16 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "3c1f5f7c2a1b"
down_revision: Union[str, None] = "6c2f2db0c7b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "group_offset_log",
        sa.Column("topic_partition", sa.Integer(), nullable=False),
        sa.Column("log_offset", sa.BigInteger(), nullable=False),
        sa.Column("key_bytes", sa.LargeBinary(), nullable=False),
        sa.Column("value_bytes", sa.LargeBinary(), nullable=True),
        sa.Column(
            "commit_ts",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.PrimaryKeyConstraint("topic_partition", "log_offset"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("group_offset_log")
