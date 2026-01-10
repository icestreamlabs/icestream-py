"""add group state version, sync deadline, and static member uniqueness

Revision ID: 6c2f2db0c7b1
Revises: e7c90b4c2f1a
Create Date: 2025-12-16 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "6c2f2db0c7b1"
down_revision: Union[str, None] = "e7c90b4c2f1a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "consumer_groups",
        sa.Column("state_version", sa.BigInteger(), nullable=False, server_default="0"),
    )
    op.add_column(
        "consumer_groups",
        sa.Column("sync_phase_deadline_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        "ck_consumer_groups_state_valid",
        "consumer_groups",
        "state IN ('Empty', 'PreparingRebalance', 'CompletingRebalance', 'Stable')",
    )

    op.drop_index("ix_group_members_gid_instance", table_name="group_members")
    op.create_index(
        "uq_group_members_gid_instance",
        "group_members",
        ["consumer_group_id", "group_instance_id"],
        unique=True,
        postgresql_where=sa.text("group_instance_id IS NOT NULL"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("uq_group_members_gid_instance", table_name="group_members")
    op.create_index(
        "ix_group_members_gid_instance",
        "group_members",
        ["consumer_group_id", "group_instance_id"],
        unique=False,
    )
    op.drop_constraint(
        "ck_consumer_groups_state_valid",
        "consumer_groups",
        type_="check",
    )
    op.drop_column("consumer_groups", "sync_phase_deadline_at")
    op.drop_column("consumer_groups", "state_version")
