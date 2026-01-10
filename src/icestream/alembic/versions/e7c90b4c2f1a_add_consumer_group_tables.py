"""add consumer group tables

Revision ID: e7c90b4c2f1a
Revises: 9f0f8ebe3914
Create Date: 2025-12-16 00:10:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = "e7c90b4c2f1a"
down_revision: Union[str, None] = "9f0f8ebe3914"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "consumer_groups",
        sa.Column("group_id", sa.String(), nullable=False),
        sa.Column("protocol_type", sa.String(), nullable=True),
        sa.Column("selected_protocol", sa.String(), nullable=True),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(), nullable=False),
        sa.Column("leader_member_id", sa.String(), nullable=True),
        sa.Column("session_timeout_ms", sa.Integer(), nullable=False),
        sa.Column("rebalance_timeout_ms", sa.Integer(), nullable=False),
        sa.Column("join_phase_deadline_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("id", sa.Integer(), sa.Identity(always=True), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("group_id"),
    )
    op.create_index(
        "ix_consumer_groups_gid",
        "consumer_groups",
        ["group_id"],
        unique=False,
    )

    op.create_table(
        "group_members",
        sa.Column("consumer_group_id", sa.Integer(), nullable=False),
        sa.Column("member_id", sa.String(), nullable=False),
        sa.Column("group_instance_id", sa.String(), nullable=True),
        sa.Column("session_timeout_ms", sa.Integer(), nullable=False),
        sa.Column("rebalance_timeout_ms", sa.Integer(), nullable=False),
        sa.Column(
            "last_heartbeat_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("member_generation", sa.Integer(), nullable=False),
        sa.Column("is_in_sync", sa.Boolean(), nullable=False),
        sa.Column("join_generation", sa.Integer(), nullable=False),
        sa.Column("assignment", sa.LargeBinary(), nullable=True),
        sa.Column("protocols_hash", sa.String(), nullable=True),
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["consumer_group_id"], ["consumer_groups.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("consumer_group_id", "member_id"),
    )
    op.create_index(
        "ix_group_members_gid_instance",
        "group_members",
        ["consumer_group_id", "group_instance_id"],
        unique=False,
    )
    op.create_index(
        "ix_group_members_gid_member",
        "group_members",
        ["consumer_group_id", "member_id"],
        unique=False,
    )
    op.create_index(
        "ix_group_members_gid_join",
        "group_members",
        ["consumer_group_id", "join_generation"],
        unique=False,
    )

    op.create_table(
        "group_member_protocols",
        sa.Column("group_member_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("metadata", sa.LargeBinary(), nullable=False),
        sa.Column("preference_rank", sa.Integer(), nullable=False),
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["group_member_id"], ["group_members.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("group_member_id", "name"),
    )
    op.create_index(
        "ix_gmp_member", "group_member_protocols", ["group_member_id"], unique=False
    )
    op.create_index(
        "ix_gmp_member_name",
        "group_member_protocols",
        ["group_member_id", "name"],
        unique=False,
    )

    op.create_table(
        "group_assignments",
        sa.Column("consumer_group_id", sa.Integer(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("topic_name", sa.String(), nullable=False),
        sa.Column("partition_number", sa.Integer(), nullable=False),
        sa.Column("group_member_id", sa.BigInteger(), nullable=False),
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["consumer_group_id"], ["consumer_groups.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["group_member_id"], ["group_members.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["topic_name", "partition_number"],
            ["partitions.topic_name", "partitions.partition_number"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "consumer_group_id",
            "generation",
            "topic_name",
            "partition_number",
            name="uq_group_assignment_gid_gen_tp",
        ),
    )
    op.create_index(
        "ix_group_assignments_gid_gen",
        "group_assignments",
        ["consumer_group_id", "generation"],
        unique=False,
    )
    op.create_index(
        "ix_group_assignments_member",
        "group_assignments",
        ["group_member_id"],
        unique=False,
    )

    op.create_table(
        "group_offsets",
        sa.Column("consumer_group_id", sa.Integer(), nullable=False),
        sa.Column("topic_name", sa.String(), nullable=False),
        sa.Column("partition_number", sa.Integer(), nullable=False),
        sa.Column("committed_offset", sa.BigInteger(), nullable=False),
        sa.Column("committed_metadata", sa.Text(), nullable=True),
        sa.Column(
            "commit_timestamp",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("committed_generation", sa.Integer(), nullable=False),
        sa.Column("committed_member_id", sa.String(), nullable=True),
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["consumer_group_id"], ["consumer_groups.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["topic_name", "partition_number"],
            ["partitions.topic_name", "partitions.partition_number"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("consumer_group_id", "topic_name", "partition_number"),
    )
    op.create_index(
        "ix_group_offsets_gid_tp",
        "group_offsets",
        ["consumer_group_id", "topic_name", "partition_number"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_group_offsets_gid_tp", table_name="group_offsets")
    op.drop_table("group_offsets")
    op.drop_index("ix_group_assignments_member", table_name="group_assignments")
    op.drop_index("ix_group_assignments_gid_gen", table_name="group_assignments")
    op.drop_table("group_assignments")
    op.drop_index("ix_gmp_member_name", table_name="group_member_protocols")
    op.drop_index("ix_gmp_member", table_name="group_member_protocols")
    op.drop_table("group_member_protocols")
    op.drop_index("ix_group_members_gid_join", table_name="group_members")
    op.drop_index("ix_group_members_gid_member", table_name="group_members")
    op.drop_index("ix_group_members_gid_instance", table_name="group_members")
    op.drop_table("group_members")
    op.drop_index("ix_consumer_groups_gid", table_name="consumer_groups")
    op.drop_table("consumer_groups")
