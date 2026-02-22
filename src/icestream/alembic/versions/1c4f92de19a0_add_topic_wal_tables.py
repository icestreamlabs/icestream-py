"""add topic wal tables

Revision ID: 1c4f92de19a0
Revises: 7b09d6ef0f6b
Create Date: 2026-02-22 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "1c4f92de19a0"
down_revision: Union[str, None] = "7b09d6ef0f6b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "topic_wal_files",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("topic_name", sa.String(), nullable=False),
        sa.Column("uri", sa.Text(), nullable=False),
        sa.Column("etag", sa.Text(), nullable=True),
        sa.Column("total_bytes", sa.BigInteger(), nullable=False),
        sa.Column("total_messages", sa.BigInteger(), nullable=False),
        sa.Column("compacted_at", sa.TIMESTAMP(timezone=True), nullable=True),
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
        sa.ForeignKeyConstraint(["topic_name"], ["topics.name"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("uri"),
    )

    op.create_index(
        "ix_twf_topic_compacted",
        "topic_wal_files",
        ["topic_name", "compacted_at"],
        unique=False,
    )

    op.create_table(
        "topic_wal_file_offsets",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("topic_wal_file_id", sa.BigInteger(), nullable=False),
        sa.Column("topic_name", sa.String(), nullable=False),
        sa.Column("partition_number", sa.Integer(), nullable=False),
        sa.Column("base_offset", sa.BigInteger(), nullable=False),
        sa.Column("last_offset", sa.BigInteger(), nullable=False),
        sa.Column("byte_start", sa.BigInteger(), nullable=False),
        sa.Column("byte_end", sa.BigInteger(), nullable=False),
        sa.Column("min_timestamp", sa.BigInteger(), nullable=True),
        sa.Column("max_timestamp", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(
            ["topic_wal_file_id"], ["topic_wal_files.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["topic_name", "partition_number"],
            ["partitions.topic_name", "partitions.partition_number"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("topic_wal_file_id", "partition_number"),
    )

    op.create_index(
        "ix_twfo_file",
        "topic_wal_file_offsets",
        ["topic_wal_file_id"],
        unique=False,
    )
    op.create_index(
        "ix_twfo_topic_part_base",
        "topic_wal_file_offsets",
        ["topic_name", "partition_number", "base_offset"],
        unique=False,
    )
    op.create_index(
        "ix_twfo_topic_part_last",
        "topic_wal_file_offsets",
        ["topic_name", "partition_number", "last_offset"],
        unique=False,
    )
    op.create_index(
        "ix_twfo_topic_part_min_ts",
        "topic_wal_file_offsets",
        ["topic_name", "partition_number", "min_timestamp"],
        unique=False,
    )
    op.create_index(
        "ix_twfo_topic_part_max_ts",
        "topic_wal_file_offsets",
        ["topic_name", "partition_number", "max_timestamp"],
        unique=False,
    )

    op.create_table(
        "topic_wal_file_sources",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("topic_wal_file_id", sa.BigInteger(), nullable=False),
        sa.Column("wal_file_id", sa.BigInteger(), nullable=False),
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
            ["topic_wal_file_id"], ["topic_wal_files.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["wal_file_id"], ["wal_files.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("topic_wal_file_id", "wal_file_id"),
    )

    op.create_index(
        "ix_twfs_twf",
        "topic_wal_file_sources",
        ["topic_wal_file_id"],
        unique=False,
    )
    op.create_index(
        "ix_twfs_wf",
        "topic_wal_file_sources",
        ["wal_file_id"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_twfs_wf", table_name="topic_wal_file_sources")
    op.drop_index("ix_twfs_twf", table_name="topic_wal_file_sources")
    op.drop_table("topic_wal_file_sources")

    op.drop_index("ix_twfo_topic_part_max_ts", table_name="topic_wal_file_offsets")
    op.drop_index("ix_twfo_topic_part_min_ts", table_name="topic_wal_file_offsets")
    op.drop_index("ix_twfo_topic_part_last", table_name="topic_wal_file_offsets")
    op.drop_index("ix_twfo_topic_part_base", table_name="topic_wal_file_offsets")
    op.drop_index("ix_twfo_file", table_name="topic_wal_file_offsets")
    op.drop_table("topic_wal_file_offsets")

    op.drop_index("ix_twf_topic_compacted", table_name="topic_wal_files")
    op.drop_table("topic_wal_files")
