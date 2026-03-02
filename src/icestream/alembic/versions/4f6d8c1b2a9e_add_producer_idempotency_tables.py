"""add producer idempotency tables

Revision ID: 4f6d8c1b2a9e
Revises: 1c4f92de19a0
Create Date: 2026-02-23 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "4f6d8c1b2a9e"
down_revision: Union[str, None] = "1c4f92de19a0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "producer_sessions",
        sa.Column(
            "producer_id", sa.BigInteger(), sa.Identity(always=True), nullable=False
        ),
        sa.Column("transactional_id", sa.String(), nullable=True),
        sa.Column(
            "producer_epoch",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("expires_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "last_seen_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
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
        sa.PrimaryKeyConstraint("producer_id"),
        sa.UniqueConstraint("transactional_id"),
    )

    op.create_index(
        "ix_producer_sessions_expires_at",
        "producer_sessions",
        ["expires_at"],
        unique=False,
    )

    op.create_table(
        "producer_partition_state",
        sa.Column("producer_id", sa.BigInteger(), nullable=False),
        sa.Column("producer_epoch", sa.Integer(), nullable=False),
        sa.Column("topic_name", sa.String(), nullable=False),
        sa.Column("partition_number", sa.Integer(), nullable=False),
        sa.Column(
            "next_expected_sequence",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("last_acked_first_offset", sa.BigInteger(), nullable=True),
        sa.Column("last_acked_last_offset", sa.BigInteger(), nullable=True),
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
            ["producer_id"], ["producer_sessions.producer_id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["topic_name", "partition_number"],
            ["partitions.topic_name", "partitions.partition_number"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "producer_id", "producer_epoch", "topic_name", "partition_number"
        ),
    )

    op.create_index(
        "ix_producer_partition_state_topic_partition",
        "producer_partition_state",
        ["topic_name", "partition_number"],
        unique=False,
    )

    op.create_table(
        "producer_partition_recent_batches",
        sa.Column("producer_id", sa.BigInteger(), nullable=False),
        sa.Column("producer_epoch", sa.Integer(), nullable=False),
        sa.Column("topic_name", sa.String(), nullable=False),
        sa.Column("partition_number", sa.Integer(), nullable=False),
        sa.Column("base_sequence", sa.Integer(), nullable=False),
        sa.Column("last_sequence", sa.Integer(), nullable=False),
        sa.Column("first_offset", sa.BigInteger(), nullable=False),
        sa.Column("last_offset", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["producer_id"], ["producer_sessions.producer_id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["producer_id", "producer_epoch", "topic_name", "partition_number"],
            [
                "producer_partition_state.producer_id",
                "producer_partition_state.producer_epoch",
                "producer_partition_state.topic_name",
                "producer_partition_state.partition_number",
            ],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "producer_id",
            "producer_epoch",
            "topic_name",
            "partition_number",
            "base_sequence",
        ),
    )

    op.create_index(
        "ix_producer_recent_batches_lookup",
        "producer_partition_recent_batches",
        [
            "producer_id",
            "producer_epoch",
            "topic_name",
            "partition_number",
            "base_sequence",
        ],
        unique=False,
    )
    op.create_index(
        "ix_producer_recent_batches_created_at",
        "producer_partition_recent_batches",
        ["created_at"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "ix_producer_recent_batches_created_at",
        table_name="producer_partition_recent_batches",
    )
    op.drop_index(
        "ix_producer_recent_batches_lookup",
        table_name="producer_partition_recent_batches",
    )
    op.drop_table("producer_partition_recent_batches")

    op.drop_index(
        "ix_producer_partition_state_topic_partition",
        table_name="producer_partition_state",
    )
    op.drop_table("producer_partition_state")

    op.drop_index("ix_producer_sessions_expires_at", table_name="producer_sessions")
    op.drop_table("producer_sessions")
