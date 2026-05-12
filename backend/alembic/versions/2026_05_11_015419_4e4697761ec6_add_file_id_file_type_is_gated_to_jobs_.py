"""add file_id file_type is_gated to jobs and composite index for reaper

Revision ID: 4e4697761ec6
Revises: 5c56f12bc84d
Create Date: 2026-05-11 01:54:19.472028

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "4e4697761ec6"
down_revision: str | Sequence[str] | None = "5c56f12bc84d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """add cleanup-hint columns to jobs + composite index for the reaper scan.

    the three columns let the stuck-upload reaper (in
    `backend/_internal/tasks/reaper.py`) delete the staged R2 blob from the
    right bucket before marking a stuck upload row as failed. nullable so
    non-upload job types (export, pds_backfill) don't have to fill them and
    so old rows created before this migration just don't get R2 cleanup
    (which is the same behavior we had before this PR — acceptable for the
    handful of pre-migration rows).

    the composite index covers the reaper's hot query:
    `WHERE type = 'upload' AND status = 'processing' AND updated_at < ?`.
    without it, that scan would full-table-scan jobs every minute.
    """
    op.add_column("jobs", sa.Column("file_id", sa.String(), nullable=True))
    op.add_column("jobs", sa.Column("file_type", sa.String(), nullable=True))
    op.add_column("jobs", sa.Column("is_gated", sa.Boolean(), nullable=True))
    op.create_index(
        "idx_jobs_reaper_scan",
        "jobs",
        ["type", "status", "updated_at"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("idx_jobs_reaper_scan", table_name="jobs")
    op.drop_column("jobs", "is_gated")
    op.drop_column("jobs", "file_type")
    op.drop_column("jobs", "file_id")
