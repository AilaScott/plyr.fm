"""Database-backed job tracking service."""

import logging
from datetime import UTC, datetime
from typing import Any

import logfire
from sqlalchemy import select

from backend.models.job import Job, JobStatus, JobType
from backend.utilities.database import db_session

logger = logging.getLogger(__name__)


class JobService:
    """Service for managing database-backed jobs."""

    async def create_job(
        self,
        job_type: JobType,
        owner_did: str,
        initial_message: str = "job created",
        *,
        file_id: str | None = None,
        file_type: str | None = None,
        is_gated: bool | None = None,
    ) -> str:
        """Create a new job and return its ID.

        for upload jobs, callers should pass `file_id`, `file_type`, and
        `is_gated` so the stuck-upload reaper can clean up the staged R2
        blob from the right bucket if the job stalls. other job types
        (export, pds_backfill) leave them None.
        """
        async with db_session() as db:
            job = Job(
                type=job_type.value,
                owner_did=owner_did,
                status=JobStatus.PENDING.value,
                message=initial_message,
                progress_pct=0.0,
                file_id=file_id,
                file_type=file_type,
                is_gated=is_gated,
            )
            db.add(job)
            await db.commit()
            await db.refresh(job)
            return job.id

    async def update_progress(
        self,
        job_id: str,
        status: JobStatus,
        message: str,
        progress_pct: float | None = None,
        phase: str | None = None,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        """Update job progress."""
        async with db_session() as db:
            stmt = select(Job).where(Job.id == job_id)
            result_db = await db.execute(stmt)
            job = result_db.scalar_one_or_none()

            if not job:
                logger.warning(f"attempted to update unknown job: {job_id}")
                return

            job.status = status.value
            job.message = message
            if progress_pct is not None:
                job.progress_pct = progress_pct
            if phase:
                job.phase = phase
            if result:
                job.result = {**(job.result or {}), **result}
            if error:
                job.error = error

            if status in (JobStatus.COMPLETED, JobStatus.FAILED):
                job.completed_at = datetime.now(UTC)

            await db.commit()

            # log significant updates
            if status in (JobStatus.COMPLETED, JobStatus.FAILED) or (
                progress_pct and int(progress_pct) % 25 == 0
            ):
                logfire.info(
                    "job updated",
                    job_id=job_id,
                    status=status.value,
                    progress=progress_pct,
                )

    async def set_cleanup_hints(
        self,
        job_id: str,
        *,
        file_id: str,
        file_type: str,
        is_gated: bool,
    ) -> None:
        """Populate the staged-media cleanup hints on an existing job.

        called by the upload handler once `stage_audio_to_storage` has
        returned a file_id. these fields are what the stuck-upload reaper
        reads to delete the staged R2 blob from the right bucket before
        marking a stalled job failed.
        """
        async with db_session() as db:
            stmt = select(Job).where(Job.id == job_id)
            db_result = await db.execute(stmt)
            job = db_result.scalar_one_or_none()
            if not job:
                logger.warning(
                    f"attempted to set cleanup hints on unknown job: {job_id}"
                )
                return
            job.file_id = file_id
            job.file_type = file_type
            job.is_gated = is_gated
            await db.commit()

    async def get_job(self, job_id: str) -> Job | None:
        """Get job by ID."""
        async with db_session() as db:
            stmt = select(Job).where(Job.id == job_id)
            result = await db.execute(stmt)
            return result.scalar_one_or_none()


job_service = JobService()
