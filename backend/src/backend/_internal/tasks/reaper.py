"""stuck-upload reaper.

a periodic docket task that closes the loop on upload jobs which sit in
`status = 'processing'` past a wall-clock budget. without it, a worker
that dies between staging an R2 blob and finalizing a track row leaves
the user's frontend spinning on "uploading to storage… 100%" forever.

see docs/internal/retrospectives/2026-05-10-worker-oom-loop-streaming.md
for the incident that motivated this task.

design (locked-in via PR conversation):

- threshold: 10 minutes. if `updated_at` hasn't ticked forward in 10 min,
  the job is definitionally stuck — the worker calls `update_progress`
  at each phase boundary, so a live task always has fresh `updated_at`.
- action: mark failed (no retry). user re-uploads. retry would require
  storing full upload kwargs on the `jobs` row; deferred to a follow-up.
- staged R2 cleanup: yes. `jobs.file_id / file_type / is_gated` are
  populated by the upload handler after staging, and the reaper uses them
  to delete the right bucket's blob before marking failed.
- notification: one bsky DM per reaper run summarizing affected users —
  not one per stuck job, to avoid DM spam in a system-wide outage like
  the May 6 incident (which would have produced 9 DMs).
"""

import logging
from datetime import UTC, datetime, timedelta

import logfire
from docket import Perpetual
from sqlalchemy import select

from backend._internal.notifications import notification_service
from backend.models import Artist
from backend.models.job import Job, JobStatus, JobType
from backend.storage import storage
from backend.utilities.database import db_session

logger = logging.getLogger(__name__)

# how long a job can sit in `processing` before we call it stuck. tracks
# `updated_at`, not `created_at`, so a legitimately long upload that's
# still emitting phase-progress updates is safe.
STUCK_UPLOAD_THRESHOLD = timedelta(minutes=10)


async def reap_stuck_uploads(
    perpetual: Perpetual = Perpetual(every=timedelta(seconds=60), automatic=True),  # noqa: B008
) -> None:
    """find upload jobs that have been stuck in `processing` and fail them.

    runs automatically every 60 seconds via docket's Perpetual scheduler.
    """
    cutoff = datetime.now(UTC) - STUCK_UPLOAD_THRESHOLD

    async with db_session() as db:
        result = await db.execute(
            select(Job).where(
                Job.type == JobType.UPLOAD.value,
                Job.status == JobStatus.PROCESSING.value,
                Job.updated_at < cutoff,
            )
        )
        stuck_jobs = list(result.scalars().all())

        if not stuck_jobs:
            return

        with logfire.span(
            "reap_stuck_uploads",
            stuck_count=len(stuck_jobs),
            threshold_minutes=int(STUCK_UPLOAD_THRESHOLD.total_seconds() / 60),
        ):
            # delete staged R2 blobs first, then mark failed. order matters:
            # if R2 delete throws on a single job we still want to mark the
            # others failed; we wrap each cleanup in a try/log block.
            owner_dids: set[str] = set()
            reaped_job_ids: list[str] = []
            for job in stuck_jobs:
                owner_dids.add(job.owner_did)
                reaped_job_ids.append(job.id)
                await _cleanup_staged_blob(job)
                job.status = JobStatus.FAILED.value
                job.message = "upload failed"
                job.error = (
                    f"upload timed out — task did not complete in "
                    f"{int(STUCK_UPLOAD_THRESHOLD.total_seconds() / 60)} minutes; "
                    f"please re-upload"
                )
                job.completed_at = datetime.now(UTC)
            await db.commit()

            logfire.warning(
                "reaped {count} stuck upload jobs ({owners} affected users)",
                count=len(stuck_jobs),
                owners=len(owner_dids),
            )

            # resolve affected DIDs to handles for the DM. best-effort —
            # if a lookup misses we fall back to the DID so the message
            # still ships.
            handles = await _resolve_owner_handles(owner_dids)
            await notification_service.send_reaper_notification(
                reaped_count=len(stuck_jobs),
                affected_handles=handles,
                threshold_minutes=int(STUCK_UPLOAD_THRESHOLD.total_seconds() / 60),
                job_ids=reaped_job_ids,
            )


async def _cleanup_staged_blob(job: Job) -> None:
    """delete the staged R2 audio blob for a stuck upload job.

    skips silently when the job pre-dates the cleanup-hints migration
    (file_id is NULL) — those rows simply don't get R2 cleanup, which is
    the same behavior we had before this PR.
    """
    if not job.file_id or not job.file_type:
        logfire.info(
            "skipping R2 cleanup for stuck job (no cleanup hints)",
            job_id=job.id,
        )
        return

    try:
        if job.is_gated:
            await storage.delete_gated(job.file_id, job.file_type)
        else:
            await storage.delete(job.file_id, job.file_type)
    except Exception as e:
        logfire.warning(
            "R2 cleanup failed for stuck job (continuing to mark failed anyway)",
            job_id=job.id,
            file_id=job.file_id,
            error=str(e),
        )


async def _resolve_owner_handles(dids: set[str]) -> list[str]:
    """map owner DIDs to handles via the artists table. unresolved DIDs
    pass through as `<did>` so the DM never silently drops a user.
    """
    if not dids:
        return []
    async with db_session() as db:
        rows = await db.execute(
            select(Artist.did, Artist.handle).where(Artist.did.in_(list(dids)))
        )
        by_did = dict(rows.all())
    return [by_did.get(d, d) for d in dids]
