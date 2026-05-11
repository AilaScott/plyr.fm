"""tests for the stuck-upload reaper.

regression coverage for 2026-05-10 — without a reaper, an upload job
left stuck in `status = processing` (e.g. worker died mid-task) sits
there forever, the user's frontend spins, and we have no way to
notice short of the user reporting it.

see docs/internal/retrospectives/2026-05-10-worker-oom-loop-streaming.md
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

from sqlalchemy.ext.asyncio import AsyncSession

from backend._internal.tasks.reaper import (
    STUCK_UPLOAD_THRESHOLD,
    reap_stuck_uploads,
)
from backend.models import Artist
from backend.models.job import Job, JobStatus, JobType


def _stuck_in_past(minutes_ago: int) -> datetime:
    return datetime.now(UTC) - timedelta(minutes=minutes_ago)


async def _seed_artist(db: AsyncSession, *, did: str, handle: str) -> Artist:
    artist = Artist(did=did, handle=handle, display_name=handle)
    db.add(artist)
    await db.commit()
    return artist


async def _seed_upload_job(
    db: AsyncSession,
    *,
    owner_did: str,
    status: JobStatus = JobStatus.PROCESSING,
    updated_at: datetime,
    file_id: str | None = "abc123",
    file_type: str | None = "mp3",
    is_gated: bool | None = False,
) -> Job:
    job = Job(
        type=JobType.UPLOAD.value,
        status=status.value,
        owner_did=owner_did,
        message="uploading to storage...",
        progress_pct=100.0,
        file_id=file_id,
        file_type=file_type,
        is_gated=is_gated,
        created_at=updated_at,
        updated_at=updated_at,
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)
    return job


async def test_reaper_fails_stuck_upload_and_deletes_staged_blob(
    db_session: AsyncSession,
) -> None:
    """the central case. job is past the threshold → R2 delete + mark failed."""
    await _seed_artist(db_session, did="did:plc:stuck", handle="stuck.test")
    stuck = await _seed_upload_job(
        db_session,
        owner_did="did:plc:stuck",
        updated_at=_stuck_in_past(int(STUCK_UPLOAD_THRESHOLD.total_seconds() / 60) + 5),
    )

    with (
        patch(
            "backend._internal.tasks.reaper.storage.delete",
            new_callable=AsyncMock,
        ) as mock_delete,
        patch(
            "backend._internal.tasks.reaper.notification_service.send_reaper_notification",
            new_callable=AsyncMock,
        ) as mock_notify,
    ):
        await reap_stuck_uploads()

    mock_delete.assert_awaited_once_with("abc123", "mp3")
    mock_notify.assert_awaited_once()
    notify_kwargs = mock_notify.await_args.kwargs
    assert notify_kwargs["reaped_count"] == 1
    assert notify_kwargs["affected_handles"] == ["stuck.test"]
    assert notify_kwargs["job_ids"] == [stuck.id]

    refreshed = await db_session.get(Job, stuck.id)
    assert refreshed is not None
    assert refreshed.status == JobStatus.FAILED.value
    assert refreshed.error is not None
    assert "timed out" in refreshed.error
    assert refreshed.completed_at is not None


async def test_reaper_leaves_recent_processing_jobs_alone(
    db_session: AsyncSession,
) -> None:
    """false-positive safety. a job that's still ticking forward must not be reaped."""
    await _seed_artist(db_session, did="did:plc:fresh", handle="fresh.test")
    fresh = await _seed_upload_job(
        db_session,
        owner_did="did:plc:fresh",
        updated_at=_stuck_in_past(2),  # well under threshold
    )

    with (
        patch(
            "backend._internal.tasks.reaper.storage.delete",
            new_callable=AsyncMock,
        ) as mock_delete,
        patch(
            "backend._internal.tasks.reaper.notification_service.send_reaper_notification",
            new_callable=AsyncMock,
        ) as mock_notify,
    ):
        await reap_stuck_uploads()

    mock_delete.assert_not_awaited()
    mock_notify.assert_not_awaited()

    refreshed = await db_session.get(Job, fresh.id)
    assert refreshed is not None
    assert refreshed.status == JobStatus.PROCESSING.value


async def test_reaper_uses_delete_gated_for_gated_uploads(
    db_session: AsyncSession,
) -> None:
    """gated tracks live in a separate R2 bucket; cleanup must route correctly."""
    await _seed_artist(db_session, did="did:plc:gated", handle="gated.test")
    gated = await _seed_upload_job(
        db_session,
        owner_did="did:plc:gated",
        updated_at=_stuck_in_past(int(STUCK_UPLOAD_THRESHOLD.total_seconds() / 60) + 5),
        is_gated=True,
    )

    with (
        patch(
            "backend._internal.tasks.reaper.storage.delete",
            new_callable=AsyncMock,
        ) as mock_delete,
        patch(
            "backend._internal.tasks.reaper.storage.delete_gated",
            new_callable=AsyncMock,
        ) as mock_delete_gated,
        patch(
            "backend._internal.tasks.reaper.notification_service.send_reaper_notification",
            new_callable=AsyncMock,
        ),
    ):
        await reap_stuck_uploads()

    mock_delete.assert_not_awaited()
    mock_delete_gated.assert_awaited_once_with("abc123", "mp3")

    refreshed = await db_session.get(Job, gated.id)
    assert refreshed is not None
    assert refreshed.status == JobStatus.FAILED.value


async def test_reaper_handles_job_without_cleanup_hints(
    db_session: AsyncSession,
) -> None:
    """rows that pre-date the cleanup-hints migration must still get marked
    failed; R2 cleanup is skipped (best effort)."""
    await _seed_artist(db_session, did="did:plc:legacy", handle="legacy.test")
    legacy = await _seed_upload_job(
        db_session,
        owner_did="did:plc:legacy",
        updated_at=_stuck_in_past(int(STUCK_UPLOAD_THRESHOLD.total_seconds() / 60) + 5),
        file_id=None,
        file_type=None,
        is_gated=None,
    )

    with (
        patch(
            "backend._internal.tasks.reaper.storage.delete",
            new_callable=AsyncMock,
        ) as mock_delete,
        patch(
            "backend._internal.tasks.reaper.storage.delete_gated",
            new_callable=AsyncMock,
        ) as mock_delete_gated,
        patch(
            "backend._internal.tasks.reaper.notification_service.send_reaper_notification",
            new_callable=AsyncMock,
        ) as mock_notify,
    ):
        await reap_stuck_uploads()

    mock_delete.assert_not_awaited()
    mock_delete_gated.assert_not_awaited()
    mock_notify.assert_awaited_once()

    refreshed = await db_session.get(Job, legacy.id)
    assert refreshed is not None
    assert refreshed.status == JobStatus.FAILED.value


async def test_reaper_sends_one_batched_dm_for_multiple_stuck_jobs(
    db_session: AsyncSession,
) -> None:
    """May 6 scenario: 9 stuck jobs across 3 users → ONE DM, not 9.

    avoids spamming the admin during a system-wide failure (and prevents
    rate-limited bsky DMs from dropping the notification entirely).
    """
    await _seed_artist(db_session, did="did:plc:a", handle="alice.test")
    await _seed_artist(db_session, did="did:plc:b", handle="bob.test")

    minutes_past = int(STUCK_UPLOAD_THRESHOLD.total_seconds() / 60) + 5
    for did in ("did:plc:a", "did:plc:a", "did:plc:b"):
        await _seed_upload_job(
            db_session,
            owner_did=did,
            updated_at=_stuck_in_past(minutes_past),
        )

    with (
        patch(
            "backend._internal.tasks.reaper.storage.delete",
            new_callable=AsyncMock,
        ),
        patch(
            "backend._internal.tasks.reaper.notification_service.send_reaper_notification",
            new_callable=AsyncMock,
        ) as mock_notify,
    ):
        await reap_stuck_uploads()

    mock_notify.assert_awaited_once()
    kwargs = mock_notify.await_args.kwargs
    assert kwargs["reaped_count"] == 3
    assert sorted(kwargs["affected_handles"]) == ["alice.test", "bob.test"]
    assert len(kwargs["job_ids"]) == 3


async def test_reaper_marks_failed_even_when_r2_delete_throws(
    db_session: AsyncSession,
) -> None:
    """R2 cleanup is best-effort — a transient delete failure must not
    prevent us from marking the user's job failed, otherwise the user
    keeps seeing the indefinite progress bar.
    """
    await _seed_artist(db_session, did="did:plc:r2fail", handle="r2fail.test")
    job = await _seed_upload_job(
        db_session,
        owner_did="did:plc:r2fail",
        updated_at=_stuck_in_past(int(STUCK_UPLOAD_THRESHOLD.total_seconds() / 60) + 5),
    )

    with (
        patch(
            "backend._internal.tasks.reaper.storage.delete",
            new_callable=AsyncMock,
            side_effect=RuntimeError("R2 temporarily unavailable"),
        ),
        patch(
            "backend._internal.tasks.reaper.notification_service.send_reaper_notification",
            new_callable=AsyncMock,
        ),
    ):
        await reap_stuck_uploads()

    refreshed = await db_session.get(Job, job.id)
    assert refreshed is not None
    assert refreshed.status == JobStatus.FAILED.value
