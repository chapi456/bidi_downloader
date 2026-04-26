"""
File: test_step_check.py
Path: tests/test_step_check.py

Version: 1.0.0
Date: 2026-04-17

Changelog:
- 1.0.0 (2026-04-17): Tests unitaires step_check
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock

from database import BiDiDB
from steps.step_check import run, _classify, _find_files, _all_tasks_done, _has_success


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def db(tmp_path):
    return BiDiDB(tmp_path / "test.db")


@pytest.fixture
def save_dir(tmp_path):
    d = tmp_path / "downloads"
    d.mkdir()
    return d


@pytest.fixture
def cfg(save_dir):
    mock = MagicMock()
    mock.get_save_dir.return_value = str(save_dir)
    return mock


def _make_email_at_download_sent(db: BiDiDB, msg_id: str, source_url: str) -> int:
    eid = db.add_email(msg_id, subject="Test", body_text="corps")
    db.update_email_parse(eid, source_url=source_url, known_keywords=[], unknown_keywords=[])
    db.advance_step(eid, "parsed")
    db.advance_step(eid, "meta_done")
    db.advance_step(eid, "download_sent")
    return eid


# ── _classify ─────────────────────────────────────────────────────────────────

class TestClassify:
    def test_should_classify_mp4_as_video(self):
        assert _classify(Path("video.mp4")) == "video"

    def test_should_classify_jpg_as_image(self):
        assert _classify(Path("photo.jpg")) == "image"

    def test_should_classify_mp3_as_audio(self):
        assert _classify(Path("audio.mp3")) == "audio"

    def test_should_classify_unknown_as_other(self):
        assert _classify(Path("file.xyz")) == "other"

    def test_should_be_case_insensitive(self):
        assert _classify(Path("VIDEO.MP4")) == "video"


# ── _find_files ───────────────────────────────────────────────────────────────

class TestFindFiles:
    def test_should_find_video_for_primary_task(self, save_dir):
        video = save_dir / "author" / "video.mp4"
        video.parent.mkdir(parents=True)
        video.write_bytes(b"fake video")
        results = _find_files(save_dir, "https://reddit.com/r/sub/s/abc", "primary")
        assert video in results

    def test_should_find_thumbnail_by_name(self, save_dir):
        thumb = save_dir / "maxresdefault.jpg"
        thumb.write_bytes(b"fake thumb")
        results = _find_files(save_dir, "https://i.ytimg.com/vi/abc/maxresdefault.jpg", "thumbnail")
        assert thumb in results

    def test_should_not_find_thumbnail_with_wrong_name(self, save_dir):
        thumb = save_dir / "other_image.jpg"
        thumb.write_bytes(b"fake")
        results = _find_files(save_dir, "https://i.ytimg.com/vi/abc/maxresdefault.jpg", "thumbnail")
        assert thumb not in results

    def test_should_return_empty_when_save_dir_missing(self, tmp_path):
        results = _find_files(tmp_path / "nonexistent", "https://example.com/v", "primary")
        assert results == []

    def test_should_find_nested_files(self, save_dir):
        deep = save_dir / "a" / "b" / "c"
        deep.mkdir(parents=True)
        video = deep / "clip.webm"
        video.write_bytes(b"data")
        results = _find_files(save_dir, "https://example.com/v", "primary")
        assert video in results


# ── _all_tasks_done / _has_success ────────────────────────────────────────────

class TestTasksState:
    def test_should_return_true_when_all_done(self, db):
        eid = db.add_email("msg-done")
        tid = db.add_download_task(eid, "https://x.com/v", url_type="primary")
        db.set_download_task_status(tid, "done")
        assert _all_tasks_done(db, eid) is True

    def test_should_return_false_when_sent(self, db):
        eid = db.add_email("msg-sent")
        tid = db.add_download_task(eid, "https://x.com/v")
        db.send_download_task(tid, "gallery-dl")
        assert _all_tasks_done(db, eid) is False

    def test_should_return_false_when_no_tasks(self, db):
        eid = db.add_email("msg-empty")
        assert _all_tasks_done(db, eid) is False

    def test_has_success_true_when_one_done(self, db):
        eid = db.add_email("msg-has-success")
        tid = db.add_download_task(eid, "https://x.com/v")
        db.set_download_task_status(tid, "done")
        assert _has_success(db, eid) is True

    def test_has_success_false_when_all_failed(self, db):
        eid = db.add_email("msg-all-failed")
        tid = db.add_download_task(eid, "https://x.com/v")
        db.set_download_task_status(tid, "failed", error="crash")
        assert _has_success(db, eid) is False


# ── run() ─────────────────────────────────────────────────────────────────────

class TestRun:
    def test_should_find_file_and_advance_email(self, db, cfg, save_dir):
        eid = _make_email_at_download_sent(db, "msg-run-1", "https://reddit.com/r/x/s/abc")
        tid = db.add_download_task(eid, "https://reddit.com/r/x/s/abc", url_type="primary")
        db.send_download_task(tid, "gallery-dl")

        video = save_dir / "clip.mp4"
        video.write_bytes(b"data")

        stats = run(db, cfg)
        assert stats["found"] == 1
        assert stats["emails_done"] == 1
        assert db.get_email(eid)["step"] == "download_done"

    def test_should_not_advance_when_file_missing(self, db, cfg):
        eid = _make_email_at_download_sent(db, "msg-run-2", "https://reddit.com/r/x/s/abc")
        tid = db.add_download_task(eid, "https://reddit.com/r/x/s/abc", url_type="primary")
        db.send_download_task(tid, "gallery-dl")

        stats = run(db, cfg)
        assert stats["not_found"] == 1
        assert db.get_email(eid)["step"] == "download_sent"  # pas encore avancé

    def test_should_create_media_file_on_found(self, db, cfg, save_dir):
        eid = _make_email_at_download_sent(db, "msg-run-3", "https://reddit.com/r/x/s/abc")
        tid = db.add_download_task(eid, "https://reddit.com/r/x/s/abc", url_type="primary")
        db.send_download_task(tid, "gallery-dl")

        video = save_dir / "clip.mp4"
        video.write_bytes(b"data" * 100)

        run(db, cfg)
        files = db.get_media_files(eid)
        assert len(files) >= 1
        assert files[0]["file_type"] == "video"
        assert files[0]["is_primary"] == 1

    def test_should_set_task_status_to_done(self, db, cfg, save_dir):
        eid = _make_email_at_download_sent(db, "msg-run-4", "https://reddit.com/r/x/s/abc")
        tid = db.add_download_task(eid, "https://reddit.com/r/x/s/abc", url_type="primary")
        db.send_download_task(tid, "gallery-dl")
        (save_dir / "clip.mp4").write_bytes(b"x")

        run(db, cfg)
        assert db.get_download_tasks(eid)[0]["status"] == "done"

    def test_should_not_advance_when_some_tasks_still_sent(self, db, cfg, save_dir):
        eid = _make_email_at_download_sent(db, "msg-run-5", "https://reddit.com/r/x/s/abc")
        t1 = db.add_download_task(eid, "https://reddit.com/r/x/s/abc", url_type="primary")
        t2 = db.add_download_task(eid, "https://i.ytimg.com/thumb.jpg", url_type="thumbnail")
        db.send_download_task(t1, "gallery-dl")
        db.send_download_task(t2, "direct")

        (save_dir / "clip.mp4").write_bytes(b"x")
        # pas de thumbnail sur disque

        run(db, cfg)
        assert db.get_email(eid)["step"] == "download_sent"  # toujours bloqué

    def test_should_mark_failed_when_all_tasks_failed(self, db, cfg):
        eid = _make_email_at_download_sent(db, "msg-run-6", "https://reddit.com/r/x/s/abc")
        tid = db.add_download_task(eid, "https://reddit.com/r/x/s/abc", url_type="primary")
        db.send_download_task(tid, "gallery-dl")
        db.set_download_task_status(tid, "failed", error="timeout")

        run(db, cfg)
        assert db.get_email(eid)["step_status"] == "failed"

    def test_should_not_duplicate_media_file(self, db, cfg, save_dir):
        eid = _make_email_at_download_sent(db, "msg-run-7", "https://reddit.com/r/x/s/abc")
        tid = db.add_download_task(eid, "https://reddit.com/r/x/s/abc", url_type="primary")
        db.send_download_task(tid, "gallery-dl")
        (save_dir / "clip.mp4").write_bytes(b"x")

        run(db, cfg)
        run(db, cfg)  # deuxième run — ne doit pas dupliquer
        assert len(db.get_media_files(eid)) == 1

    def test_should_return_empty_stats_when_no_sent_tasks(self, db, cfg):
        stats = run(db, cfg)
        assert stats == {"checked": 0, "found": 0, "not_found": 0, "emails_done": 0}
