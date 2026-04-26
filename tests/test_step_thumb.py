"""
File: test_step_thumb.py
Path: tests/test_step_thumb.py

Version: 1.0.0
Date: 2026-04-17

Changelog:
- 1.0.0 (2026-04-17): Tests unitaires step_thumb
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from database import BiDiDB
from steps.step_thumb import run, _extract_video_frame, _thumb_path_for


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


def _make_email_at_download_done(db: BiDiDB, msg_id: str) -> int:
    eid = db.add_email(msg_id, subject="Test", body_text="corps")
    db.update_email_parse(eid, source_url="https://example.com/v", known_keywords=[], unknown_keywords=[])
    db.advance_step(eid, "parsed")
    db.advance_step(eid, "meta_done")
    db.advance_step(eid, "download_sent")
    db.advance_step(eid, "download_done")
    return eid


# ── _thumb_path_for ───────────────────────────────────────────────────────────

class TestThumbPathFor:
    def test_should_replace_extension_with_thumb_jpg(self):
        p = Path("/downloads/author/video.mp4")
        assert _thumb_path_for(p) == Path("/downloads/author/video.thumb.jpg")

    def test_should_work_with_webm(self):
        p = Path("/downloads/clip.webm")
        assert _thumb_path_for(p).suffix == ".jpg"


# ── _extract_video_frame ──────────────────────────────────────────────────────

class TestExtractVideoFrame:
    def test_should_return_true_on_success(self, tmp_path):
        out = tmp_path / "frame.jpg"
        out.write_bytes(b"fake")  # simuler que ffmpeg a produit le fichier
        with patch("steps.step_thumb.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = _extract_video_frame(Path("/fake/video.mp4"), out)
        assert result is True

    def test_should_return_false_on_nonzero_exit(self, tmp_path):
        out = tmp_path / "frame.jpg"
        with patch("steps.step_thumb.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1)
            result = _extract_video_frame(Path("/fake/video.mp4"), out)
        assert result is False

    def test_should_return_false_when_file_not_created(self, tmp_path):
        out = tmp_path / "frame.jpg"  # fichier absent
        with patch("steps.step_thumb.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = _extract_video_frame(Path("/fake/video.mp4"), out)
        assert result is False


# ── run() ─────────────────────────────────────────────────────────────────────

class TestRun:
    def test_should_reuse_existing_thumbnail(self, db, cfg, save_dir):
        eid = _make_email_at_download_done(db, "msg-t1")
        tid = db.add_download_task(eid, "https://example.com/v")
        db.add_media_file(eid, "thumb.jpg", file_type="thumbnail", task_id=tid)
        stats = run(db, cfg)
        assert stats["done"] == 1
        assert db.get_email(eid)["step"] == "thumb_done"
        # pas de doublon
        assert len([f for f in db.get_media_files(eid) if f["file_type"] == "thumbnail"]) == 1

    def test_should_use_image_as_own_thumbnail(self, db, cfg, save_dir):
        eid = _make_email_at_download_done(db, "msg-t2")
        img = save_dir / "photo.jpg"
        img.write_bytes(b"fake image")
        db.add_media_file(eid, "photo.jpg", file_type="image", is_primary=True, file_size=10)
        stats = run(db, cfg)
        assert stats["done"] == 1
        thumbs = [f for f in db.get_media_files(eid) if f["file_type"] == "thumbnail"]
        assert len(thumbs) == 1
        assert thumbs[0]["file_path"] == "photo.jpg"

    def test_should_extract_frame_for_video(self, db, cfg, save_dir):
        eid = _make_email_at_download_done(db, "msg-t3")
        video = save_dir / "clip.mp4"
        video.write_bytes(b"fake video")
        db.add_media_file(eid, "clip.mp4", file_type="video", is_primary=True, file_size=10)

        thumb_path = save_dir / "clip.thumb.jpg"

        def fake_extract(vpath, opath, offset=None):
            opath.write_bytes(b"fake thumb")
            return True

        with patch("steps.step_thumb._extract_video_frame", side_effect=fake_extract):
            stats = run(db, cfg)

        assert stats["done"] == 1
        thumbs = [f for f in db.get_media_files(eid) if f["file_type"] == "thumbnail"]
        assert len(thumbs) == 1
        assert "thumb.jpg" in thumbs[0]["file_path"]

    def test_should_advance_even_when_ffmpeg_fails(self, db, cfg, save_dir):
        eid = _make_email_at_download_done(db, "msg-t4")
        video = save_dir / "clip.mp4"
        video.write_bytes(b"fake")
        db.add_media_file(eid, "clip.mp4", file_type="video", is_primary=True)
        with patch("steps.step_thumb._extract_video_frame", return_value=False):
            stats = run(db, cfg)
        assert stats["done"] == 1
        assert db.get_email(eid)["step"] == "thumb_done"

    def test_should_skip_when_no_primary_file(self, db, cfg):
        eid = _make_email_at_download_done(db, "msg-t5")
        stats = run(db, cfg)
        assert stats["skipped"] == 1
        assert db.get_email(eid)["step"] == "thumb_done"

    def test_should_return_empty_stats_on_no_emails(self, db, cfg):
        assert run(db, cfg) == {"done": 0, "skipped": 0, "failed": 0}

    def test_should_process_multiple_emails(self, db, cfg, save_dir):
        for i in range(3):
            eid = _make_email_at_download_done(db, f"msg-multi-{i}")
            img = save_dir / f"photo{i}.jpg"
            img.write_bytes(b"x")
            db.add_media_file(eid, f"photo{i}.jpg", file_type="image", is_primary=True)
        stats = run(db, cfg)
        assert stats["done"] == 3
