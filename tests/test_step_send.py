"""
File: test_step_send.py
Path: tests/test_step_send.py

Version: 1.2.0
Date: 2026-04-17

Changelog:
- 1.2.0 (2026-04-17): Patch _SENDERS dict au lieu des fonctions individuelles (fix mock capturé à l'import)
- 1.1.0 (2026-04-17): Corrections tests thumbnail et failed
- 1.0.0 (2026-04-17): Tests unitaires step_send
"""

import pytest
from unittest.mock import MagicMock, patch

from database import BiDiDB
from steps.step_send import run, _detect_downloader, _all_tasks_sent


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def db(tmp_path):
    return BiDiDB(tmp_path / "test.db")


@pytest.fixture
def cfg():
    mock = MagicMock()
    mock.get_gdl_enabled.return_value = True
    mock.get_jd_enabled.return_value = True
    mock.get_save_dir.return_value = "/tmp/bidi"
    mock.get_gdl_extra_args.return_value = []
    return mock


def _make_email_at_meta_done(db: BiDiDB, msg_id: str, source_url: str) -> int:
    eid = db.add_email(msg_id, subject="Test", body_text="corps")
    db.update_email_parse(eid, source_url=source_url, known_keywords=[], unknown_keywords=[])
    db.advance_step(eid, "parsed")
    db.advance_step(eid, "meta_done")
    return eid


def _noop_senders() -> dict:
    """Dict _SENDERS avec tous les senders en no-op pour les tests qui ne testent pas le dispatch."""
    return {k: MagicMock() for k in ("direct", "gallery-dl", "yt-dlp", "jdownloader")}


# ── _detect_downloader ────────────────────────────────────────────────────────

class TestDetectDownloader:
    def test_should_detect_direct_gif(self, cfg):
        assert _detect_downloader("https://i.redd.it/abc.gif", cfg) == "direct"

    def test_should_detect_direct_jpg(self, cfg):
        assert _detect_downloader("https://i.imgur.com/abc.jpg", cfg) == "direct"

    def test_should_detect_direct_reddit_media(self, cfg):
        assert _detect_downloader("https://reddit.com/media?url=https://preview.redd.it/x.gif", cfg) == "direct"

    def test_should_detect_gallery_dl_for_reddit(self, cfg):
        assert _detect_downloader("https://www.reddit.com/r/sub/s/abcdef", cfg) == "gallery-dl"

    def test_should_detect_ytdlp_for_youtube(self, cfg):
        cfg.get_gdl_enabled.return_value = False
        assert _detect_downloader("https://www.youtube.com/watch?v=abc", cfg) == "yt-dlp"

    def test_should_detect_ytdlp_for_pornhub(self, cfg):
        cfg.get_gdl_enabled.return_value = False
        assert _detect_downloader("https://www.pornhub.com/view_video.php?viewkey=abc", cfg) == "yt-dlp"

    def test_should_fallback_to_jdownloader(self, cfg):
        cfg.get_gdl_enabled.return_value = False
        assert _detect_downloader("https://unknown-site.com/video", cfg) == "jdownloader"

    def test_should_fallback_to_ytdlp_when_jd_disabled(self, cfg):
        cfg.get_gdl_enabled.return_value = False
        cfg.get_jd_enabled.return_value = False
        assert _detect_downloader("https://unknown-site.com/video", cfg) == "yt-dlp"


# ── _all_tasks_sent ───────────────────────────────────────────────────────────

class TestAllTasksSent:
    def test_should_return_true_when_all_sent(self, db):
        eid = db.add_email("msg-sent-all")
        tid = db.add_download_task(eid, "https://example.com/v", url_type="primary")
        db.send_download_task(tid, "gallery-dl")
        assert _all_tasks_sent(db, eid) is True

    def test_should_return_false_when_pending(self, db):
        eid = db.add_email("msg-pending")
        db.add_download_task(eid, "https://example.com/v", url_type="primary")
        assert _all_tasks_sent(db, eid) is False

    def test_should_return_false_when_no_tasks(self, db):
        eid = db.add_email("msg-no-tasks")
        assert _all_tasks_sent(db, eid) is False


# ── run() ─────────────────────────────────────────────────────────────────────

class TestRun:
    def test_should_advance_to_download_sent(self, db, cfg):
        eid = _make_email_at_meta_done(db, "msg-adv-1", "https://www.reddit.com/r/sub/s/abc")
        db.add_download_task(eid, "https://www.reddit.com/r/sub/s/abc", url_type="primary")
        with patch("steps.step_send._SENDERS", _noop_senders()):
            stats = run(db, cfg)
        assert stats["done"] == 1
        assert db.get_email(eid)["step"] == "download_sent"

    def test_should_send_all_pending_tasks(self, db, cfg):
        eid = _make_email_at_meta_done(db, "msg-send-2", "https://www.reddit.com/r/sub/s/abc")
        db.add_download_task(eid, "https://www.reddit.com/r/sub/s/abc", url_type="primary")
        db.add_download_task(eid, "https://i.ytimg.com/thumb.jpg", url_type="thumbnail")
        with patch("steps.step_send._SENDERS", _noop_senders()):
            stats = run(db, cfg)
        assert stats["tasks_sent"] == 2

    def test_should_mark_thumbnail_as_direct(self, db, cfg):
        """La task url_type=thumbnail doit toujours utiliser le sender direct."""
        eid = _make_email_at_meta_done(db, "msg-thumb-3", "https://www.reddit.com/r/sub/s/abc")
        db.add_download_task(eid, "https://www.reddit.com/r/sub/s/abc", url_type="primary")
        db.add_download_task(eid, "https://i.ytimg.com/vi/abc/maxresdefault.jpg", url_type="thumbnail")
        senders = _noop_senders()
        # _SENDERS est un dict capturé à l'import → patcher le dict lui-même
        with patch("steps.step_send._SENDERS", senders):
            run(db, cfg)
        senders["direct"].assert_called_once()
        senders["gallery-dl"].assert_called_once()

    def test_should_mark_failed_when_task_error(self, db, cfg):
        """Si le sender lève une exception, l'email doit être marqué failed."""
        eid = _make_email_at_meta_done(db, "msg-fail-4", "https://www.reddit.com/r/sub/s/abc")
        db.add_download_task(eid, "https://www.reddit.com/r/sub/s/abc", url_type="primary")
        crashing = MagicMock(side_effect=RuntimeError("crash réseau"))
        senders = {k: crashing for k in ("direct", "gallery-dl", "yt-dlp", "jdownloader")}
        with patch("steps.step_send._SENDERS", senders):
            stats = run(db, cfg)
        assert stats["failed"] == 1
        assert db.get_email(eid)["step_status"] == "failed"

    def test_should_mark_failed_when_no_tasks(self, db, cfg):
        eid = _make_email_at_meta_done(db, "msg-notask-5", "https://example.com/v")
        stats = run(db, cfg)
        assert stats["failed"] == 1
        assert db.get_email(eid)["step_status"] == "failed"

    def test_should_return_empty_stats_on_no_emails(self, db, cfg):
        assert run(db, cfg) == {"done": 0, "failed": 0, "tasks_sent": 0}

    def test_should_set_task_status_to_sent(self, db, cfg):
        eid = _make_email_at_meta_done(db, "msg-status-6", "https://www.reddit.com/r/sub/s/abc")
        db.add_download_task(eid, "https://www.reddit.com/r/sub/s/abc", url_type="primary")
        with patch("steps.step_send._SENDERS", _noop_senders()):
            run(db, cfg)
        task = db.get_download_tasks(eid)[0]
        assert task["status"] == "sent"
        assert task["downloader"] == "gallery-dl"
