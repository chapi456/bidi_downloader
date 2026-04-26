"""
File: test_step_meta.py
Path: tests/test_step_meta.py

Version: 1.1.0
Date: 2026-04-17

Changelog:
- 1.1.0 (2026-04-17): Ajout tests URL non supportée → meta_done/ok (pas failed)
- 1.0.1 (2026-04-17): Fix noms champs DB (body_text, source_url) et steps (meta_done)
- 1.0.0 (2026-04-17): Tests unitaires step_meta
"""

import json
import pytest
from unittest.mock import patch, MagicMock

from database import BiDiDB
from steps.step_meta import run, _run_ytdlp, _extract_meta, _create_tasks, _is_unsupported_url_error


@pytest.fixture
def db(tmp_path):
    return BiDiDB(tmp_path / "test.db")


@pytest.fixture
def db_with_parsed_email(db):
    email_id = db.add_email("msg-meta-001", subject="Test meta", body_text="corps")
    db.update_email_parse(email_id, source_url="https://example.com/video", known_keywords=["pose"], unknown_keywords=[])
    db.advance_step(email_id, "parsed")
    return db, email_id


YTDLP_FULL = {
    "title": "Super Video",
    "description": "Une description",
    "uploader": "MonAuteur",
    "channel": "MaChaine",
    "extractor_key": "Youtube",
    "upload_date": "20260101",
    "duration": 120,
    "thumbnail": "https://i.ytimg.com/thumb.jpg",
    "tags": ["tag1", "tag2"],
    "chapters": [{"title": "Intro", "start_time": 0}],
    "view_count": 1000,
    "webpage_url": "https://example.com/video",
    "id": "abc123",
}

YTDLP_MINIMAL = {
    "title": "Minimal",
    "extractor_key": "Generic",
}


# ---------------------------------------------------------------------------
# _is_unsupported_url_error
# ---------------------------------------------------------------------------

class TestIsUnsupportedUrlError:
    def test_should_detect_unsupported_url(self):
        assert _is_unsupported_url_error("ERROR: Unsupported URL: https://example.com") is True

    def test_should_detect_unable_to_extract(self):
        assert _is_unsupported_url_error("ERROR: Unable to extract video data") is True

    def test_should_not_match_network_error(self):
        assert _is_unsupported_url_error("ERROR: Connection timeout") is False

    def test_should_not_match_empty_string(self):
        assert _is_unsupported_url_error("") is False


# ---------------------------------------------------------------------------
# _run_ytdlp
# ---------------------------------------------------------------------------

class TestRunYtdlp:
    def test_should_return_dict_on_success(self):
        fake_output = json.dumps({"title": "Test"})
        with patch("steps.step_meta.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout=fake_output, stderr="")
            result = _run_ytdlp("https://example.com/video")
        assert result["title"] == "Test"

    def test_should_raise_runtime_on_real_error(self):
        with patch("steps.step_meta.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="Connection timeout")
            with pytest.raises(RuntimeError, match="yt-dlp exit 1"):
                _run_ytdlp("https://example.com/video")

    def test_should_raise_value_error_on_unsupported_url(self):
        with patch("steps.step_meta.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=1, stdout="",
                stderr="ERROR: Unsupported URL: https://reddit.com/media?url=xxx"
            )
            with pytest.raises(ValueError, match="URL non supportée"):
                _run_ytdlp("https://reddit.com/media?url=xxx")

    def test_should_raise_on_timeout(self):
        import subprocess
        with patch("steps.step_meta.subprocess.run", side_effect=subprocess.TimeoutExpired("yt-dlp", 60)):
            with pytest.raises(subprocess.TimeoutExpired):
                _run_ytdlp("https://example.com/video", timeout=60)


# ---------------------------------------------------------------------------
# _extract_meta
# ---------------------------------------------------------------------------

class TestExtractMeta:
    def test_should_extract_all_fields(self):
        meta = _extract_meta(YTDLP_FULL)
        assert meta["title"] == "Super Video"
        assert meta["author"] == "MonAuteur"
        assert meta["platform"] == "youtube"
        assert meta["duration"] == "120"
        assert meta["remote_thumbnail"] == "https://i.ytimg.com/thumb.jpg"
        assert meta["tags"] == ["tag1", "tag2"]
        assert meta["chapters"][0]["title"] == "Intro"
        assert meta["meta_extra"]["view_count"] == 1000

    def test_should_handle_minimal_data(self):
        meta = _extract_meta(YTDLP_MINIMAL)
        assert meta["title"] == "Minimal"
        assert meta["author"] is None
        assert meta["duration"] is None
        assert meta["remote_thumbnail"] is None
        assert meta["tags"] == []

    def test_should_map_unknown_extractor(self):
        meta = _extract_meta(dict(YTDLP_MINIMAL, extractor_key="SomePlatform"))
        assert meta["platform"] == "someplatform"

    def test_should_map_empty_extractor_to_none(self):
        raw = dict(YTDLP_MINIMAL)
        raw.pop("extractor_key", None)
        assert _extract_meta(raw)["platform"] is None

    def test_should_truncate_description(self):
        raw = dict(YTDLP_MINIMAL, description="x" * 2000)
        assert len(_extract_meta(raw)["description"]) <= 1000

    def test_should_return_none_meta_extra_when_empty(self):
        assert _extract_meta(YTDLP_MINIMAL)["meta_extra"] is None


# ---------------------------------------------------------------------------
# _create_tasks
# ---------------------------------------------------------------------------

class TestCreateTasks:
    def test_should_create_primary_task(self, db):
        eid = db.add_email("msg-task-001")
        n = _create_tasks(db, eid, "https://example.com/video", {})
        assert n == 1
        assert db.get_download_tasks(eid)[0]["url_type"] == "primary"

    def test_should_create_thumbnail_task_when_present(self, db):
        eid = db.add_email("msg-task-002")
        n = _create_tasks(db, eid, "https://example.com/video", {"remote_thumbnail": "https://thumb.com/img.jpg"})
        assert n == 2
        types = {t["url_type"] for t in db.get_download_tasks(eid)}
        assert types == {"primary", "thumbnail"}

    def test_should_not_create_thumbnail_task_when_absent(self, db):
        eid = db.add_email("msg-task-003")
        assert _create_tasks(db, eid, "https://example.com/video", {"remote_thumbnail": None}) == 1


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------

class TestRun:
    def test_should_process_parsed_email(self, db_with_parsed_email):
        db, email_id = db_with_parsed_email
        with patch("steps.step_meta._run_ytdlp", return_value=YTDLP_FULL):
            stats = run(db, MagicMock())
        assert stats["done"] == 1
        assert stats["failed"] == 0
        assert stats["tasks_created"] == 2
        email = db.get_email(email_id)
        assert email["step"] == "meta_done"
        assert email["title"] == "Super Video"

    def test_should_advance_to_meta_done_on_unsupported_url(self, db_with_parsed_email):
        """URL non supportée par yt-dlp = comportement normal → meta_done/ok quand même."""
        db, email_id = db_with_parsed_email
        with patch("steps.step_meta._run_ytdlp", side_effect=ValueError("URL non supportée")):
            stats = run(db, MagicMock())
        assert stats["done"] == 1
        assert stats["failed"] == 0
        email = db.get_email(email_id)
        assert email["step"] == "meta_done"
        assert email["title"] is None  # pas de meta, mais pipeline avancé

    def test_should_create_task_even_without_meta(self, db_with_parsed_email):
        """Même sans meta yt-dlp, la task primaire doit être créée."""
        db, email_id = db_with_parsed_email
        with patch("steps.step_meta._run_ytdlp", side_effect=ValueError("URL non supportée")):
            run(db, MagicMock())
        tasks = db.get_download_tasks(email_id)
        assert len(tasks) == 1
        assert tasks[0]["url_type"] == "primary"

    def test_should_mark_failed_on_real_error(self, db_with_parsed_email):
        db, email_id = db_with_parsed_email
        with patch("steps.step_meta._run_ytdlp", side_effect=RuntimeError("Connection timeout")):
            stats = run(db, MagicMock())
        assert stats["failed"] == 1
        assert db.get_email(email_id)["step_status"] == "failed"

    def test_should_count_no_url_separately(self, db):
        email_id = db.add_email("msg-no-url")
        db.advance_step(email_id, "parsed")
        stats = run(db, MagicMock())
        assert stats["no_url"] == 1
        assert stats["failed"] == 0

    def test_should_return_empty_stats_on_no_emails(self, db):
        assert run(db, MagicMock()) == {"done": 0, "no_url": 0, "failed": 0, "tasks_created": 0}

    def test_should_process_multiple_emails(self, db):
        for i in range(3):
            eid = db.add_email(f"msg-multi-{i}")
            db.update_email_parse(eid, source_url=f"https://example.com/video{i}", known_keywords=[], unknown_keywords=[])
            db.advance_step(eid, "parsed")
        with patch("steps.step_meta._run_ytdlp", return_value=YTDLP_MINIMAL):
            stats = run(db, MagicMock())
        assert stats["done"] == 3
        assert stats["tasks_created"] == 3  # minimal = pas de thumbnail

    def test_should_skip_emails_not_in_parsed_ok(self, db):
        eid = db.add_email("msg-other-step")
        db.advance_step(eid, "meta_done")
        with patch("steps.step_meta._run_ytdlp", return_value=YTDLP_FULL):
            stats = run(db, MagicMock())
        assert stats["done"] == 0

    def test_should_create_download_tasks(self, db_with_parsed_email):
        db, email_id = db_with_parsed_email
        with patch("steps.step_meta._run_ytdlp", return_value=YTDLP_FULL):
            run(db, MagicMock())
        tasks = db.get_download_tasks(email_id)
        assert len(tasks) == 2
        assert all(t["status"] == "pending" for t in tasks)