"""
File: test_database.py
Path: tests/test_database.py

Version: 5.0.0
Date: 2026-04-16

Changelog:
- 5.0.0 (2026-04-16): Création — tests unitaires BiDiDB
"""

import pytest
from pathlib import Path
from database import BiDiDB, STEPS, SCHEMA_VERSION


@pytest.fixture
def db(tmp_path):
    return BiDiDB(tmp_path / "test.db")


# ── Schéma ────────────────────────────────────────────────────────────────────

class TestSchema:
    def test_should_create_tables_on_init(self, db):
        stats = db.get_stats()
        assert stats["total_emails"] == 0
        assert stats["total_media_files"] == 0
        assert stats["total_download_tasks"] == 0

    def test_should_be_idempotent_on_reopen(self, db, tmp_path):
        db2 = BiDiDB(tmp_path / "test.db")
        assert db2.get_stats()["total_emails"] == 0

    def test_should_create_parent_dirs(self, tmp_path):
        deep = tmp_path / "a" / "b" / "c" / "bidi.db"
        db = BiDiDB(deep)
        assert deep.exists()


# ── Emails : création ─────────────────────────────────────────────────────────

class TestEmailCreation:
    def test_should_return_id_on_add(self, db):
        email_id = db.add_email("msg-001", subject="Test")
        assert email_id is not None
        assert email_id > 0

    def test_should_return_none_on_duplicate(self, db):
        db.add_email("msg-dup")
        result = db.add_email("msg-dup")
        assert result is None

    def test_should_not_count_duplicates(self, db):
        db.add_email("msg-x")
        db.add_email("msg-x")
        assert db.get_stats()["total_emails"] == 1

    def test_should_store_all_fields(self, db):
        eid = db.add_email(
            "msg-full",
            subject="Sujet test",
            sender="alice@example.com",
            received_at="2026-04-16 10:00:00",
            body_text="Corps de l'email",
        )
        e = db.get_email(eid)
        assert e["subject"] == "Sujet test"
        assert e["sender"] == "alice@example.com"
        assert e["received_at"] == "2026-04-16 10:00:00"
        assert e["body_text"] == "Corps de l'email"

    def test_should_start_at_step_new(self, db):
        eid = db.add_email("msg-new")
        e = db.get_email(eid)
        assert e["step"] == "new"
        assert e["step_status"] == "ok"

    def test_should_detect_existing_message_id(self, db):
        db.add_email("msg-exists")
        assert db.email_exists("msg-exists") is True
        assert db.email_exists("msg-ghost") is False


# ── Emails : lecture ──────────────────────────────────────────────────────────

class TestEmailRead:
    def test_should_return_none_for_unknown_id(self, db):
        assert db.get_email(9999) is None

    def test_should_list_emails(self, db):
        db.add_email("msg-a")
        db.add_email("msg-b")
        assert len(db.list_emails()) == 2

    def test_should_respect_limit(self, db):
        for i in range(10):
            db.add_email(f"msg-{i}")
        assert len(db.list_emails(limit=3)) == 3

    def test_should_filter_by_step(self, db):
        id1 = db.add_email("msg-filter-1")
        db.add_email("msg-filter-2")
        db.set_step(id1, "parsed")
        result = db.list_emails(step="parsed")
        assert len(result) == 1
        assert result[0]["step"] == "parsed"


# ── State machine ─────────────────────────────────────────────────────────────

class TestStateMachine:
    def test_should_advance_step(self, db):
        eid = db.add_email("msg-sm-1")
        db.set_step(eid, "parsed")
        assert db.get_email(eid)["step"] == "parsed"
        assert db.get_email(eid)["step_status"] == "ok"

    def test_should_mark_failed(self, db):
        eid = db.add_email("msg-sm-2")
        db.mark_failed(eid, "parsed", "URL introuvable")
        e = db.get_email(eid)
        assert e["step"] == "parsed"
        assert e["step_status"] == "failed"
        assert e["step_error"] == "URL introuvable"

    def test_should_mark_running(self, db):
        eid = db.add_email("msg-sm-3")
        db.mark_running(eid, "meta_done")
        e = db.get_email(eid)
        assert e["step"] == "meta_done"
        assert e["step_status"] == "running"

    def test_should_raise_on_invalid_step(self, db):
        eid = db.add_email("msg-sm-4")
        with pytest.raises(ValueError, match="Step inconnu"):
            db.set_step(eid, "inexistant")

    def test_should_accept_all_defined_steps(self, db):
        eid = db.add_email("msg-sm-all")
        for step in STEPS:
            db.set_step(eid, step)
            assert db.get_email(eid)["step"] == step

    def test_should_clear_error_on_ok(self, db):
        eid = db.add_email("msg-sm-clear")
        db.mark_failed(eid, "parsed", "Erreur temporaire")
        db.set_step(eid, "parsed", status="ok")
        assert db.get_email(eid)["step_error"] is None

    def test_should_get_emails_by_step_and_status(self, db):
        id1 = db.add_email("msg-by-step-1")
        id2 = db.add_email("msg-by-step-2")
        id3 = db.add_email("msg-by-step-3")
        db.set_step(id1, "parsed")
        db.set_step(id2, "parsed")
        db.mark_failed(id3, "parsed", "err")

        ok_parsed = db.get_emails_by_step("parsed", step_status="ok")
        assert len(ok_parsed) == 2
        failed_parsed = db.get_emails_by_step("parsed", step_status="failed")
        assert len(failed_parsed) == 1

    def test_should_update_step_updated_timestamp(self, db):
        eid = db.add_email("msg-ts")
        db.set_step(eid, "parsed")
        e = db.get_email(eid)
        assert e["step_updated"] is not None


# ── Données email ─────────────────────────────────────────────────────────────

class TestEmailData:
    def test_should_store_parse_data(self, db):
        eid = db.add_email("msg-parse")
        db.update_email_parse(
            eid,
            source_url="https://example.com/video",
            known_keywords=["kw1", "kw2"],
            unknown_keywords=["unk"],
        )
        e = db.get_email(eid)
        assert e["source_url"] == "https://example.com/video"
        assert e["known_keywords"] == ["kw1", "kw2"]
        assert e["unknown_keywords"] == ["unk"]

    def test_should_store_meta_data_with_json_fields(self, db):
        eid = db.add_email("msg-meta")
        db.update_email_meta(
            eid,
            title="Super vidéo",
            platform="pornhub",
            tags=["tag1", "tag2"],
            chapters=[{"title": "Intro", "startTime": 0}, {"title": "Scène 1", "startTime": 60}],
            meta_extra={"score": 42},
        )
        e = db.get_email(eid)
        assert e["title"] == "Super vidéo"
        assert e["platform"] == "pornhub"
        assert e["tags"] == ["tag1", "tag2"]
        assert e["chapters"][1]["startTime"] == 60
        assert e["meta_extra"]["score"] == 42

    def test_should_store_llm_data(self, db):
        eid = db.add_email("msg-llm")
        db.update_email_llm(
            eid,
            llm_summary="Résumé généré",
            llm_prompt_image="Un prompt image",
            llm_params={"temperature": 0.7},
        )
        e = db.get_email(eid)
        assert e["llm_summary"] == "Résumé généré"
        assert e["llm_params"]["temperature"] == 0.7

    def test_should_set_rating(self, db):
        eid = db.add_email("msg-rating")
        db.set_rating(eid, 4)
        assert db.get_email(eid)["rating"] == 4

    def test_should_ignore_unknown_meta_keys(self, db):
        eid = db.add_email("msg-meta-unknown")
        db.update_email_meta(eid, champ_inexistant="valeur", title="OK")
        assert db.get_email(eid)["title"] == "OK"


# ── Download tasks ────────────────────────────────────────────────────────────

class TestDownloadTasks:
    def test_should_add_task(self, db):
        eid = db.add_email("msg-task-1")
        tid = db.add_download_task(eid, "https://example.com/video.mp4")
        tasks = db.get_download_tasks(eid)
        assert len(tasks) == 1
        assert tasks[0]["url"] == "https://example.com/video.mp4"
        assert tasks[0]["status"] == "pending"

    def test_should_add_multiple_tasks(self, db):
        eid = db.add_email("msg-task-2")
        db.add_download_task(eid, "https://example.com/img1.jpg", url_type="secondary")
        db.add_download_task(eid, "https://example.com/img2.jpg", url_type="secondary")
        assert len(db.get_download_tasks(eid)) == 2

    def test_should_transition_to_sent(self, db):
        eid = db.add_email("msg-task-sent")
        tid = db.add_download_task(eid, "https://x.com")
        db.set_download_task_status(tid, "sent")
        t = db.get_download_tasks(eid)[0]
        assert t["status"] == "sent"
        assert t["sent_at"] is not None
        assert t["done_at"] is None

    def test_should_transition_to_done(self, db):
        eid = db.add_email("msg-task-done")
        tid = db.add_download_task(eid, "https://x.com")
        db.set_download_task_status(tid, "done")
        t = db.get_download_tasks(eid)[0]
        assert t["status"] == "done"
        assert t["done_at"] is not None

    def test_should_record_error(self, db):
        eid = db.add_email("msg-task-err")
        tid = db.add_download_task(eid, "https://x.com")
        db.set_download_task_status(tid, "failed", error="Timeout")
        t = db.get_download_tasks(eid)[0]
        assert t["status"] == "failed"
        assert t["error"] == "Timeout"

    def test_should_get_pending_tasks(self, db):
        id1 = db.add_email("msg-pending-1")
        id2 = db.add_email("msg-pending-2")
        t1 = db.add_download_task(id1, "https://a.com")
        db.add_download_task(id2, "https://b.com")
        db.set_download_task_status(t1, "done")
        pending = db.get_pending_download_tasks()
        assert len(pending) == 1
        assert pending[0]["url"] == "https://b.com"

    def test_should_filter_pending_by_downloader(self, db):
        eid = db.add_email("msg-dl-filter")
        db.add_download_task(eid, "https://a.com", downloader="jdownloader")
        db.add_download_task(eid, "https://b.com", downloader="gallery-dl")
        jd_pending = db.get_pending_download_tasks(downloader="jdownloader")
        assert len(jd_pending) == 1
        assert jd_pending[0]["downloader"] == "jdownloader"


# ── Media files ───────────────────────────────────────────────────────────────

class TestMediaFiles:
    def test_should_add_media_file(self, db):
        eid = db.add_email("msg-media-1")
        fid = db.add_media_file(eid, "default/video.mp4", "video", is_primary=True)
        assert fid is not None
        files = db.get_media_files(eid)
        assert len(files) == 1
        assert files[0]["file_path"] == "default/video.mp4"
        assert files[0]["is_primary"] == 1

    def test_should_list_primary_first(self, db):
        eid = db.add_email("msg-media-2")
        db.add_media_file(eid, "default/thumb.jpg", "thumbnail")
        db.add_media_file(eid, "default/video.mp4", "video", is_primary=True)
        files = db.get_media_files(eid)
        assert files[0]["file_type"] == "video"

    def test_should_get_primary_file_by_type(self, db):
        eid = db.add_email("msg-media-3")
        db.add_media_file(eid, "default/thumb.jpg", "thumbnail", is_primary=True)
        db.add_media_file(eid, "default/video.mp4", "video", is_primary=True)
        primary_video = db.get_primary_file(eid, "video")
        assert primary_video["file_path"] == "default/video.mp4"

    def test_should_return_none_if_no_primary(self, db):
        eid = db.add_email("msg-media-4")
        db.add_media_file(eid, "default/video.mp4", "video")
        assert db.get_primary_file(eid) is None

    def test_should_cascade_delete_on_email_removal(self, db):
        eid = db.add_email("msg-cascade")
        db.add_media_file(eid, "f.mp4", "video")
        with db._conn() as conn:
            conn.execute("DELETE FROM emails WHERE id = ?", (eid,))
        assert db.get_media_files(eid) == []

    def test_should_detect_existing_file_path(self, db):
        eid = db.add_email("msg-exists-file")
        db.add_media_file(eid, "saves/video.mp4", "video")
        assert db.file_path_exists(eid, "saves/video.mp4") is True
        assert db.file_path_exists(eid, "saves/autre.mp4") is False


# ── Stats ─────────────────────────────────────────────────────────────────────

class TestStats:
    def test_should_return_zeros_on_empty_db(self, db):
        s = db.get_stats()
        assert s["total_emails"] == 0
        assert s["total_media_files"] == 0
        assert s["total_download_tasks"] == 0
        assert s["pending_download_tasks"] == 0
        assert s["steps"] == {}

    def test_should_count_correctly(self, db):
        id1 = db.add_email("msg-stats-1")
        id2 = db.add_email("msg-stats-2")
        db.set_step(id1, "parsed")
        db.mark_failed(id2, "meta_done", "err")
        db.add_media_file(id1, "f.mp4", "video")
        db.add_download_task(id2, "https://x.com")

        s = db.get_stats()
        assert s["total_emails"] == 2
        assert s["total_media_files"] == 1
        assert s["total_download_tasks"] == 1
        assert s["pending_download_tasks"] == 1
        assert s["steps"]["parsed"]["ok"] == 1
        assert s["steps"]["meta_done"]["failed"] == 1
