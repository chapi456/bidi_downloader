"""
File: test_step_fetch.py
Path: tests/test_step_fetch.py

Version: 5.0.0
Date: 2026-04-16

Changelog:
- 5.0.0 (2026-04-16): Création — tests step_fetch avec IMAP mocké
"""

import email as email_lib
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import BiDiDB
from steps.step_fetch import _decode_header, _extract_body, _parse_date, run


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def db(tmp_path):
    return BiDiDB(tmp_path / "test.db")


@pytest.fixture
def cfg():
    mock = MagicMock()
    mock.get_imap_server.return_value   = "imap.example.com"
    mock.get_imap_port.return_value     = 993
    mock.get_imap_ssl.return_value      = True
    mock.get_imap_user.return_value     = "user@example.com"
    mock.get_imap_password.return_value = "secret"
    mock.get_imap_folder.return_value   = "INBOX"
    mock.get_imap_max.return_value      = 50
    return mock


def _make_raw_email(
    message_id="<test-001@example.com>",
    subject="Test subject",
    sender="alice@example.com",
    date="Thu, 16 Apr 2026 10:00:00 +0000",
    body="Corps du mail.",
):
    msg = email_lib.message.Message()
    msg["Message-ID"] = message_id
    msg["Subject"]    = subject
    msg["From"]       = sender
    msg["Date"]       = date
    msg.set_payload(body, charset="utf-8")
    return msg.as_bytes()


def _mock_imap(messages, unseen_ids=None):
    conn = MagicMock()
    conn.select.return_value = ("OK", [b"1"])

    if unseen_ids is None:
        unseen_ids = [m[0] for m in messages]

    conn.search.return_value = ("OK", [b" ".join(unseen_ids)])

    def _fetch_side(uid, fmt):
        for msg_uid, raw in messages:
            if msg_uid == uid:
                return ("OK", [(b"RFC822 data", raw)])
        return ("NO", [None])

    conn.fetch.side_effect = _fetch_side
    conn.store.return_value  = ("OK", [])
    conn.logout.return_value = ("BYE", [])
    return conn


# ── Tests helpers ─────────────────────────────────────────────────────────────

class TestDecodeHeader:
    def test_should_decode_plain_ascii(self):
        assert _decode_header("Hello World") == "Hello World"

    def test_should_return_empty_on_none(self):
        assert _decode_header(None) == ""

    def test_should_decode_utf8_encoded(self):
        encoded = "=?utf-8?b?VGVzdCDDqWzDqG1lbnQ=?="
        result = _decode_header(encoded)
        assert "Test" in result

    def test_should_decode_q_encoding(self):
        encoded = "=?iso-8859-1?Q?Pr=E9nom?="
        result = _decode_header(encoded)
        assert "Pr" in result


class TestExtractBody:
    def test_should_extract_simple_text(self):
        msg = email_lib.message_from_string("Subject: test\n\nCeci est le corps.")
        assert "corps" in _extract_body(msg)

    def test_should_extract_from_multipart(self):
        raw = (
            "MIME-Version: 1.0\n"
            'Content-Type: multipart/mixed; boundary="boundary"\n\n'
            "--boundary\n"
            "Content-Type: text/plain; charset=utf-8\n\n"
            "Texte plain.\n"
            "--boundary\n"
            "Content-Type: text/html\n\n"
            "<html>HTML content</html>\n"
            "--boundary--\n"
        )
        msg = email_lib.message_from_string(raw)
        body = _extract_body(msg)
        assert "Texte plain" in body

    def test_should_return_empty_on_no_text_part(self):
        raw = (
            "MIME-Version: 1.0\n"
            'Content-Type: multipart/mixed; boundary="b"\n\n'
            "--b\n"
            "Content-Type: application/octet-stream\n\n"
            "binary data\n"
            "--b--\n"
        )
        msg = email_lib.message_from_string(raw)
        assert _extract_body(msg) == ""


class TestParseDate:
    def test_should_parse_valid_date(self):
        result = _parse_date("Thu, 16 Apr 2026 10:00:00 +0000")
        assert result == "2026-04-16 10:00:00"

    def test_should_handle_timezone_offset(self):
        result = _parse_date("Thu, 16 Apr 2026 12:00:00 +0200")
        assert result == "2026-04-16 10:00:00"

    def test_should_return_none_on_none(self):
        assert _parse_date(None) is None

    def test_should_return_raw_on_invalid(self):
        result = _parse_date("pas une date")
        assert result == "pas une date"


# ── Tests run() ───────────────────────────────────────────────────────────────

class TestFetchRun:
    def test_should_raise_on_missing_config(self, db):
        cfg = MagicMock()
        cfg.get_imap_server.return_value   = ""
        cfg.get_imap_user.return_value     = ""
        cfg.get_imap_password.return_value = ""
        with pytest.raises(ValueError, match="incomplète"):
            run(db, cfg)

    def test_should_fetch_and_insert_email(self, db, cfg):
        raw  = _make_raw_email("<msg-001@test.com>", "Sujet test", "alice@test.com")
        conn = _mock_imap([(b"1", raw)])

        with patch("steps.step_fetch._connect", return_value=conn):
            stats = run(db, cfg, mark_as_read=False)

        assert stats == {"fetched": 1, "new": 1, "duplicate": 0, "failed": 0}
        emails = db.list_emails()
        assert len(emails) == 1
        assert emails[0]["subject"] == "Sujet test"
        assert emails[0]["sender"]  == "alice@test.com"
        assert emails[0]["step"]    == "new"

    def test_should_not_insert_duplicate(self, db, cfg):
        raw = _make_raw_email("<msg-dup@test.com>", "Doublon")

        with patch("steps.step_fetch._connect", return_value=_mock_imap([(b"1", raw)])):
            s1 = run(db, cfg, mark_as_read=False)
        with patch("steps.step_fetch._connect", return_value=_mock_imap([(b"1", raw)])):
            s2 = run(db, cfg, mark_as_read=False)

        assert s1["new"] == 1
        assert s2["new"] == 0 and s2["duplicate"] == 1
        assert len(db.list_emails()) == 1

    def test_should_fetch_multiple_emails(self, db, cfg):
        messages = [
            (b"1", _make_raw_email("<a@test.com>", "Email A")),
            (b"2", _make_raw_email("<b@test.com>", "Email B")),
            (b"3", _make_raw_email("<c@test.com>", "Email C")),
        ]
        with patch("steps.step_fetch._connect", return_value=_mock_imap(messages)):
            stats = run(db, cfg, mark_as_read=False)

        assert stats["new"] == 3
        assert len(db.list_emails()) == 3

    def test_should_mark_as_read_when_enabled(self, db, cfg):
        raw  = _make_raw_email("<msg-read@test.com>", "Mark read")
        conn = _mock_imap([(b"42", raw)])

        with patch("steps.step_fetch._connect", return_value=conn):
            run(db, cfg, mark_as_read=True)

        conn.store.assert_called_once_with(b"42", "+FLAGS", "\\Seen")

    def test_should_not_mark_as_read_for_duplicates(self, db, cfg):
        raw = _make_raw_email("<msg-nomark@test.com>", "No mark")

        conn1 = _mock_imap([(b"1", raw)])
        with patch("steps.step_fetch._connect", return_value=conn1):
            run(db, cfg, mark_as_read=True)
        assert conn1.store.call_count == 1

        conn2 = _mock_imap([(b"1", raw)])
        with patch("steps.step_fetch._connect", return_value=conn2):
            run(db, cfg, mark_as_read=True)
        conn2.store.assert_not_called()

    def test_should_respect_max_emails_limit(self, db, cfg):
        cfg.get_imap_max.return_value = 2
        messages = [
            (b"1", _make_raw_email("<x1@t.com>", "X1")),
            (b"2", _make_raw_email("<x2@t.com>", "X2")),
            (b"3", _make_raw_email("<x3@t.com>", "X3")),
        ]
        with patch("steps.step_fetch._connect", return_value=_mock_imap(messages)):
            stats = run(db, cfg, mark_as_read=False)

        assert stats["fetched"] == 2
        assert stats["new"]     == 2

    def test_should_handle_empty_inbox(self, db, cfg):
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"0"])
        conn.search.return_value = ("OK", [b""])
        conn.logout.return_value = ("BYE", [])

        with patch("steps.step_fetch._connect", return_value=conn):
            stats = run(db, cfg, mark_as_read=False)

        assert stats["fetched"] == 0 and stats["new"] == 0

    def test_should_generate_id_for_email_without_message_id(self, db, cfg):
        raw_str = (
            "From: sender@test.com\n"
            "Subject: Sans ID\n"
            "Date: Thu, 16 Apr 2026 10:00:00 +0000\n\n"
            "Corps."
        )
        conn = _mock_imap([(b"1", raw_str.encode())])
        with patch("steps.step_fetch._connect", return_value=conn):
            stats = run(db, cfg, mark_as_read=False)

        assert stats["new"] == 1
        assert db.list_emails()[0]["message_id"].startswith("<generated-")

    def test_should_logout_even_on_error(self, db, cfg):
        conn = MagicMock()
        conn.select.return_value = ("OK", [b"1"])
        conn.search.side_effect  = RuntimeError("IMAP error")
        conn.logout.return_value = ("BYE", [])

        with patch("steps.step_fetch._connect", return_value=conn):
            with pytest.raises(RuntimeError):
                run(db, cfg)

        conn.logout.assert_called_once()

    def test_should_store_body_text(self, db, cfg):
        raw = _make_raw_email("<body@t.com>", "Body", body="Contenu important.")
        with patch("steps.step_fetch._connect", return_value=_mock_imap([(b"1", raw)])):
            run(db, cfg, mark_as_read=False)

        assert "Contenu important" in db.list_emails()[0]["body_text"]

    def test_should_store_received_at_in_utc(self, db, cfg):
        raw = _make_raw_email("<date@t.com>", "Date", date="Thu, 16 Apr 2026 14:00:00 +0200")
        with patch("steps.step_fetch._connect", return_value=_mock_imap([(b"1", raw)])):
            run(db, cfg, mark_as_read=False)

        assert db.list_emails()[0]["received_at"] == "2026-04-16 12:00:00"
