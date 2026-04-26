"""
File: database.py
Path: database.py

Version: 8.0.0
Date: 2026-04-22

Changelog:
- 8.0.0 (2026-04-22): Migrations v4 (+jd_package_name/uuid, +progress_pct sur
  download_tasks) et v5 (+post_body, +post_comments sur emails).
  Nouvelles méthodes : set_task_jd_info(), set_task_progress(),
  set_meta_reddit(). get_tasks_by_status() filter downloader déjà présent.
  set_parse_data() : source_url optionnel (None = pas de mise à jour URL).
- 7.0.1 (2026-04-21): Ajout update_media_file_path(media_id, new_path).
- 7.0.0 (2026-04-17): Migration v2→v3 : ajout colonne failed_step dans emails.
  mark_failed() renseigne failed_step automatiquement.
  get_emails_by_step() accepte step_status=None pour ignorer le filtre status.
  Ajout get_tasks_by_status(), set_task_sent(), set_task_done(), set_task_failed(),
  set_task_output_dir(), get_primary_media_file() (alias get_primary_file supprimé).
  set_meta_data() remplace update_email_meta().
- 6.0.0 (2026-04-17): Migration v1→v2 : ajout colonne output_dir dans download_tasks.
- 5.0.0 (2026-04-16): Refonte complète — schéma email-centrique, state machine par étape.
"""

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 3

STEPS = [
    "new",
    "parsed",
    "meta_done",
    "download_sent",
    "download_done",
    "thumb_done",
    "llm_done",
    "done",
]
STEP_SET = set(STEPS)

# ── DDL ───────────────────────────────────────────────────────────────────────

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS schema_version (
        version    INTEGER PRIMARY KEY,
        applied_at TEXT DEFAULT (datetime('now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS emails (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id  TEXT UNIQUE NOT NULL,
        subject     TEXT,
        sender      TEXT,
        received_at TEXT,
        body_text   TEXT,

        -- Parse step
        source_url        TEXT,
        known_keywords    TEXT,   -- JSON []
        unknown_keywords  TEXT,   -- JSON []

        -- Meta step
        title             TEXT,
        description       TEXT,
        author            TEXT,
        channel           TEXT,
        platform          TEXT,
        post_date         TEXT,
        duration          TEXT,
        remote_thumbnail  TEXT,
        tags              TEXT,   -- JSON []
        chapters          TEXT,   -- JSON []
        meta_extra        TEXT,   -- JSON {}

        -- LLM step
        llm_summary       TEXT,
        llm_prompt_image  TEXT,
        llm_prompt_video  TEXT,
        llm_params        TEXT,   -- JSON {}
        llm_workflow      TEXT,

        -- User data
        rating  INTEGER,
        notes   TEXT,

        -- State machine
        step         TEXT NOT NULL DEFAULT 'new',
        step_status  TEXT NOT NULL DEFAULT 'ok',
        step_error   TEXT,
        step_updated TEXT,
        failed_step  TEXT,         -- dernier step ayant appelé mark_failed()
        post_body    TEXT,         -- contenu Reddit selftext
        post_comments TEXT,        -- JSON : [{author, body, score}]

        created_at TEXT DEFAULT (datetime('now'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS download_tasks (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        email_id   INTEGER NOT NULL REFERENCES emails(id) ON DELETE CASCADE,
        url        TEXT NOT NULL,
        url_type   TEXT DEFAULT 'primary',   -- primary|secondary|thumbnail
        downloader TEXT,                      -- jdownloader|gallery-dl|yt-dlp|direct
        output_dir TEXT,                      -- relatif à save_dir
        status          TEXT DEFAULT 'pending',   -- pending|sent|done|failed
        error           TEXT,
        sent_at         TEXT,
        done_at         TEXT,
        created_at      TEXT DEFAULT (datetime('now')),
        jd_package_name TEXT,
        jd_package_uuid TEXT,
        progress_pct    INTEGER DEFAULT 0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS media_files (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        email_id   INTEGER NOT NULL REFERENCES emails(id) ON DELETE CASCADE,
        task_id    INTEGER REFERENCES download_tasks(id),
        file_path  TEXT NOT NULL,   -- relatif à save_dir
        file_type  TEXT,            -- video|image|thumbnail|audio|other
        file_size  INTEGER,
        is_primary INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    )
    """,
]

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_emails_step    ON emails(step, step_status)",
    "CREATE INDEX IF NOT EXISTS idx_emails_msg_id  ON emails(message_id)",
    "CREATE INDEX IF NOT EXISTS idx_dl_tasks_email  ON download_tasks(email_id)",
    "CREATE INDEX IF NOT EXISTS idx_dl_tasks_status ON download_tasks(status)",
    "CREATE INDEX IF NOT EXISTS idx_media_email     ON media_files(email_id)",
]

# ── Migrations ────────────────────────────────────────────────────────────────

def _migrate(conn: sqlite3.Connection, current: int) -> None:
    if current < 2:
        try:
            conn.execute("ALTER TABLE download_tasks ADD COLUMN output_dir TEXT")
            logger.info("[DB] Migration v2 : download_tasks.output_dir ajouté")
        except sqlite3.OperationalError:
            pass
        conn.execute("INSERT OR IGNORE INTO schema_version(version) VALUES (2)")

    if current < 3:
        try:
            conn.execute("ALTER TABLE emails ADD COLUMN failed_step TEXT")
            logger.info("[DB] Migration v3 : emails.failed_step ajouté")
        except sqlite3.OperationalError:
            pass
        conn.execute("INSERT OR IGNORE INTO schema_version(version) VALUES (3)")

    if current < 4:
        for col, typedef in [
            ("jd_package_name", "TEXT"),
            ("jd_package_uuid", "TEXT"),
            ("progress_pct",    "INTEGER DEFAULT 0"),
        ]:
            try:
                conn.execute(f"ALTER TABLE download_tasks ADD COLUMN {col} {typedef}")
                logger.info(f"[DB] Migration v4 : download_tasks.{col} ajouté")
            except sqlite3.OperationalError:
                pass
        conn.execute("INSERT OR IGNORE INTO schema_version(version) VALUES (4)")

    if current < 5:
        for col, typedef in [
            ("post_body",     "TEXT"),
            ("post_comments", "TEXT"),
        ]:
            try:
                conn.execute(f"ALTER TABLE emails ADD COLUMN {col} {typedef}")
                logger.info(f"[DB] Migration v5 : emails.{col} ajouté")
            except sqlite3.OperationalError:
                pass
        conn.execute("INSERT OR IGNORE INTO schema_version(version) VALUES (5)")

# ── Helpers ───────────────────────────────────────────────────────────────────

def _j(v: Any) -> Optional[str]:
    return None if v is None else json.dumps(v, ensure_ascii=False)

def _uj(v: Optional[str]) -> Any:
    if not v:
        return None
    try:
        return json.loads(v)
    except (json.JSONDecodeError, TypeError):
        return None

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ── Classe principale ─────────────────────────────────────────────────────────

class BiDiDB:
    """Base de données BiDi — schéma email-centrique, state machine par step."""

    def __init__(self, db_path: "Path | str"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    # ── Connexion ──────────────────────────────────────────────────────────

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ── Schéma + migrations ────────────────────────────────────────────────

    def _init_schema(self) -> None:
        with self._conn() as conn:
            for ddl in _DDL:
                conn.execute(ddl)
            for idx in _INDEXES:
                conn.execute(idx)

            row = conn.execute(
                "SELECT MAX(version) as v FROM schema_version"
            ).fetchone()
            current = row["v"] if row and row["v"] else 0

            if current == 0:
                conn.execute(
                    "INSERT INTO schema_version(version) VALUES (?)",
                    (SCHEMA_VERSION,),
                )
                logger.info(f"[DB] Schéma v{SCHEMA_VERSION} créé : {self.db_path}")
            elif current < SCHEMA_VERSION:
                _migrate(conn, current)
                logger.info(f"[DB] Migré v{current}→v{SCHEMA_VERSION} : {self.db_path}")
            else:
                logger.info(f"[DB] Schéma v{current} : {self.db_path}")

    # ── Emails : création ──────────────────────────────────────────────────

    def add_email(
        self,
        message_id: str,
        subject: Optional[str] = None,
        sender: Optional[str] = None,
        received_at: Optional[str] = None,
        body_text: Optional[str] = None,
    ) -> Optional[int]:
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO emails
                    (message_id, subject, sender, received_at, body_text,
                     step, step_status, step_updated)
                VALUES (?, ?, ?, ?, ?, 'new', 'ok', ?)
                """,
                (message_id, subject, sender, received_at, body_text, _now()),
            )
            return cur.lastrowid if cur.rowcount else None

    # ── Emails : lecture ───────────────────────────────────────────────────

    def get_email(self, email_id: int) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM emails WHERE id = ?", (email_id,)
            ).fetchone()
            return self._deser_email(dict(row)) if row else None

    def get_emails_by_step(
        self,
        step: str,
        step_status: Optional[str] = "ok",
        limit: int = 0,
    ) -> list:
        """
        Retourne les emails au step donné.
        step_status=None  → pas de filtre sur le status (tous : ok, running, failed).
        step_status="ok"  → défaut, uniquement les emails ok (comportement historique).
        """
        if step_status is None:
            sql = "SELECT * FROM emails WHERE step = ? ORDER BY created_at"
            params: list = [step]
        else:
            sql = "SELECT * FROM emails WHERE step = ? AND step_status = ? ORDER BY created_at"
            params = [step, step_status]
        if limit:
            sql += " LIMIT ?"
            params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [self._deser_email(dict(r)) for r in rows]

    def list_emails(
        self,
        step: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list:
        sql = "SELECT * FROM emails"
        params: list = []
        if step:
            sql += " WHERE step = ?"
            params.append(step)
        sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params += [limit, offset]
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [self._deser_email(dict(r)) for r in rows]

    def email_exists(self, message_id: str) -> bool:
        with self._conn() as conn:
            return conn.execute(
                "SELECT 1 FROM emails WHERE message_id = ?", (message_id,)
            ).fetchone() is not None

    # ── Emails : state machine ─────────────────────────────────────────────

    def set_step(
        self,
        email_id: int,
        step: str,
        status: str = "ok",
        error: Optional[str] = None,
    ) -> None:
        if step not in STEP_SET:
            raise ValueError(f"Step inconnu : {step!r}. Valeurs valides : {STEPS}")
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE emails
                SET step = ?, step_status = ?, step_error = ?, step_updated = ?
                WHERE id = ?
                """,
                (step, status, error, _now(), email_id),
            )

    def mark_running(self, email_id: int, step: str) -> None:
        """Réservé à step_send (opération non atomique). Ne pas utiliser ailleurs."""
        if step not in STEP_SET:
            raise ValueError(f"Step inconnu : {step!r}")
        with self._conn() as conn:
            conn.execute(
                "UPDATE emails SET step = ?, step_status = 'running', step_updated = ? WHERE id = ?",
                (step, _now(), email_id),
            )

    def mark_failed(self, email_id: int, step: str, error: str) -> None:
        """Passe l'email en failed et enregistre quel step a échoué."""
        if step not in STEP_SET:
            raise ValueError(f"Step inconnu : {step!r}")
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE emails
                SET step = ?, step_status = 'failed', step_error = ?,
                    failed_step = ?, step_updated = ?
                WHERE id = ?
                """,
                (step, error, step, _now(), email_id),
            )

    def advance_step(self, email_id: int, new_step: str) -> None:
        self.set_step(email_id, new_step, status="ok", error=None)

    # ── Emails : données ───────────────────────────────────────────────────

    def set_parse_data(
        self,
        email_id: int,
        url: Optional[str] = None,
        known_kws: Optional[list] = None,
        unknown_kws: Optional[list] = None,
        # aliases step_reparse
        source_url: Optional[str] = None,
        known_keywords: Optional[list] = None,
        unknown_keywords: Optional[list] = None,
    ) -> None:
        _url     = url     if url     is not None else source_url
        _known   = known_kws   if known_kws   is not None else known_keywords
        _unknown = unknown_kws if unknown_kws is not None else unknown_keywords
        with self._conn() as conn:
            if _url is not None:
                conn.execute("UPDATE emails SET source_url=? WHERE id=?", (_url, email_id))
            if _known is not None:
                conn.execute("UPDATE emails SET known_keywords=? WHERE id=?", (_j(_known), email_id))
            if _unknown is not None:
                conn.execute("UPDATE emails SET unknown_keywords=? WHERE id=?", (_j(_unknown), email_id))

    def set_meta_data(self, email_id: int, **kwargs) -> None:
        _JSON    = {"tags", "chapters", "meta_extra"}
        _ALLOWED = {
            "title", "description", "author", "channel", "platform", "post_date",
            "duration", "remote_thumbnail", "tags", "chapters", "meta_extra",
        }
        data = {k: (_j(v) if k in _JSON else v) for k, v in kwargs.items() if k in _ALLOWED}
        if not data:
            return
        sets = ", ".join(f"{k} = ?" for k in data)
        with self._conn() as conn:
            conn.execute(
                f"UPDATE emails SET {sets} WHERE id = ?",
                list(data.values()) + [email_id],
            )

    def update_email_llm(self, email_id: int, **kwargs) -> None:
        _JSON    = {"llm_params"}
        _ALLOWED = {"llm_summary", "llm_prompt_image", "llm_prompt_video",
                    "llm_params", "llm_workflow"}
        data = {k: (_j(v) if k in _JSON else v) for k, v in kwargs.items() if k in _ALLOWED}
        if not data:
            return
        sets = ", ".join(f"{k} = ?" for k in data)
        with self._conn() as conn:
            conn.execute(
                f"UPDATE emails SET {sets} WHERE id = ?",
                list(data.values()) + [email_id],
            )

    def set_rating(self, email_id: int, rating: int) -> None:
        with self._conn() as conn:
            conn.execute("UPDATE emails SET rating = ? WHERE id = ?", (rating, email_id))

    # ── Download tasks ─────────────────────────────────────────────────────

    def add_download_task(
        self,
        email_id: int,
        url: str,
        url_type: str = "primary",
        downloader: Optional[str] = None,
        output_dir: Optional[str] = None,
    ) -> int:
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO download_tasks (email_id, url, url_type, downloader, output_dir)
                VALUES (?, ?, ?, ?, ?)
                """,
                (email_id, url, url_type, downloader, output_dir),
            )
            return cur.lastrowid

    def get_download_tasks(self, email_id: int) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM download_tasks WHERE email_id = ? ORDER BY created_at",
                (email_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_tasks_by_status(self, status: str, downloader: Optional[str] = None) -> list:
        sql    = "SELECT * FROM download_tasks WHERE status = ?"
        params: list = [status]
        if downloader:
            sql += " AND downloader = ?"
            params.append(downloader)
        sql += " ORDER BY created_at"
        with self._conn() as conn:
            return [dict(r) for r in conn.execute(sql, params).fetchall()]

    def set_task_sent(self, task_id: int) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE download_tasks SET status = 'sent', sent_at = ? WHERE id = ?",
                (_now(), task_id),
            )

    def set_task_done(self, task_id: int) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE download_tasks SET status = 'done', done_at = ? WHERE id = ?",
                (_now(), task_id),
            )

    def set_task_failed(self, task_id: int, error: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE download_tasks SET status = 'failed', error = ? WHERE id = ?",
                (error, task_id),
            )


    def set_task_jd_info(self, task_id: int, pkg_name: str, pkg_uuid: str | None) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE download_tasks SET jd_package_name=?, jd_package_uuid=? WHERE id=?",
                (pkg_name, pkg_uuid, task_id),
            )

    def set_task_progress(self, task_id: int, pct: int) -> None:
        pct = max(0, min(100, int(pct)))
        with self._conn() as conn:
            conn.execute(
                "UPDATE download_tasks SET progress_pct=? WHERE id=?",
                (pct, task_id),
            )

    def set_meta_reddit(self, email_id: int,
                        post_body: str | None,
                        post_comments: list | None) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE emails SET post_body=?, post_comments=? WHERE id=?",
                (post_body, _j(post_comments), email_id),
            )


    def set_task_output_dir(self, task_id: int, output_dir: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE download_tasks SET output_dir = ? WHERE id = ?",
                (output_dir, task_id),
            )

    def reset_tasks(self, email_id: Optional[int] = None) -> int:
        """Remet en pending les tasks sent/failed sans media_files associés."""
        sql = """
            UPDATE download_tasks SET status = 'pending', error = NULL, sent_at = NULL
            WHERE status IN ('sent', 'failed')
            AND id NOT IN (SELECT DISTINCT task_id FROM media_files WHERE task_id IS NOT NULL)
        """
        params: list = []
        if email_id:
            sql += " AND email_id = ?"
            params.append(email_id)
        with self._conn() as conn:
            return conn.execute(sql, params).rowcount

    # ── Media files ────────────────────────────────────────────────────────

    def add_media_file(
        self,
        email_id: int,
        file_path: str,
        file_type: str = "other",
        task_id: Optional[int] = None,
        is_primary: bool = False,
        file_size: Optional[int] = None,
    ) -> int:
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO media_files
                    (email_id, task_id, file_path, file_type, file_size, is_primary)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (email_id, task_id, file_path, file_type, file_size, int(is_primary)),
            )
            return cur.lastrowid

    def update_media_file_path(self, media_id: int, new_path: str) -> None:
        """Met à jour file_path d'un media_file (ex: renommage thumbnail)."""
        with self._conn() as conn:
            conn.execute(
                "UPDATE media_files SET file_path = ? WHERE id = ?",
                (new_path, media_id),
            )

    def delete_media_file(self, media_id: int) -> None:
        """Supprime une entrée media_file par son id (ex: nettoyage stale thumb)."""
        with self._conn() as conn:
            conn.execute("DELETE FROM media_files WHERE id = ?", (media_id,))

    def get_media_files(self, email_id: int) -> list:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM media_files WHERE email_id = ? ORDER BY is_primary DESC, created_at",
                (email_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_primary_media_file(
        self, email_id: int, file_type: Optional[str] = None
    ) -> Optional[dict]:
        sql    = "SELECT * FROM media_files WHERE email_id = ? AND is_primary = 1"
        params: list = [email_id]
        if file_type:
            sql += " AND file_type = ?"
            params.append(file_type)
        sql += " LIMIT 1"
        with self._conn() as conn:
            row = conn.execute(sql, params).fetchone()
            return dict(row) if row else None

    def file_path_exists(self, email_id: int, file_path: str) -> bool:
        with self._conn() as conn:
            return conn.execute(
                "SELECT 1 FROM media_files WHERE email_id = ? AND file_path = ?",
                (email_id, file_path),
            ).fetchone() is not None

    # ── Stats ──────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT step, step_status, COUNT(*) as n FROM emails GROUP BY step, step_status"
            ).fetchall()
            step_counts: dict = {}
            for r in rows:
                step_counts.setdefault(r["step"], {})[r["step_status"]] = r["n"]

            return {
                "total_emails": conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0],
                "total_media_files": conn.execute("SELECT COUNT(*) FROM media_files").fetchone()[0],
                "total_download_tasks": conn.execute("SELECT COUNT(*) FROM download_tasks").fetchone()[0],
                "pending_download_tasks": conn.execute(
                    "SELECT COUNT(*) FROM download_tasks WHERE status IN ('pending', 'sent')"
                ).fetchone()[0],
                "steps": step_counts,
            }

    # ── Désérialisation ────────────────────────────────────────────────────

    @staticmethod
    def _deser_email(row: dict) -> dict:
        for field in {"known_keywords", "unknown_keywords", "tags",
                      "chapters", "meta_extra", "llm_params", "post_comments"}:
            if field in row:
                row[field] = _uj(row[field])
        return row
