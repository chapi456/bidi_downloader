"""
File: step_fetch.py
Path: steps/step_fetch.py

Version: 5.2.0-diag
Date: 2026-04-25

Changelog:
- 5.2.0-diag (2026-04-25): Diagnostic complet : dump structure RFC822 + payload brut.
                            Fallback HTML si pas de text/plain (emails iPhone).
- 5.1.0 (2026-04-25): Fallback text/html → text/plain + logs parts.
- 3.1.0 (2026-04-21): Ajout count(db, cfg) + on_progress callback dans run().
- 5.0.0 (2026-04-16): Création — fetch IMAP → DB (step new)
"""

import email
import email.header
import email.message
import imaplib
import logging
import re
import ssl
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── Helpers décodage ──────────────────────────────────────────────────────────

def _decode_header(raw: Optional[str]) -> str:
    """Décode un header email (RFC 2047 : encodages Q/B)."""
    if not raw:
        return ""
    parts = email.header.decode_header(raw)
    decoded = []
    for chunk, charset in parts:
        if isinstance(chunk, bytes):
            try:
                decoded.append(chunk.decode(charset or "utf-8", errors="replace"))
            except (LookupError, UnicodeDecodeError):
                decoded.append(chunk.decode("latin-1", errors="replace"))
        else:
            decoded.append(chunk)
    return " ".join(decoded).strip()


def _html_to_text(html: str) -> str:
    """Conversion HTML→texte brut minimaliste (sans dépendance externe)."""
    text = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _dump_structure(msg: email.message.Message, prefix: str = "") -> None:
    """Log récursif de la structure complète du message (diagnostic)."""
    ct  = msg.get_content_type()
    cd  = str(msg.get("Content-Disposition") or "")
    cte = str(msg.get("Content-Transfer-Encoding") or "")
    logger.debug(f"[fetch] {prefix}PART ct={ct!r} cte={cte!r} cd={cd!r}")
    if msg.is_multipart():
        for i, part in enumerate(msg.get_payload()):
            _dump_structure(part, prefix=f"{prefix}  [{i}] ")
    else:
        raw = msg.get_payload(decode=True)
        if raw:
            charset = msg.get_content_charset() or "utf-8"
            try:
                preview = raw.decode(charset, errors="replace")[:120]
            except Exception:
                preview = repr(raw[:80])
            logger.debug(f"[fetch] {prefix}  payload({len(raw)}B): {preview!r}")
        else:
            raw_str = msg.get_payload()
            logger.debug(f"[fetch] {prefix}  payload=None / get_payload()={str(raw_str)[:80]!r}")


def _extract_body(msg: email.message.Message, subject: str = "") -> str:
    """Extrait le corps texte brut.

    Priorité : text/plain > text/html (fallback iPhone/Apple Mail) > \'\'.
    Dumpe la structure complète en DEBUG pour diagnostic.
    """
    logger.debug(f"[fetch] --- structure email {subject[:40]!r} ---")
    logger.debug(f"[fetch] is_multipart={msg.is_multipart()} content-type={msg.get_content_type()!r}")
    _dump_structure(msg)

    if msg.is_multipart():
        html_fallback: Optional[str] = None
        for part in msg.walk():
            ct  = part.get_content_type()
            cd  = str(part.get("Content-Disposition") or "")
            if "attachment" in cd:
                continue
            if ct == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    text = payload.decode(charset, errors="replace")
                    logger.debug(f"[fetch] → text/plain trouvé ({len(payload)}B)")
                    return text
            elif ct == "text/html" and html_fallback is None:
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    html_fallback = payload.decode(charset, errors="replace")
        if html_fallback:
            text = _html_to_text(html_fallback)
            logger.debug(f"[fetch] → fallback HTML→text ({len(html_fallback)}B): {text[:80]!r}")
            return text
        logger.warning(f"[fetch] _extract_body: aucun part text/* pour {subject[:40]!r}")
        return ""
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            text = payload.decode(charset, errors="replace")
            logger.debug(f"[fetch] → non-multipart payload ({len(payload)}B)")
            return text
        # Dernier recours : get_payload() sans decode
        raw_str = msg.get_payload()
        if isinstance(raw_str, str) and raw_str.strip():
            logger.warning(f"[fetch] → get_payload(decode=True) vide, fallback str: {raw_str[:80]!r}")
            return raw_str
        logger.warning(f"[fetch] → payload vide pour {subject[:40]!r}")
        return ""


def _parse_date(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return raw


# ── Connexion IMAP ────────────────────────────────────────────────────────────

def _make_ssl_context() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    if hasattr(ssl, "VERIFY_X509_STRICT"):
        ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
        logger.debug("SSL: VERIFY_X509_STRICT désactivé (Python 3.13 compat)")
    return ctx


def _connect(server: str, port: int, use_ssl: bool, user: str, password: str) -> imaplib.IMAP4:
    if use_ssl:
        conn = imaplib.IMAP4_SSL(server, port, ssl_context=_make_ssl_context())
    else:
        conn = imaplib.IMAP4(server, port)
    conn.login(user, password)
    return conn


def _fetch_ids(conn: imaplib.IMAP4, folder: str, max_emails: int) -> list[bytes]:
    status, _ = conn.select(folder, readonly=False)
    if status != "OK":
        raise RuntimeError(f"Impossible d'ouvrir le dossier IMAP : {folder!r}")
    status, data = conn.search(None, "UNSEEN")
    if status != "OK" or not data[0]:
        return []
    ids = data[0].split()
    ids = list(reversed(ids))
    return ids[:max_emails] if max_emails else ids


def _fetch_message(conn: imaplib.IMAP4, uid: bytes) -> Optional[email.message.Message]:
    status, data = conn.fetch(uid, "(RFC822)")
    if status != "OK" or not data or not data[0]:
        return None
    raw = data[0][1]
    logger.debug(f"[fetch] RFC822 brut : {len(raw)}B — aperçu: {raw[:200]!r}")
    return email.message_from_bytes(raw)


def count(db, cfg) -> int:
    n = cfg.get_imap_max()
    logger.info(f"[fetch] count() → {n} (max_emails config)")
    return n


def run(db, cfg, *, mark_as_read: bool = True, on_progress=None) -> dict:
    server     = cfg.get_imap_server()
    port       = cfg.get_imap_port()
    use_ssl    = cfg.get_imap_ssl()
    user       = cfg.get_imap_user()
    password   = cfg.get_imap_password()
    folder     = cfg.get_imap_folder()
    max_emails = cfg.get_imap_max()

    if not server or not user or not password:
        raise ValueError("Config IMAP incomplète (server / user / password manquants)")

    stats = {"fetched": 0, "new": 0, "duplicate": 0, "failed": 0}

    logger.info(f"[fetch] Connexion IMAP {user}@{server}:{port} (SSL={use_ssl})")
    conn = _connect(server, port, use_ssl, user, password)

    try:
        ids = _fetch_ids(conn, folder, max_emails)
        logger.info(f"[fetch] {len(ids)} messages non lus trouvés")

        for uid in ids:
            stats["fetched"] += 1
            try:
                msg = _fetch_message(conn, uid)
                if not msg:
                    stats["failed"] += 1
                    continue

                message_id = msg.get("Message-ID", "").strip()
                if not message_id:
                    import hashlib
                    raw_id = f"{msg.get('Date','')}{msg.get('From','')}{msg.get('Subject','')}"
                    message_id = f"<generated-{hashlib.sha1(raw_id.encode()).hexdigest()[:16]}>"

                subject     = _decode_header(msg.get("Subject"))
                sender      = _decode_header(msg.get("From"))
                received_at = _parse_date(msg.get("Date"))
                body_text   = _extract_body(msg, subject)

                email_id = db.add_email(
                    message_id=message_id,
                    subject=subject,
                    sender=sender,
                    received_at=received_at,
                    body_text=body_text,
                )
                if email_id is not None:
                    stats["new"] += 1
                    logger.info(f"[fetch] #{email_id} nouveau : {subject[:60]!r}")
                    if mark_as_read:
                        conn.store(uid, "+FLAGS", "\\Seen")
                else:
                    stats["duplicate"] += 1
                    logger.debug(f"[fetch] Doublon ignoré : {message_id}")

            except Exception as e:
                stats["failed"] += 1
                logger.error(f"[fetch] Erreur sur message {uid}: {e}", exc_info=True)
            if on_progress:
                on_progress()

    finally:
        try:
            conn.logout()
        except Exception:
            pass

    logger.info(
        f"[fetch] Terminé — fetched={stats['fetched']} "
        f"new={stats['new']} dup={stats['duplicate']} failed={stats['failed']}"
    )
    return stats


# ── Point d'entrée CLI ────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s [%(name)s] %(message)s")
    from config_manager import get_config
    from database import BiDiDB
    cfg = get_config()
    db = BiDiDB(cfg.get_db_path())
    result = run(db, cfg)
    print(result)
