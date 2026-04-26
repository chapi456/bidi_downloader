"""
File: step_parse.py
Path: steps/step_parse.py

Version: 2.3.0
Date: 2026-04-26

Changelog:
- 2.3.0 (2026-04-26): FIX normalisation body avant parse — remplace \r\n par
  espace au lieu de le supprimer, évitant la concaténation URL+tag suivant
  (ex: "5Bbcmdd40C\r\nTitshot" → url correcte "5Bbcmdd40C").
- 2.2.0 (2026-04-25): Résolution des short-links Reddit /s/ via cookies à la parse.
  Source unique de vérité : l'URL résolue est stockée en DB.
  step_meta et run_task utilisent source_url sans modification.
- 2.1.0 (2026-04-21): Ajout count(db, cfg) et on_progress callback dans run.
- 1.0.0 (2026-04-16): Création step parse new→parsed
"""
import http.cookiejar
import logging
import sys
import urllib.request
from pathlib import Path

if str(Path(__file__).resolve().parent.parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config_manager import get_config
from database import BiDiDB
from email_parser import parse_email_body

logger = logging.getLogger(__name__)


def _resolve_reddit_short_url(url: str) -> str:
    """Résout un short-link Reddit (/r/.../s/XXXXX) vers l'URL complète du post.

    Utilise le fichier cookies Reddit configuré pour suivre la redirection.
    Appelé une seule fois depuis step_parse — résultat stocké en DB.
    """
    if not url or "reddit.com" not in url or "/s/" not in url:
        return url

    cfg = get_config()
    cookies_path = cfg.get_reddit_cookies()

    cj = http.cookiejar.MozillaCookieJar()
    if cookies_path and Path(cookies_path).exists():
        try:
            cj.load(str(cookies_path), ignore_discard=True, ignore_expires=True)
            logger.info(f"[parse] cookies OK : {len(list(cj))} cookie(s) depuis {cookies_path!r}")
        except Exception as e:
            logger.warning(f"[parse] cookies non chargés ({cookies_path!r}) : {e}")
    elif cookies_path:
        logger.warning(f"[parse] fichier cookies introuvable : {cookies_path!r}")
    else:
        logger.warning("[parse] aucun cookies_path configuré")

    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    opener.addheaders = [(
        "User-Agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36",
    )]
    try:
        with opener.open(url, timeout=15) as resp:
            resolved = resp.url
            if resolved and resolved != url:
                logger.info(f"[parse] resolve {url!r} → {resolved!r}")
            return resolved
        return url
    except Exception as e:
        logger.warning(f"[parse] impossible de résoudre {url!r} : {e}")
        return url


def count(db, cfg) -> int:
    n = len(db.get_emails_by_step('new', 'ok'))
    logger.info(f"[parse] count() → {n} email(s)")
    return n


def run(db: BiDiDB, keywords: list[str], on_progress=None) -> dict:
    emails = db.get_emails_by_step('new', 'ok')
    stats = {'parsed': 0, 'no_url': 0, 'errors': 0}
    for email in emails:
        email_id = email['id']
        try:
            db.mark_running(email_id, 'new')
            body = email.get('body_text') or email.get('bodytext') or ''

            # FIX : normaliser les fins de ligne en espaces
            # Sans ce replace, "5Bbcmdd40C\r\nTitshot" donnait l'URL "5Bbcmdd40CTitshot"
            # (le \r\n était supprimé, collant le tag suivant à l'ID du short-link)
            body = body.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ')

            logger.debug(f"[parse] #{email_id} body({len(body)}c): {body[:120]!r}")
            result = parse_email_body(body, keywords)
            logger.debug(f"[parse] #{email_id} extract → url={result.url!r}")

            # Résolution short-link ici, une seule fois, stockée en DB
            url = _resolve_reddit_short_url(result.url) if result.url else result.url

            db.set_parse_data(
                email_id,
                url=url,
                known_kws=result.known_kws,
                unknown_kws=result.unknown_kws,
            )

            if result.has_url:
                db.advance_step(email_id, 'parsed')
                stats['parsed'] += 1
                logger.info(f"[parse] #{email_id} parsed → {url!r}")
            else:
                db.advance_step(email_id, 'done')
                stats['no_url'] += 1
                logger.info(f"[parse] #{email_id} no URL → done")

            if on_progress:
                on_progress()

        except Exception as exc:
            db.mark_failed(email_id, 'new', str(exc))
            stats['errors'] += 1
            logger.error(f"[parse] #{email_id} erreur {exc}")

    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(name)s] %(message)s")
    cfg = get_config()
    db = BiDiDB(cfg.get_db_path())
    stats = run(db, cfg.get_keywords())
    print(f"Résultat parse : {stats}")
