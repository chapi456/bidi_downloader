"""
File: step_meta.py
Path: steps/step_meta.py

Version: 3.2.0
Date: 2026-04-26

Changelog:
- 3.2.0 (2026-04-26): FIX timeout 30s — détection URL subreddit racine avant appel
  gallery-dl. Les URLs /r/subreddit/ (sans /comments/) sont skip-pées immédiatement
  (gdl essayait de scraper tout le subreddit → timeout systématique).
  Même garde ajoutée dans run() avec log ERROR explicite.
- 3.1.0 (2026-04-26): Fix critique — post_body/post_comments jamais sauvegardés en DB.
  db.set_meta_reddit() appelé systématiquement pour tout URL Reddit.
  post_body/post_comments extraits du résultat gdl avant set_meta_data()
  (set_meta_data ignore ces champs car absents de _ALLOWED).
  Fallback API JSON Reddit (/comments/{id}.json) si gdl retourne 0 items.
  gdl stderr logué en WARNING quand items==0 (était DEBUG, infos perdues).
  Fix typo : "postdate" → "post_date" dans extract_meta().
- 3.0.0 (2026-04-24): Fix typo advance_step 'metadone' → 'meta_done'.
  Reddit : remplace l'API JSON publique (bloquée) par
  gallery-dl --dump-json avec follow-redirects=true
  pour résoudre les short-links /s/XXXXX.
  Cookies Reddit injectés via config temporaire.
- 2.3.0 (2026-04-22): fetch_reddit_content via API JSON publique.
- 2.2.0 (2026-04-21): Logs create_tasks.
- 2.1.0 (2026-04-21): count() + on_progress callback.
- 1.1.0 (2026-04-17): URL non supportée → meta_done/ok, pas failed.
- 1.0.1 (2026-04-17): Fix noms steps + champs DB.
- 1.0.0 (2026-04-17): Création step_meta.
"""

import json
import logging
import re
import subprocess
import sys
import tempfile
import os
from pathlib import Path

if str(Path(__file__).resolve().parent.parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import BiDiDB
from config_manager import get_config

logger = logging.getLogger(__name__)

PLATFORM_MAP = {
    "youtube": "youtube", "pornhub": "pornhub",
    "reddit": "reddit", "twitter": "twitter",
    "x": "twitter", "instagram": "instagram",
    "xvideos": "xvideos", "xhamster": "xhamster",
    "redgifs": "redgifs",
}

KNOWN_UNSUPPORTED = [
    "Unsupported URL", "Unable to extract", "No video formats found",
]

# FIX : pattern de détection URL subreddit racine (sans /comments/)
# Exemples bloquants : https://www.reddit.com/r/tittyfuck/
# OK : https://www.reddit.com/r/tittyfuck/comments/abc123/...
_SUBREDDIT_ROOT_RE = re.compile(
    r'^https?://(?:www\.)?reddit\.com/r/[^/]+/?$', re.IGNORECASE
)


def _is_subreddit_root(url: str) -> bool:
    """True si l'URL pointe sur un subreddit entier, pas un post spécifique."""
    return bool(_SUBREDDIT_ROOT_RE.match(url))


def _is_unsupported_url_error(stderr: str) -> bool:
    return any(msg in stderr for msg in KNOWN_UNSUPPORTED)


def _is_reddit_url(url: str) -> bool:
    u = url.lower()
    return "reddit.com" in u or "redd.it" in u


# ── yt-dlp ────────────────────────────────────────────────────────────────────

def run_ytdlp(url: str, timeout: int = 60) -> dict:
    """Lance yt-dlp --dump-json. Retourne le dict ou lève une exception."""
    cmd = [sys.executable, "-m", "yt_dlp",
           "--dump-json", "--no-playlist", "--quiet", "--no-warnings", url]
    result = subprocess.run(
        cmd, capture_output=True, text=True,
        timeout=timeout, encoding="utf-8", errors="replace",
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        if _is_unsupported_url_error(stderr):
            raise ValueError(f"URL non supportée par yt-dlp: {stderr[:200]}")
        raise RuntimeError(f"yt-dlp exit {result.returncode}: {stderr[:500]}")
    return json.loads(result.stdout)


def extract_meta(raw: dict) -> dict:
    """Mappe les champs yt-dlp vers les champs DB."""
    extractor = (raw.get("extractor_key") or raw.get("extractor") or "").lower()
    platform = PLATFORM_MAP.get(extractor, extractor) or None
    tags = raw.get("tags") or []
    if isinstance(tags, str):
        tags = [tags]
    duration = raw.get("duration")
    return {
        "title": raw.get("title"),
        "description": (raw.get("description") or "")[:1000] or None,
        "author": raw.get("uploader") or raw.get("creator") or raw.get("channel"),
        "channel": raw.get("channel") or raw.get("uploader"),
        "platform": platform,
        "post_date": raw.get("upload_date"),
        "duration": str(int(duration)) if duration is not None else None,
        "remote_thumbnail": raw.get("thumbnail"),
        "tags": tags,
        "chapters": raw.get("chapters") or [],
        "meta_extra": {k: raw[k] for k in
                       ("view_count", "like_count", "webpage_url", "id")
                       if k in raw} or None,
    }


# ── gallery-dl dump-json pour Reddit ─────────────────────────────────────────

def _build_gdl_meta_config(cookies_path: str | None, tmpdir: str) -> str:
    """Écrit un gdl_meta_cfg.json minimal (follow-redirects + cookies). Retourne son chemin."""
    cfg: dict = {
        "extractor": {
            "reddit": {
                "comments": 10,
                "recursion": 0,
            },
            "follow-redirects": True,
        },
    }
    cfg_path = os.path.join(tmpdir, "gdl_meta_cfg.json")
    with open(cfg_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False)
    return cfg_path


def _run_gdl_dump_json(url: str, cookies_path: str | None = None,
                       timeout: int = 30) -> list[dict]:
    """
    Lance gallery-dl --dump-json sur url.
    Retourne la liste de dicts (peut être vide).
    """
    with tempfile.TemporaryDirectory(prefix="bidi_meta_") as tmpdir:
        cfg_path = _build_gdl_meta_config(cookies_path, tmpdir)
        cmd = [sys.executable, "-m", "gallery_dl",
               "--config", cfg_path, "--dump-json"]
        if cookies_path and Path(cookies_path).exists():
            cmd += ["--cookies", cookies_path]
            logger.info(f"meta gdl cookies: {cookies_path}")
        else:
            logger.warning(f"meta gdl: pas de cookies ({cookies_path!r})")
        cmd.append(url)
        logger.info(f"meta gdl dump-json: {url[:80]}")
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True,
                timeout=timeout, encoding="utf-8", errors="replace",
            )
        except subprocess.TimeoutExpired:
            logger.warning(f"meta gdl dump-json: timeout {timeout}s pour {url}")
            return []

        items = []
        for line in result.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, list) and len(obj) == 3:
                    items.append(obj[2])
                elif isinstance(obj, dict):
                    items.append(obj)
            except json.JSONDecodeError:
                pass

        logger.info(f"meta gdl dump-json: {len(items)} item(s)")

        if result.stderr.strip():
            log_level = logging.WARNING if not items else logging.DEBUG
            logger.log(log_level, f"meta gdl stderr: {result.stderr.strip()[:500]}")

        return items


def _extract_reddit_meta(items: list[dict]) -> dict:
    """
    Extrait title, author, body, comments, thumbnail depuis les items gallery-dl.
    gallery-dl Reddit : 1 item submission + N items comments.
    """
    submission = None
    comments = []
    thumbnail_url = None

    for item in items:
        subcategory = item.get("subcategory", "") or item.get("_type", "")
        if subcategory in ("submission", "post") or ("title" in item and submission is None):
            submission = item
            # Chercher thumbnail : champ direct d'abord
            for key in ("thumbnail", "url"):
                val = item.get(key)
                if val and isinstance(val, str) and val.startswith("http"):
                    thumbnail_url = val
                    break
            # Puis dans preview.images[0].source.url (format API Reddit)
            if not thumbnail_url:
                try:
                    preview = item.get("preview") or {}
                    if isinstance(preview, dict):
                        src = preview["images"][0]["source"]["url"]
                        if src and src.startswith("http"):
                            thumbnail_url = src.replace("&amp;", "&")
                except (KeyError, IndexError, TypeError):
                    pass
        elif subcategory == "comment" or ("body" in item and submission is not None):
            body = (item.get("body") or "").strip()
            if body and body not in ("[deleted]", "[removed]"):
                comments.append({
                    "author": item.get("author", ""),
                    "body": body[:500],
                    "score": item.get("score", 0),
                })

    if not submission:
        return {}

    selftext = (submission.get("selftext") or submission.get("body") or "").strip()
    return {
        "title": submission.get("title") or submission.get("_title"),
        "author": submission.get("author") or submission.get("_author"),
        "platform": "reddit",
        "channel": submission.get("subreddit") or "",
        "post_body": selftext if selftext not in ("", "[deleted]", "[removed]") else None,
        "post_comments": comments[:10],
        "remote_thumbnail": thumbnail_url,
    }


def _subreddit_from_url(url: str) -> str | None:
    """Extrait le nom du subreddit depuis l'URL Reddit."""
    m = re.search(r'reddit\.com/r/([^/\s?#]+)', url, re.IGNORECASE)
    return m.group(1).lower() if m else None


def _extract_reddit_id(url: str) -> str | None:
    """Extrait l'ID du post Reddit depuis une URL /comments/XXXXX."""
    m = re.search(r"/comments/([a-z0-9]+)", url, re.I)
    return m.group(1) if m else None


def _fetch_reddit_content_api(url: str, max_comments: int = 10) -> dict:
    """
    Fallback : récupère post_body + N commentaires via l'API JSON Reddit publique.
    Retourne {"post_body": str|None, "post_comments": list}.
    Utilisé quand gallery-dl retourne 0 items.
    """
    import urllib.request
    import urllib.error

    result: dict = {"post_body": None, "post_comments": []}
    post_id = _extract_reddit_id(url)
    if not post_id:
        logger.warning(f"meta API Reddit: impossible d'extraire l'ID depuis {url}")
        return result

    api_url = (
        f"https://www.reddit.com/comments/{post_id}.json"
        f"?limit={max_comments}&sort=top"
    )
    logger.info(f"meta API Reddit fallback: {api_url}")
    req = urllib.request.Request(
        api_url, headers={"User-Agent": "bidi_downloader/1.0"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        logger.warning(f"meta API Reddit: HTTP {e.code} — {api_url}")
        return result
    except Exception as e:
        logger.warning(f"meta API Reddit: erreur connexion — {e}")
        return result

    try:
        post_children = data[0]["data"]["children"]
        if post_children:
            post_data = post_children[0]["data"]
            selftext = (post_data.get("selftext") or "").strip()
            result["post_body"] = (
                selftext
                if selftext not in ("", "[removed]", "[deleted]")
                else None
            )
        comments = []
        for child in data[1]["data"]["children"][:max_comments]:
            c = child.get("data", {})
            body = (c.get("body") or "").strip()
            if body and body not in ("[deleted]", "[removed]"):
                comments.append({
                    "author": c.get("author", ""),
                    "body": body[:500],
                    "score": c.get("score", 0),
                })
        result["post_comments"] = comments
        logger.info(
            f"meta API Reddit: post_body={'oui' if result['post_body'] else 'non'} "
            f"comments={len(comments)}"
        )
    except (KeyError, IndexError, TypeError) as e:
        logger.warning(f"meta API Reddit: erreur parsing réponse — {e}")

    return result


def get_reddit_meta(url: str, cookies_path: str | None,
                    timeout: int = 30) -> dict:
    """Point d'entrée meta Reddit via gallery-dl dump-json."""
    # FIX : court-circuit pour les URLs subreddit racine
    # gallery-dl essaierait de scraper tout le subreddit → timeout 30s garanti
    if _is_subreddit_root(url):
        logger.warning(
            f"meta gdl skip: URL subreddit racine (pas un post), "
            f"gallery-dl non lancé → {url}"
        )
        return {}

    items = _run_gdl_dump_json(url, cookies_path=cookies_path, timeout=timeout)
    if not items:
        logger.warning(f"meta gdl vide pour {url[:60]} → meta Reddit incomplète")
        return {}
    return _extract_reddit_meta(items)


# ── Tasks DB ──────────────────────────────────────────────────────────────────

def create_tasks(db: BiDiDB, email_id: int, source_url: str, meta: dict) -> int:
    """Crée les download_tasks PRIMARY + THUMBNAIL. Retourne le nombre créé."""
    db.add_download_task(email_id, source_url, url_type="primary")
    logger.info(f"meta email={email_id} task PRIMARY url={source_url}")
    created = 1
    thumb_url = meta.get("remote_thumbnail")
    if thumb_url:
        db.add_download_task(email_id, thumb_url, url_type="thumbnail")
        logger.info(f"meta email={email_id} task THUMBNAIL url={thumb_url}")
        created += 1
    else:
        logger.info(f"meta email={email_id} pas de remote_thumbnail")
    return created


# ── count / run ───────────────────────────────────────────────────────────────

def count(db, cfg, *, retry_failed: bool = False) -> int:
    emails = db.get_emails_by_step("parsed", "ok")
    if retry_failed:
        emails += db.get_emails_by_step("parsed", "failed")
    n = len(emails)
    logger.info(f"meta count: {n} emails")
    return n


def run(db: BiDiDB, cfg, *, yt_dlp_timeout: int = 60,
        retry_failed: bool = False, on_progress=None) -> dict:
    """
    Traite tous les emails en step=parsed/ok.
    Reddit → gallery-dl dump-json (cookies injectés, follow-redirects)
    + fallback API JSON Reddit pour post_body/comments si gdl vide.
    Autres → yt-dlp dump-json.
    Les deux chemins appellent advance_step(id, 'meta_done') en fin de traitement.
    """
    cookies_path = cfg.get_reddit_cookies_path()

    emails = db.get_emails_by_step("parsed", "ok")
    if retry_failed:
        failed = db.get_emails_by_step("parsed", "failed")
        for e in failed:
            db.advance_step(e["id"], "parsed")
        emails = db.get_emails_by_step("parsed", "ok")

    stats = {"done": 0, "no_url": 0, "failed": 0, "tasks_created": 0}

    for i, email in enumerate(emails, 1):
        email_id = email["id"]
        source_url = email.get("source_url")

        if not source_url:
            db.mark_failed(email_id, "parsed", "source_url manquante")
            stats["no_url"] += 1
            logger.warning(f"meta {i}/{len(emails)}: email={email_id} pas de source_url")
            continue

        # FIX : détection préventive URL subreddit racine (défense en profondeur)
        # Normalement éliminé en amont par step_parse (fix \r\n), mais garde ici
        if _is_reddit_url(source_url) and _is_subreddit_root(source_url):
            logger.error(
                f"meta {i}/{len(emails)}: email={email_id} URL subreddit racine "
                f"détectée — impossible d'extraire la meta → {source_url}"
            )
            db.mark_failed(email_id, "parsed", "URL subreddit racine — parse incorrect")
            stats["failed"] += 1
            if on_progress:
                on_progress()
            continue

        meta: dict = {}
        post_body: str | None = None
        post_comments: list = []

        try:
            if _is_reddit_url(source_url):
                logger.info(f"meta {i}/{len(emails)}: email={email_id} Reddit → gallery-dl")
                meta = get_reddit_meta(source_url, cookies_path, timeout=30)

                post_body = meta.pop("post_body", None)
                post_comments = meta.pop("post_comments", None) or []
                logger.debug(
                    f"meta {i}: gdl → post_body={'oui' if post_body else 'non'} "
                    f"comments={len(post_comments)}"
                )

                if not meta.get("channel"):
                    sr = _subreddit_from_url(source_url)
                    if sr:
                        meta["channel"] = sr
                        meta.setdefault("platform", "reddit")
                        logger.info(f"meta {i}: fallback subreddit depuis URL → {sr!r}")

                if post_body is None and not post_comments:
                    logger.info(f"meta {i}: gdl sans contenu → tentative API JSON Reddit")
                    api_content = _fetch_reddit_content_api(source_url)
                    post_body = api_content.get("post_body")
                    post_comments = api_content.get("post_comments") or []

            else:
                logger.info(
                    f"meta {i}/{len(emails)}: email={email_id} "
                    f"yt-dlp → {source_url[:80]}"
                )
                try:
                    raw = run_ytdlp(source_url, timeout=yt_dlp_timeout)
                    meta = extract_meta(raw)
                    logger.info(f"meta {i}: ok title={meta.get('title')!r}")
                except ValueError as exc:
                    logger.info(f"meta {i}: URL non supportée, fallback step_send: {exc}")
                except Exception as exc:
                    db.mark_failed(email_id, "parsed", str(exc)[:500])
                    stats["failed"] += 1
                    logger.error(f"meta {i}: erreur yt-dlp: {exc}")
                    continue

        except Exception as exc:
            db.mark_failed(email_id, "parsed", str(exc)[:500])
            stats["failed"] += 1
            logger.error(f"meta {i}: erreur inattendue: {exc}")
            continue

        if meta:
            db.set_meta_data(email_id, **meta)

        if _is_reddit_url(source_url):
            db.set_meta_reddit(email_id, post_body, post_comments or None)
            logger.info(
                f"meta {i}: set_meta_reddit → "
                f"post_body={'oui' if post_body else 'non'} "
                f"comments={len(post_comments)}"
            )

        try:
            n = create_tasks(db, email_id, source_url, meta)
            stats["tasks_created"] += n
            db.advance_step(email_id, "meta_done")
            stats["done"] += 1
            logger.info(f"meta {i}: meta_done — {n} task(s)")
            if on_progress:
                on_progress()
        except Exception as exc:
            db.mark_failed(email_id, "parsed", f"DB error: {exc}")
            stats["failed"] += 1
            logger.error(f"meta {i}: erreur DB: {exc}")

    return stats


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s [%(name)s] %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--retry-failed", action="store_true")
    args = parser.parse_args()
    cfg = get_config()
    db = BiDiDB(cfg.get_db_path())
    stats = run(db, cfg, retry_failed=args.retry_failed)
    print(f"Résultat meta: {stats}")
