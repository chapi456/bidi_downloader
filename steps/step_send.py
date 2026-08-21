"""
File: step_send.py
Path: steps/step_send.py

Version: 4.3.0
Date: 2026-08-05

Changelog:
- 4.3.0 (2026-08-05): FIX nettoyage _tmp robuste : rmtree ignore_errors +
  list() avant itération pour éviter RuntimeError. Nettoyage _tmp résiduel
  au démarrage de run(). FIX timeouts adaptés par downloader.
- 4.2.0 (2026-04-26): FIX détection URL subreddit racine avant Popen.
"""

import logging
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_LOGS_DIR = ROOT / "_logs"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config_manager import get_config
from database import BiDiDB, get_db

logger = logging.getLogger(__name__)

_JD_PLATFORMS    = {"pornhub", "xhamster", "xvideos"}
_JD_URL_PATTERNS = ("pornhub.com", "xhamster.com", "xvideos.com")

_SUBREDDIT_ROOT_RE = re.compile(
    r'^https?://(?:www\.)?reddit\.com/r/[^/]+/?$', re.IGNORECASE
)


def _is_subreddit_root(url: str) -> bool:
    return bool(_SUBREDDIT_ROOT_RE.match(url))


def _choose_downloader(url: str, platform: str | None = None) -> str:
    u = url.lower()
    if platform and platform.lower() in _JD_PLATFORMS:
        return "jdownloader"
    if any(s in u for s in _JD_URL_PATTERNS):
        return "jdownloader"
    if any(s in u for s in ("youtube.com", "youtu.be", "twitch.tv")):
        return "yt-dlp"
    if any(u.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif",
                                        ".webp", ".mp4", ".webm")):
        return "direct"
    return "gallery-dl"


def _output_dir_for(email: dict) -> str:
    import json as _json
    kws = email.get("known_keywords") or []
    if isinstance(kws, str):
        try:
            kws = _json.loads(kws)
        except Exception:
            kws = []
    return kws[0] if (isinstance(kws, list) and kws) else "download"


def _reap(procs: list) -> list:
    return [item for item in procs if item[0].poll() is None]


def _wait_for_slot(procs: list, max_par: int, poll_interval: float = 0.5) -> list:
    while True:
        procs = _reap(procs)
        if len(procs) < max_par:
            return procs
        time.sleep(poll_interval)


def _cleanup_tmp(tmp_dir: Path) -> None:
    """
    Nettoyage robuste des résidus _tmp d'un run précédent.
    - list() avant itération : évite RuntimeError si modification concurrente
    - ignore_errors=True : ne bloque jamais même si fichier verrouillé
    """
    if not tmp_dir.exists():
        return
    for child in list(tmp_dir.iterdir()):
        try:
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)
            logger.info(f"[send] nettoyage résidu _tmp: {child.name}")
        except Exception as e:
            logger.warning(f"[send] nettoyage _tmp/{child.name} échoué (ignoré): {e}")
    # Supprimer le répertoire lui-même s'il est vide
    try:
        if tmp_dir.exists() and not any(tmp_dir.iterdir()):
            tmp_dir.rmdir()
    except Exception:
        pass


def count(db, cfg) -> int:
    n = len(db.get_tasks_by_status("pending"))
    logger.info(f"[send] count() → {n} task(s)")
    return n


def run(db: BiDiDB, cfg, on_progress=None) -> dict:
    # FIX : vider _tmp résiduel avant de lancer les nouvelles tâches
    _tmp_dir = Path(cfg.get_save_dir()) / "_tmp"
    _cleanup_tmp(_tmp_dir)

    tasks          = db.get_tasks_by_status("pending")
    run_task_script = ROOT / "run_task.py"

    limits = {
        "gallery-dl":  cfg.get_gdl_max_parallel(),
        "yt-dlp":      cfg.get_ytdlp_max_parallel(),
        "direct":      cfg.get_gdl_max_parallel(),
        "jdownloader": cfg.get_jd_max_parallel(),
    }

    # FIX : timeouts adaptés par downloader (pas de hardcode 600s)
    _wait_timeouts = {
        "gallery-dl": cfg.get_gdl_timeout()   + 120,
        "yt-dlp":     cfg.get_ytdlp_timeout() + 120,
        "direct":     120,
    }

    stats = {"launched": 0, "failed": 0, "tasks_sent": 0, "jd_sent": 0}
    pools: dict[str, list] = {}

    for task in tasks:
        task_id  = task["id"]
        email_id = task["email_id"]
        url      = task["url"]

        email = db.get_email(email_id)
        if not email:
            logger.warning(f"task={task_id} skip — email #{email_id} introuvable")
            db.set_task_failed(task_id, "email introuvable")
            stats["failed"] += 1
            continue

        platform   = email.get("platform")
        downloader = task.get("downloader") or _choose_downloader(url, platform)
        output_dir = _output_dir_for(email)

        db.set_task_output_dir(task_id, output_dir)
        with db._conn() as conn:
            conn.execute(
                "UPDATE download_tasks SET downloader=? WHERE id=?",
                (downloader, task_id),
            )

        # ── JDownloader ───────────────────────────────────────────────────
        if downloader == "jdownloader":
            if not cfg.get_jd_enabled():
                logger.warning(f"task={task_id} JD désactivé, skip")
                db.set_task_failed(task_id, "JDownloader désactivé")
                stats["failed"] += 1
                continue
            try:
                from jd_client import add_download
                dest_path = Path(cfg.get_save_dir()) / output_dir
                pkg_name, pkg_uuid = add_download(cfg, url, dest_path)
                db.set_task_sent(task_id)
                db.set_task_jd_info(task_id, pkg_name, pkg_uuid)
                if email.get("step") == "meta_done":
                    db.advance_step(email_id, "download_sent")
                stats["jd_sent"] += 1
                stats["tasks_sent"] += 1
                logger.info(f"task={task_id} → JD pkg='{pkg_name}'")
                if on_progress:
                    on_progress()
            except Exception as e:
                logger.error(f"task={task_id} JD échoué: {e}")
                db.set_task_failed(task_id, str(e))
                stats["failed"] += 1
            continue

        # ── gallery-dl / yt-dlp / direct : Popen ─────────────────────────
        if downloader == "gallery-dl" and _is_subreddit_root(url):
            logger.error(f"task={task_id}: URL subreddit racine → skip")
            db.set_task_failed(task_id, "URL subreddit racine")
            stats["failed"] += 1
            if on_progress:
                on_progress()
            continue

        max_par = limits.get(downloader, 1)
        pool    = pools.setdefault(downloader, [])
        pools[downloader] = _wait_for_slot(pool, max_par)

        db.set_task_sent(task_id)

        cmd = [sys.executable, str(run_task_script), "--task-id", str(task_id)]
        try:
            _LOGS_DIR.mkdir(parents=True, exist_ok=True)
            task_log = _LOGS_DIR / f"task_{task_id}.log"
            log_fh   = open(task_log, "w", encoding="utf-8")
            _env = os.environ.copy()
            _env["PYTHONIOENCODING"] = "utf-8"
            proc = subprocess.Popen(
                cmd,
                stdout=log_fh,
                stderr=log_fh,
                cwd=str(ROOT),
                env=_env,
                close_fds=True,
            )
            log_fh.close()
            pools[downloader].append((proc, task_id, task_log))  # FIX : garder task_log pour diagnostic
            logger.info(
                f"task={task_id} email={email_id} "
                f"→ {downloader} dir={output_dir} url={url[:90]} [pid={proc.pid}]"
            )
            stats["launched"] += 1
            stats["tasks_sent"] += 1
            if on_progress:
                on_progress()
        except Exception as e:
            logger.error(f"task={task_id} Popen échoué: {e}")
            db.set_task_failed(task_id, str(e))
            stats["failed"] += 1
            continue

        if email.get("step") == "meta_done":
            db.advance_step(email_id, "download_sent")

    # ── Attente robuste des Popen ─────────────────────────────────────────
    logger.info("Attente fin des téléchargements...")
    for dl_name, pool in pools.items():
        wait_sec = _wait_timeouts.get(dl_name, 600)
        for proc, tid, task_log in pool:
            try:
                proc.wait(timeout=wait_sec)
                if proc.returncode not in (0, None):
                    logger.warning(f"[{dl_name}] task={tid} pid={proc.pid} retcode={proc.returncode}")
                    # FIX : remonter le vrai message d'erreur dans le log principal
                    # au lieu de forcer à aller ouvrir _logs/task_N.log manuellement.
                    try:
                        tail = task_log.read_text(encoding="utf-8", errors="replace").strip()
                        tail_lines = "\n".join(tail.splitlines()[-15:])
                        logger.warning(f"[{dl_name}] task={tid} détail échec :\n{tail_lines}")
                        # FIX : remonter la dernière ligne utile aux clients (web/CLI) via SSE
                        last_line = next((l for l in reversed(tail.splitlines()) if l.strip()), "?")
                        if on_progress:
                            try:
                                on_progress(f"task={tid} échec : {last_line[:120]}")
                            except TypeError:
                                on_progress()  # compat callback sans argument
                    except Exception as e:
                        logger.warning(f"[{dl_name}] task={tid} impossible de lire {task_log}: {e}")
            except subprocess.TimeoutExpired:
                proc.kill()
                logger.warning(f"[{dl_name}] task={tid} pid={proc.pid} killed (timeout {wait_sec}s)")

    logger.info(f"step_send terminé — {stats}")
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cfg = get_config()
    db  = get_db()
    print(run(db, cfg))