"""
File: step_send.py
Path: steps/step_send.py

Version: 4.2.0
Date: 2026-04-26

Changelog:
- 4.2.0 (2026-04-26): FIX défense en profondeur — détection URL subreddit racine
  avant lancement Popen. Si task.url est un subreddit racine (ex: /r/tittyfuck/),
  la tâche est marquée failed immédiatement sans lancer run_task.py.
  Évite un téléchargement qui ne peut que échouer (gallery-dl ne peut pas
  télécharger un subreddit entier sans paramétrage explicite).
- 4.1.0 (2026-04-26): Correction _reap et boucle wait (tuples vs Popen simple).
  Suppression import run_task parasite. Ajout cwd=ROOT dans Popen.
  Log tâche dans _logs/task_{id}.log (chemin absolu).
- 4.0.0 (2026-04-22): Dispatcher JD pour pornhub/xhamster/xvideos.
  add_download() JD non-bloquant : stocke jd_package_name en DB.
  run_task.py lancé en Popen uniquement pour gallery-dl/yt-dlp/direct.
- 3.0.0 (2026-04-17): Bloquant multi-parallel.
- 2.1.0 (2026-04-21): count() + on_progress.
- 1.0.0 (2026-04-16): Version initiale.
"""

import logging
import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_LOGS_DIR = ROOT / "_logs"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config_manager import get_config
from database import BiDiDB

logger = logging.getLogger(__name__)

# Plateformes routées vers JDownloader
_JD_PLATFORMS = {"pornhub", "xhamster", "xvideos"}
_JD_URL_PATTERNS = ("pornhub.com", "xhamster.com", "xvideos.com")

# FIX : pattern de détection URL subreddit racine
# Cas produit quand step_parse laisse passer un short-link mal résolu
_SUBREDDIT_ROOT_RE = re.compile(
    r'^https?://(?:www\.)?reddit\.com/r/[^/]+/?$', re.IGNORECASE
)


def _is_subreddit_root(url: str) -> bool:
    """True si l'URL pointe sur un subreddit entier et non un post."""
    return bool(_SUBREDDIT_ROOT_RE.match(url))


def _choose_downloader(url: str, platform: str | None = None) -> str:
    """
    Retourne le downloader à utiliser selon la plateforme/URL.
    - JD : pornhub, xhamster, xvideos
    - yt-dlp : youtube, twitch
    - direct : fichiers statiques
    - gallery-dl : tout le reste (reddit, x.com, redgifs, ...)
    """
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
    """Retire du pool les process terminés. Pool = liste de Popen simples."""
    return [p for p in procs if p.poll() is None]


def _wait_for_slot(procs: list, max_par: int, poll_interval: float = 0.5) -> list:
    while True:
        procs = _reap(procs)
        if len(procs) < max_par:
            return procs
        time.sleep(poll_interval)


def count(db, cfg) -> int:
    n = len(db.get_tasks_by_status("pending"))
    logger.info(f"[send] count() → {n} task(s)")
    return n


def run(db: BiDiDB, cfg, on_progress=None) -> dict:
    # Nettoyage _tmp résiduel du run précédent (tâches orphelines)
    _tmp_dir = Path(cfg.get_save_dir()) / "_tmp"
    if _tmp_dir.exists():
        import shutil as _shutil
        for child in _tmp_dir.iterdir():
            try:
                _shutil.rmtree(child, ignore_errors=True)
                logger.info(f"[send] nettoyage résidu _tmp: {child.name}")
            except Exception:
                pass

    tasks = db.get_tasks_by_status("pending")
    run_task_script = ROOT / "run_task.py"

    limits = {
        "gallery-dl": cfg.get_gdl_max_parallel(),
        "yt-dlp": cfg.get_ytdlp_max_parallel(),
        "direct": cfg.get_gdl_max_parallel(),
        "jdownloader": cfg.get_jd_max_parallel(),
    }

    stats = {"launched": 0, "failed": 0, "tasks_sent": 0, "jd_sent": 0}
    pools: dict[str, list] = {"gallery-dl": [], "yt-dlp": [], "direct": []}

    for task in tasks:
        task_id = task["id"]
        email_id = task["email_id"]
        url = task["url"]

        email = db.get_email(email_id)
        if not email:
            logger.warning(f"task={task_id} skip — email #{email_id} introuvable")
            db.set_task_failed(task_id, "email introuvable")
            stats["failed"] += 1
            continue

        platform = email.get("platform")
        downloader = task.get("downloader") or _choose_downloader(url, platform)
        output_dir = _output_dir_for(email)

        db.set_task_output_dir(task_id, output_dir)
        with db._conn() as conn:
            conn.execute(
                "UPDATE download_tasks SET downloader=? WHERE id=?",
                (downloader, task_id),
            )

        # ── JDownloader : non-bloquant, pas de Popen ──────────────────────
        if downloader == "jdownloader":
            if not cfg.get_jd_enabled():
                logger.warning(f"task={task_id} JD désactivé, skip")
                db.set_task_failed(task_id, "JDownloader désactivé dans la config")
                stats["failed"] += 1
                continue
            try:
                from jd_client import add_download
                save_dir = Path(cfg.get_save_dir())
                dest_path = save_dir / output_dir
                pkg_name, pkg_uuid = add_download(cfg, url, dest_path)
                db.set_task_sent(task_id)
                db.set_task_jd_info(task_id, pkg_name, pkg_uuid)
                if email.get("step") == "meta_done":
                    db.advance_step(email_id, "download_sent")
                stats["jd_sent"] += 1
                stats["tasks_sent"] += 1
                logger.info(
                    f"task={task_id} email={email_id} "
                    f"→ JD pkg='{pkg_name}' dir={output_dir}"
                )
                if on_progress:
                    on_progress()
            except Exception as e:
                logger.error(f"task={task_id} JD échoué: {e}")
                db.set_task_failed(task_id, str(e))
                stats["failed"] += 1
            continue

        # ── gallery-dl / yt-dlp / direct : Popen ─────────────────────────

        # FIX : défense en profondeur — URL subreddit racine
        # Normalement filtré par step_parse (fix \r\n) et step_meta,
        # mais si ça arrive ici gallery-dl échouerait silencieusement.
        if downloader == "gallery-dl" and _is_subreddit_root(url):
            logger.error(
                f"task={task_id} email={email_id}: URL subreddit racine détectée, "
                f"impossible de télécharger → {url}"
            )
            db.set_task_failed(task_id, "URL subreddit racine — parse incorrect")
            stats["failed"] += 1
            if on_progress:
                on_progress()
            continue

        max_par = limits.get(downloader, 1)
        pool = pools.setdefault(downloader, [])
        pools[downloader] = _wait_for_slot(pool, max_par)

        db.set_task_sent(task_id)

        cmd = [sys.executable, str(run_task_script), "--task-id", str(task_id)]
        try:
            _LOGS_DIR.mkdir(parents=True, exist_ok=True)
            task_log = _LOGS_DIR / f"task_{task_id}.log"
            log_fh = open(task_log, "w", encoding="utf-8")
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
            pools[downloader].append(proc)
            logger.info(
                f"task={task_id} email={email_id} "
                f"→ {downloader} dir={output_dir} [pid={proc.pid}] log={task_log}"
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

    # ── Attendre fin de tous les Popen ────────────────────────────────────
    logger.info("Attente fin des téléchargements en cours...")
    for dl_name, pool in pools.items():
        for proc in pool:
            try:
                proc.wait(timeout=600)
                rc = proc.returncode
                if rc not in (0, None):
                    logger.warning(f"[{dl_name}] pid={proc.pid} retcode={rc}")
            except subprocess.TimeoutExpired:
                proc.kill()
                logger.warning(f"[{dl_name}] pid={proc.pid} killed (timeout 600s)")

    logger.info(f"step_send terminé — {stats}")
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cfg = get_config()
    db = BiDiDB(cfg.get_db_path())
    result = run(db, cfg)
    print(f"Résultat send: {result}")
