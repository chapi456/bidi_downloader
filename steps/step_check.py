"""
File: step_check.py
Path: steps/step_check.py

Version: 5.0.0
Date: 2026-04-22

Changelog:
- 5.0.0 (2026-04-22): _poll_jd_tasks() — interroge JD pour toutes les tasks
  downloader=jdownloader/sent. Mise à jour progress_pct en DB.
  Finalisation automatique quand finished=True (fichiers → DB, advance_step).
- 4.0.0 (2026-04-17): Délai de grâce par downloader.
- 2.1.0 (2026-04-21): Ajout count(db, cfg) + on_progress callback dans run().
- 4.0.0 (2026-04-17): Délai de grâce par downloader avant de traiter les emails running.
  get_emails_by_step("download_sent") sans filtre status → voit ok ET running.
  Emails running récents → skip (dl en cours).
  Emails running trop vieux (stale) → rescan disque + avance ou fail.
- 3.0.0 (2026-04-17): Réécriture sur API DB v6 uniquement.
- 2.0.0 (2026-04-17): Utilisait get_sent_download_tasks/set_download_task_status (API inexistante).
- 1.0.0 (2026-04-16): Version initiale.
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config_manager import get_config
from database import BiDiDB

logger = logging.getLogger(__name__)

VIDEO_EXT = {".mp4", ".m4v", ".webm", ".mkv", ".mov", ".avi", ".ts", ".gif"}
IMAGE_EXT = {".jpg", ".jpeg", ".png", ".webp", ".avif"}
MEDIA_EXT = VIDEO_EXT | IMAGE_EXT

# Délai de grâce par downloader (secondes) avant de considérer un running comme stale
GRACE_SECONDS: dict[str, int] = {
    "gallery-dl":  300,   #  5 min
    "yt-dlp":      600,   # 10 min
    "direct":       60,   #  1 min
    "jdownloader": 3600,  # 60 min — JD peut être lent
    "default":     300,
}


def _classify(path: Path) -> str:
    return "video" if path.suffix.lower() in VIDEO_EXT else "image"


def _seconds_since(iso_str: str) -> float:
    try:
        dt = datetime.strptime(iso_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return 0.0


def _grace_for_tasks(tasks: list[dict], jd_timeout: int) -> int:
    """Retourne le délai de grâce le plus long parmi les downloaders des tasks."""
    graces = dict(GRACE_SECONDS)
    graces["jdownloader"] = max(graces["jdownloader"], jd_timeout)
    return max(
        (graces.get(t.get("downloader") or "default", graces["default"]) for t in tasks),
        default=graces["default"],
    )


def _is_stale(tasks: list[dict], jd_timeout: int) -> bool:
    """True si l'email running est assez vieux pour être considéré mort."""
    grace = _grace_for_tasks(tasks, jd_timeout)
    sent_ats = [t["sent_at"] for t in tasks if t.get("sent_at")]
    if not sent_ats:
        return True  # pas de sent_at → on ne sait pas depuis quand, on considère stale
    oldest = min(sent_ats)
    return _seconds_since(oldest) > grace


def _scan_dir(directory: Path) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(
        f for f in directory.rglob("*")
        if f.is_file() and f.suffix.lower() in MEDIA_EXT
    )


def _register_files(
    db: BiDiDB,
    email_id: int,
    task_id: int,
    files: list[Path],
    save_dir: Path,
    is_thumb: bool = False,
) -> int:
    existing = {mf["file_path"] for mf in db.get_media_files(email_id)}
    added = 0
    for i, fpath in enumerate(files):
        try:
            rel = str(fpath.relative_to(save_dir))
        except ValueError:
            rel = str(fpath)
        if rel in existing:
            continue
        ftype      = "thumbnail" if is_thumb else _classify(fpath)
        is_primary = (not is_thumb) and (i == 0) and (added == 0)
        db.add_media_file(
            email_id=email_id,
            task_id=task_id,
            file_path=rel,
            file_type=ftype,
            file_size=fpath.stat().st_size if fpath.exists() else None,
            is_primary=is_primary,
        )
        added += 1
    return added


def _try_advance_email(db: BiDiDB, email_id: int) -> bool:
    """Avance l'email à download_done si toutes les tasks primaires sont terminales."""
    tasks   = db.get_download_tasks(email_id)
    primary = [t for t in tasks if t.get("url_type") == "primary"]
    if not primary:
        return False
    terminal = {"done", "failed"}
    if not all(t["status"] in terminal for t in primary):
        return False
    email = db.get_email(email_id)
    if not email or email["step"] not in ("download_sent", "meta_done"):
        return False
    if any(t["status"] == "done" for t in primary):
        db.advance_step(email_id, "download_done")
        if on_progress: on_progress()
        logger.info(f"email={email_id} → download_done")
        return True
    else:
        db.mark_failed(email_id, "download_sent", "toutes les tasks primaires ont échoué")
        return False



def count(db, cfg) -> int:
    """Retourne le nombre d'emails à vérifier (download_sent, tous statuts)."""
    n = len(db.get_emails_by_step("download_sent", step_status=None))
    logger.info(f"[check] count() → {n} email(s)")
    return n

def _poll_jd_tasks(db: BiDiDB, cfg, save_dir: Path, stats: dict, on_progress) -> None:
    """
    Interroge JDownloader pour toutes les tasks sent avec downloader=jdownloader.
    Met à jour progress_pct en DB. Finalise les tasks terminées.
    """
    jd_tasks = [
        t for t in db.get_tasks_by_status("sent", downloader="jdownloader")
        if t.get("jd_package_name")
    ]
    if not jd_tasks:
        return

    try:
        from jd_client import get_package_progress
    except ImportError:
        logger.warning("[check] jd_client non disponible")
        return

    logger.info(f"[check] poll JD — {len(jd_tasks)} task(s) JD active(s)")

    for task in jd_tasks:
        task_id  = task["id"]
        email_id = task["email_id"]
        pkg_name = task["jd_package_name"]

        try:
            prog = get_package_progress(cfg, pkg_name)
        except Exception as e:
            logger.warning(f"[check] JD poll task={task_id}: {e}")
            continue

        if not prog.get("found"):
            # Package introuvable — stale ?
            sent_at = task.get("sent_at") or ""
            grace   = GRACE_SECONDS.get("jdownloader", 3600)
            if sent_at and _seconds_since(sent_at) > grace:
                db.set_task_failed(task_id, "JD package introuvable après grace")
                logger.warning(f"task={task_id} JD package absent → failed")
                stats["timed_out"] = stats.get("timed_out", 0) + 1
            continue

        pct = prog.get("pct", 0)
        db.set_task_progress(task_id, pct)
        logger.info(
            f"task={task_id} JD {pct}% "
            f"({prog.get('loaded_mb', 0):.0f}/{prog.get('total_mb', 0):.0f} Mo)"
        )

        if not prog.get("finished"):
            continue

        # ── Terminé — enregistrer les fichiers ────────────────────────────
        save_to   = prog.get("save_to")
        file_names = prog.get("files", [])
        email     = db.get_email(email_id)
        if not email:
            continue

        output_dir = task.get("output_dir") or "download"
        search_dir = Path(save_to) if save_to else (save_dir / output_dir)
        found_files: list[Path] = []

        if file_names:
            for name in file_names:
                p = search_dir / name
                if p.exists():
                    found_files.append(p)
        if not found_files:
            # fallback scan
            found_files = _scan_dir(search_dir)

        if found_files:
            is_thumb = task.get("url_type") == "thumbnail"
            _register_files(db, email_id, task_id, found_files, save_dir, is_thumb)
            db.set_task_done(task_id)
            logger.info(f"task={task_id} JD finished — {len(found_files)} fichier(s)")
            stats["rescanned"] = stats.get("rescanned", 0) + 1
            if _try_advance_email(db, email_id):
                stats["emails_done"] = stats.get("emails_done", 0) + 1
                if on_progress:
                    on_progress()
        else:
            db.set_task_failed(task_id, "JD finished mais aucun fichier trouvé")
            logger.warning(f"task={task_id} JD finished mais aucun fichier")


def run(db: BiDiDB, cfg, on_progress=None) -> dict:
    save_dir   = Path(cfg.get_save_dir())
    jd_timeout = cfg.get_jd_timeout()

    stats = {
        "checked":     0,
        "confirmed":   0,   # emails ok avec fichiers déjà en DB
        "skipped":     0,   # emails running trop récents
        "rescanned":   0,   # tasks récupérées par rescan disque
        "not_found":   0,
        "timed_out":   0,
        "emails_done": 0,
    }

    # ── Poll JD tasks actives ─────────────────────────────────────────────
    if cfg.get_jd_enabled():
        _poll_jd_tasks(db, cfg, save_dir, stats, on_progress)

    # ── Cas 1+2 : tous les emails download_sent (ok ET running) ───────────
    for email in db.get_emails_by_step("download_sent", step_status=None):
        email_id    = email["id"]
        step_status = email["step_status"]
        stats["checked"] += 1

        # Cas 1 — ok : run_task a tout géré, fichiers déjà en DB
        if step_status == "ok":
            files = db.get_media_files(email_id)
            if files:
                db.advance_step(email_id, "download_done")
                if on_progress: on_progress()
                stats["confirmed"] += 1
                stats["emails_done"] += 1
                logger.info(f"email={email_id}: {len(files)} fichier(s) → download_done")
            else:
                db.mark_failed(
                    email_id, "download_sent",
                    "aucun fichier enregistré (run_task n'a rien produit)"
                )
                logger.warning(f"email={email_id}: aucun fichier en base")
            continue

        # Cas 2 — running : délai de grâce avant de toucher
        tasks = db.get_download_tasks(email_id)
        if not _is_stale(tasks, jd_timeout):
            logger.debug(f"email={email_id}: running récent — skip")
            stats["skipped"] += 1
            continue

        logger.info(f"email={email_id}: running stale — rescan disque")

        # Rescan de toutes les tasks sent/running de cet email
        email_needs_advance = False
        for task in tasks:
            if task["status"] not in ("sent", "pending"):
                continue
            task_id    = task["id"]
            output_dir = task.get("output_dir") or "download"
            downloader = task.get("downloader") or ""
            search_dir = save_dir / output_dir
            files      = _scan_dir(search_dir)

            if files:
                is_thumb = task.get("url_type") == "thumbnail"
                added    = _register_files(db, email_id, task_id, files, save_dir, is_thumb)
                db.set_task_done(task_id)
                logger.info(f"task={task_id} [{downloader}] rescan: {added} fichier(s)")
                stats["rescanned"] += 1
            else:
                # Pas de fichier et stale → timeout de la task
                db.set_task_failed(task_id, f"stale sans fichier (grace dépassée)")
                logger.warning(f"task={task_id} [{downloader}] stale → failed")
                stats["timed_out"] += 1

            email_needs_advance = True

        if email_needs_advance and _try_advance_email(db, email_id):
            stats["emails_done"] += 1

    # ── Cas 3 : tasks 'sent' orphelines hors emails download_sent ──────────
    # (email en meta_done/running si step_send crashé avant advance_step)
    sent_tasks = db.get_tasks_by_status("sent")
    orphan_tasks = [
        t for t in sent_tasks
        if db.get_email(t["email_id"]) and
        db.get_email(t["email_id"]).get("step") != "download_sent"
    ]

    if orphan_tasks:
        logger.info(f"check: {len(orphan_tasks)} task(s) orpheline(s) hors download_sent")
        email_ids: set[int] = set()

        for task in orphan_tasks:
            task_id    = task["id"]
            email_id   = task["email_id"]
            output_dir = task.get("output_dir") or "download"
            downloader = task.get("downloader") or ""
            sent_at    = task.get("sent_at") or ""
            grace      = GRACE_SECONDS.get(downloader, GRACE_SECONDS["default"])

            if sent_at and _seconds_since(sent_at) <= grace:
                logger.debug(f"task={task_id} orphan récente — skip")
                continue

            search_dir = save_dir / output_dir
            files      = _scan_dir(search_dir)

            if files:
                is_thumb = task.get("url_type") == "thumbnail"
                added    = _register_files(db, email_id, task_id, files, save_dir, is_thumb)
                db.set_task_done(task_id)
                logger.info(f"task={task_id} [orphan/{downloader}] récupéré — {added} fichier(s)")
                stats["rescanned"] += 1
            else:
                db.set_task_failed(task_id, "orphan stale sans fichier")
                stats["timed_out"] += 1

            email_ids.add(email_id)

        for email_id in email_ids:
            if _try_advance_email(db, email_id):
                stats["emails_done"] += 1

    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    cfg  = get_config()
    db   = BiDiDB(cfg.get_db_path())
    result = run(db, cfg)
    print(f"Résultat check: {result}")
