"""
File: run_task.py
Path: run_task.py

Version: 4.0.0
Date: 2026-04-22

Changelog:
- 4.0.0 (2026-04-22): Capture stderr/stdout ligne-par-ligne gallery-dl/yt-dlp.
  Extraction % progression → progress_cb(task_id, pct) + set_task_progress en DB.
  run_task() accepte progress_cb optionnel.
- 3.2.0 (2026-04-21): Logs _collect_media (catégorie + nom de chaque fichier) et _register (is_primary + is_thumb_pattern).
- 3.1.0 (2026-04-21): _move_files_to_dest : si le fichier existe déjà à destination, skip (pas de renommage _1). Cohérent avec gallery-dl skip=True.
- 3.0.0 (2026-04-20): Retour au schéma original : téléchargement dans _tmp/task_X/,
  move vers save_dir/<premier_keyword>/ (ou save_dir/download/),
  hardlinks vers les autres keywords. Suppression de platform dans le chemin.
  Logs détaillés à chaque étape. --force pour relancer done/failed.
- 2.1.0 (2026-04-20): Ajout main() + CLI --task-id.
- 2.0.0 (2026-04-18): Config gallery-dl consolidée.
- 1.0.0 (2026-04-16): Version initiale.
"""

import json
import logging
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config_manager import get_config
from database import BiDiDB

logger = logging.getLogger(__name__)

VIDEO_EXT = {".mp4", ".m4v", ".webm", ".mkv", ".mov", ".avi", ".ts", ".gif"}
IMAGE_EXT = {".jpg", ".jpeg", ".png", ".webp", ".avif"}
MEDIA_EXT = VIDEO_EXT | IMAGE_EXT

_THUMB_PATTERNS = (
    re.compile(r"^external[-_]preview", re.IGNORECASE),
    re.compile(r"\.thumb\.", re.IGNORECASE),
    re.compile(r"_preview\.", re.IGNORECASE),
)


# ── Config gallery-dl ─────────────────────────────────────────────────────────

def _build_gdl_config(cookies_path: Optional[str] = None) -> dict:
    cfg = {
        "extractor": {
            "skip": True,
            "reddit": {
                "filename": "{id}_{num:>02}_{subreddit}.{extension}",
                "videos": True,
                "previews": True,
                "external": True,
                "comments": 0,
                "recursion": 0,
            },
        }
    }
# cookies passés via --cookies CLI, pas dans le JSON
    return cfg


def _write_gdl_config(task_id: int, cookies_path: Optional[str]) -> Path:
    cfg_path = ROOT / "_tmp" / f"gdl_cfg_{task_id}.json"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        json.dumps(_build_gdl_config(cookies_path), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return cfg_path


# ── Helpers fichiers ──────────────────────────────────────────────────────────

def _is_thumb_file(path: Path) -> bool:
    return any(p.search(path.name) for p in _THUMB_PATTERNS)


def _classify(path: Path) -> str:
    return "video" if path.suffix.lower() in VIDEO_EXT else "image"


def _collect_media(directory: Path) -> tuple[list[Path], list[Path]]:
    """Retourne (media_files, thumb_files) en parcourant récursivement directory."""
    media, thumbs = [], []
    for f in sorted(directory.rglob("*")):
        if not f.is_file() or f.suffix.lower() not in MEDIA_EXT:
            continue
        is_thumb = _is_thumb_file(f)
        cat = "THUMB" if is_thumb else _classify(f).upper()
        logger.info(f"[collect] {cat:10} | {f.name}")
        if is_thumb:
            thumbs.append(f)
        else:
            media.append(f)
    return media, thumbs


def _detect_platform(url: str) -> str:
    u = url.lower()
    if "reddit.com" in u or "redd.it" in u:
        return "reddit"
    if "redgifs.com" in u:
        return "redgifs"
    if "pornhub.com" in u:
        return "pornhub"
    if "twitter.com" in u or "x.com" in u:
        return "twitter"
    return "misc"


# ── Répertoire cible ──────────────────────────────────────────────────────────

def _primary_dest(save_dir: Path, known_keywords: list[str]) -> Path:
    """
    Répertoire de destination principal :
      save_dir/<premier_keyword>/   si keywords
      save_dir/download/            sinon
    """
    kw = known_keywords[0] if known_keywords else "download"
    return save_dir / kw


def _move_files_to_dest(files: list[Path], dest: Path) -> list[Path]:
    """
    Déplace chaque fichier de tmp vers dest/.
    Si le fichier existe déjà à destination → skip (cohérent avec gallery-dl skip=True).
    Retourne la liste des paths finaux (existants inclus).
    """
    dest.mkdir(parents=True, exist_ok=True)
    result = []
    for src in files:
        target = dest / src.name
        if target.exists():
            # Déjà présent : supprimer le tmp et utiliser l'existant
            logger.info(f"[move] {src.name} déjà présent à destination → skip")
            try:
                src.unlink()
            except Exception:
                pass
        else:
            shutil.move(str(src), str(target))
            logger.info(f"[move] {src.name} → {target}")
        result.append(target)
    return result


def _create_hardlinks(files: list[Path], save_dir: Path,
                      known_keywords: list[str]) -> None:
    """
    Crée des hardlinks vers les keywords secondaires (index 1+).
    Si pas de filesystem support → copie.
    """
    secondary_kws = known_keywords[1:]
    if not secondary_kws:
        return
    for kw in secondary_kws:
        kw_dir = save_dir / kw
        kw_dir.mkdir(parents=True, exist_ok=True)
        for f in files:
            link = kw_dir / f.name
            if link.exists():
                continue
            try:
                os.link(f, link)
                logger.info(f"[hardlink] {f.name} → {kw}/")
            except OSError:
                shutil.copy2(f, link)
                logger.info(f"[copy] {f.name} → {kw}/  (hardlink non supporté)")


# ── Downloaders ───────────────────────────────────────────────────────────────

def _run_gallery_dl(url: str, tmp_dir: Path, cfg_path: Path,
                    timeout: int = 180, progress_cb=None,
                    cookies_path: str | None = None) -> list[Path]:
    """Lance gallery-dl avec capture de progression ligne par ligne."""
    import threading as _threading, re as _re
    tmp_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable, "-m", "gallery_dl",
        "--config", str(cfg_path),
        "--dest", str(tmp_dir),
    ]
    if cookies_path and Path(cookies_path).exists():
        cmd += ["--cookies", cookies_path]
        logger.info(f"[gallery-dl] cookies: {cookies_path}")
    else:
        logger.warning(f"[gallery-dl] pas de cookies ({cookies_path!r})")
    cmd.append(url)
    logger.info(f"[gallery-dl] cmd: {' '.join(cmd)}")
    logger.info(f"[gallery-dl] dest: {tmp_dir}")

    _pct_re = _re.compile(r"(\d+\.?\d*)\s*%")
    last_pct = [-1]

    def _read(pipe):
        try:
            for raw in iter(pipe.readline, ""):
                line = raw.rstrip()
                if line:
                    logger.info(f"[gallery-dl] {line}")
                m = _pct_re.search(line)
                if m and progress_cb:
                    pct = min(100, int(float(m.group(1))))
                    if pct != last_pct[0]:
                        last_pct[0] = pct
                        try:
                            progress_cb(pct)
                        except Exception:
                            pass
        except Exception:
            pass

    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8", errors="replace",
        )
        t_out = _threading.Thread(target=_read, args=(proc.stdout,), daemon=True)
        t_err = _threading.Thread(target=_read, args=(proc.stderr,), daemon=True)
        t_out.start(); t_err.start()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            logger.error(f"[gallery-dl] timeout ({timeout}s)")
            return []
        t_out.join(timeout=5); t_err.join(timeout=5)
        logger.info(f"[gallery-dl] retcode={proc.returncode}")
    except Exception as e:
        logger.error(f"[gallery-dl] exception: {e}")
        return []

    media, thumbs = _collect_media(tmp_dir)
    logger.info(f"[gallery-dl] fichiers trouvés : {len(media)} média(s), {len(thumbs)} thumb(s)")
    return media + thumbs


def _run_ytdlp(url: str, tmp_dir: Path, timeout: int = 300,
               progress_cb=None) -> list[Path]:
    """Lance yt-dlp avec capture de progression ligne par ligne."""
    import threading as _threading, re as _re
    tmp_dir.mkdir(parents=True, exist_ok=True)
    outtmpl = str(tmp_dir / "%(id)s_%(uploader_id)s.%(ext)s")
    cmd = [
        sys.executable, "-m", "yt_dlp",
        "--output", outtmpl,
        "--no-playlist",
        "--no-overwrites",
        url,
    ]
    logger.info(f"[yt-dlp] cmd: {' '.join(cmd)}")
    logger.info(f"[yt-dlp] dest: {tmp_dir}")

    # yt-dlp écrit "[download]  42.3% of ..." sur stdout
    _pct_re = _re.compile(r"(\d+\.?\d*)\s*%")
    last_pct = [-1]

    def _read(pipe, label):
        try:
            for raw in iter(pipe.readline, ""):
                line = raw.rstrip()
                if line:
                    logger.info(f"[yt-dlp/{label}] {line}")
                m = _pct_re.search(line)
                if m and progress_cb:
                    pct = min(100, int(float(m.group(1))))
                    if pct != last_pct[0]:
                        last_pct[0] = pct
                        try:
                            progress_cb(pct)
                        except Exception:
                            pass
        except Exception:
            pass

    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8", errors="replace",
        )
        t_out = _threading.Thread(target=_read, args=(proc.stdout, "out"), daemon=True)
        t_err = _threading.Thread(target=_read, args=(proc.stderr, "err"), daemon=True)
        t_out.start(); t_err.start()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            logger.error(f"[yt-dlp] timeout ({timeout}s)")
            return []
        t_out.join(timeout=5); t_err.join(timeout=5)
        logger.info(f"[yt-dlp] retcode={proc.returncode}")
    except Exception as e:
        logger.error(f"[yt-dlp] exception: {e}")
        return []

    media, thumbs = _collect_media(tmp_dir)
    logger.info(f"[yt-dlp] fichiers trouvés : {len(media)} média(s), {len(thumbs)} thumb(s)")
    return media + thumbs


def _download_direct(url: str, tmp_dir: Path, task_id: int) -> list[Path]:
    """Téléchargement HTTP direct (URL image/video directe)."""
    import urllib.request
    tmp_dir.mkdir(parents=True, exist_ok=True)
    fname = url.split("?")[0].split("/")[-1] or f"direct_{task_id}.jpg"
    dest = tmp_dir / fname
    if dest.exists():
        logger.info(f"[direct] déjà présent : {fname}")
        return [dest]
    try:
        logger.info(f"[direct] téléchargement : {url}")
        urllib.request.urlretrieve(url, dest)
        logger.info(f"[direct] ✓ {fname}")
        return [dest]
    except Exception as e:
        logger.error(f"[direct] {e}")
        return []


# ── Enregistrement DB ─────────────────────────────────────────────────────────

def _register(db: BiDiDB, email_id: int, task_id: int,
              files: list[Path], save_dir: Path) -> None:
    existing = {mf["file_path"] for mf in db.get_media_files(email_id)}
    for i, f in enumerate(files):
        try:
            rel = str(f.relative_to(save_dir))
        except ValueError:
            rel = str(f)
        if rel in existing:
            logger.debug(f"[register] déjà en base : {rel}")
            continue
        ftype      = "thumbnail" if _is_thumb_file(f) else _classify(f)
        is_primary = (i == 0 and ftype != "thumbnail")
        db.add_media_file(
            email_id=email_id,
            task_id=task_id,
            file_path=rel,
            file_type=ftype,
            file_size=f.stat().st_size if f.exists() else None,
            is_primary=is_primary,
        )
        logger.info(
            f"[register] task={task_id} email={email_id} "
            f"type={ftype!r:12} primary={is_primary} "
            f"is_thumb_pattern={_is_thumb_file(f)} "
            f"file={f.name!r}"
        )


# ── Point d'entrée principal ──────────────────────────────────────────────────

def run_task(db: BiDiDB, task: dict, email: dict, progress_cb=None) -> bool:
    """
    Télécharge les fichiers pour une task et les enregistre en DB.
    Schéma :
      1. Téléchargement dans _tmp/task_<id>/
      2. Move vers save_dir/<premier_keyword>/ (ou save_dir/download/)
      3. Hardlinks vers les autres keywords
      4. Enregistrement en DB + set_task_done
    """
    cfg        = get_config()
    save_dir   = Path(cfg.get_save_dir())
    task_id    = task["id"]
    email_id   = task["email_id"]
    url        = task["url"] 
    downloader = task.get("downloader") or "gallery-dl"
    platform   = email.get("platform") or _detect_platform(url)
    _kw_raw = email.get("known_keywords") or []
    if isinstance(_kw_raw, str):
        try:
            import json as _json
            _kw_raw = _json.loads(_kw_raw)
        except Exception:
            _kw_raw = []
    known_kws = _kw_raw if isinstance(_kw_raw, list) else []
    
    # Wrapper interne : log + DB + callback externe
    _ext_cb = progress_cb
    def _progress(pct: int) -> None:
        logger.info(f"[task={task_id}] progression {pct}%")
        try:
            db.set_task_progress(task_id, pct)
        except Exception:
            pass
        if _ext_cb:
            try:
                _ext_cb(task_id, pct)
            except Exception:
                pass
    progress_cb = _progress

    logger.info(f"[task={task_id}] ── Démarrage ──────────────────────────")
    logger.info(f"[task={task_id}] url        = {url}")
    logger.info(f"[task={task_id}] downloader = {downloader}")
    logger.info(f"[task={task_id}] platform   = {platform}")
    logger.info(f"[task={task_id}] keywords   = {known_kws}")
    logger.info(f"[task={task_id}] save_dir   = {save_dir}")

    # Répertoire tmp isolé
    tmp_dir = save_dir / "_tmp" / f"task_{task_id}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"[task={task_id}] tmp_dir    = {tmp_dir}")

    # Répertoire cible final
    dest_dir = _primary_dest(save_dir, known_kws)
    logger.info(f"[task={task_id}] dest_dir   = {dest_dir}  (keyword={known_kws[0] if known_kws else 'download'})")

    # ── Téléchargement ────────────────────────────────────────────────────
    raw_files: list[Path] = []

    if downloader == "gallery-dl":
        cookies = cfg.get_reddit_cookies_path() if "reddit" in platform else None
        cfg_path = _write_gdl_config(task_id, cookies)
        raw_files = _run_gallery_dl(url, tmp_dir, cfg_path,
                                    timeout=cfg.get_gdl_timeout(),
                                    progress_cb=progress_cb,
                                    cookies_path=cookies)
        cfg_path.unlink(missing_ok=True)
        if not raw_files:
            logger.info(f"[task={task_id}] gallery-dl vide → fallback yt-dlp")
            raw_files = _run_ytdlp(url, tmp_dir, timeout=cfg.get_ytdlp_timeout(),
                                   progress_cb=progress_cb)

    elif downloader == "yt-dlp":
        raw_files = _run_ytdlp(url, tmp_dir, timeout=cfg.get_ytdlp_timeout(),
                               progress_cb=progress_cb)
        if not raw_files:
            logger.info(f"[task={task_id}] yt-dlp vide → fallback gallery-dl")
            cookies = cfg.get_reddit_cookies_path() if "reddit" in platform else None
            cfg_path = _write_gdl_config(task_id, cookies)
            raw_files = _run_gallery_dl(url, tmp_dir, cfg_path,
                                        timeout=cfg.get_gdl_timeout(),
                                        cookies_path=cookies)
            cfg_path.unlink(missing_ok=True)

    elif downloader == "direct":
        raw_files = _download_direct(url, tmp_dir, task_id)

    else:
        logger.error(f"[task={task_id}] downloader inconnu : {downloader!r}")
        shutil.rmtree(tmp_dir, ignore_errors=True)
        db.set_task_failed(task_id, f"downloader inconnu: {downloader}")
        return False

    # ── Vérification ──────────────────────────────────────────────────────
    if not raw_files:
        logger.error(f"[task={task_id}] aucun fichier téléchargé — échec")
        shutil.rmtree(tmp_dir, ignore_errors=True)
        db.set_task_failed(task_id, "aucun fichier téléchargé")
        return False

    logger.info(f"[task={task_id}] {len(raw_files)} fichier(s) dans tmp : "
                + ", ".join(f.name for f in raw_files[:10]))

    # ── Move vers dest_dir ────────────────────────────────────────────────
    final_files = _move_files_to_dest(raw_files, dest_dir)
    try:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    except Exception as e:
        logger.warning(f"[task={task_id}] rmtree échoué (ignoré): {e}")
    logger.info(f"[task={task_id}] fichiers déplacés vers {dest_dir}")

    # ── Hardlinks vers keywords secondaires ───────────────────────────────
    _create_hardlinks(final_files, save_dir, known_kws)

    # ── Enregistrement output_dir en DB ───────────────────────────────────
    try:
        rel_dest = str(dest_dir.relative_to(save_dir))
    except ValueError:
        rel_dest = str(dest_dir)
    db.set_task_output_dir(task_id, rel_dest)

    # ── Enregistrement fichiers en DB ─────────────────────────────────────
    _register(db, email_id, task_id, final_files, save_dir)
    db.set_task_done(task_id)

    n_media = sum(1 for f in final_files if not _is_thumb_file(f))
    n_thumb = sum(1 for f in final_files if _is_thumb_file(f))
    logger.info(f"[task={task_id}] ✓ done — {n_media} média(s), {n_thumb} thumb(s)")
    return True


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s [run_task] %(message)s",
        handlers=[
            logging.FileHandler(str(ROOT / "run_task.log"), encoding="utf-8", mode="a"),
            logging.StreamHandler(),
        ],
    )

    parser = argparse.ArgumentParser(description="BiDi run_task — worker de téléchargement")
    parser.add_argument("--task-id", type=int, required=True, help="ID de la task à traiter")
    parser.add_argument("--force", action="store_true",
                        help="Relance même si statut done/failed")
    args = parser.parse_args()

    cfg = get_config()
    db  = BiDiDB(cfg.get_db_path())

    logger.info(f"=== run_task --task-id {args.task_id} ===")

    # Charger la task directement par id
    with db._conn() as conn:
        row = conn.execute(
            "SELECT * FROM download_tasks WHERE id = ?", (args.task_id,)
        ).fetchone()
    task = dict(row) if row else None

    if not task:
        logger.error(f"Task #{args.task_id} introuvable en base")
        sys.exit(1)

    logger.info(
        f"Task #{args.task_id} | status={task.get('status')!r} "
        f"| downloader={task.get('downloader')!r} "
        f"| url={task.get('url')}"
    )

    if not args.force and task.get("status") not in ("pending", "sent"):
        logger.error(
            f"Task #{args.task_id} statut={task.get('status')!r} — non traitable. "
            f"Utilise --force pour relancer."
        )
        sys.exit(1)

    if args.force:
        logger.info("--force actif : relance sans vérification de statut")

    email = db.get_email(task["email_id"])
    if not email:
        logger.error(f"Email #{task['email_id']} introuvable")
        db.set_task_failed(args.task_id, "email introuvable")
        sys.exit(1)

    logger.info(
        f"Email #{email['id']} | step={email.get('step')!r} "
        f"| platform={email.get('platform')!r} "
        f"| keywords={email.get('known_keywords')}"
    )

    logger.info(f"Lancement téléchargement task #{args.task_id}...")
    try:
        ok = run_task(db, task, email)
    except Exception as e:
        logger.error(f"Exception non gérée : {e}", exc_info=True)
        db.set_task_failed(args.task_id, str(e))
        sys.exit(1)

    if ok:
        logger.info(f"=== Task #{args.task_id} ✓ succès ===")
        sys.exit(0)
    else:
        logger.error(f"=== Task #{args.task_id} ✗ échec ===")
        sys.exit(1)


if __name__ == "__main__":
    main()
