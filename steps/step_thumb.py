"""
File: step_thumb.py
Path: steps/step_thumb.py

Version: 3.0.0
Date: 2026-04-24

Changelog:
- 3.0.0 (2026-04-24): Fix renommage : traite TOUS les fichiers media de l'email
                       (un thumbnail par fichier vidéo/image, pas seulement le premier).
                       _find_disk_thumb() appelé pour chaque fichier media du dossier.
                       _rename_all_media_files() renomme chaque fichier selon le keyword
                       primaire et met à jour le file_path en DB.
- 2.6.0 (2026-04-21): Fix matching thumbnail : _find_disk_thumb reçoit expected_thumb
                       (path exact de l'external-preview de CET email).
- 2.5.0 (2026-04-21): Fix réel external-preview + _remove_stale_thumb.
- 2.4.0 (2026-04-21): Logs détaillés.
- 2.3.0 (2026-04-21): Logs diagnostic run().
- 2.2.0 (2026-04-21): Bug fix external-preview + logs.
- 2.1.0 (2026-04-20): Ajout run(db, cfg).
- 2.0.0 (2026-04-18): Logique simplifiée thumbnail.
- 1.0.0 (2026-04-16): Version initiale (ffmpeg only).
"""

import logging
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config_manager import get_config
from database import BiDiDB

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("thumb")

VIDEO_EXT = {".mp4", ".m4v", ".webm", ".mkv", ".mov", ".avi", ".ts", ".gif"}
IMAGE_EXT = {".jpg", ".jpeg", ".png", ".webp", ".avif"}


# ── Helpers disque ────────────────────────────────────────────────────────────

def _find_disk_thumb(
    video_path: Path,
    expected_thumb: Path | None = None,
) -> Path | None:
    """
    Cherche / crée le thumbnail pour video_path.

    Ordre de priorité :
      1. <stem>.thumb.* déjà présent sur disque → retour immédiat.
      2. expected_thumb fourni et présent → renommage → retour.
      3. Aucun résultat → None (ffmpeg en dernier recours).
    """
    parent = video_path.parent
    stem   = video_path.stem

    if not parent.exists():
        logger.warning(f"[_find_disk_thumb] dossier introuvable : {parent}")
        return None

    # 1. <stem>.thumb.* déjà correctement nommé
    for ext in (".jpg", ".jpeg", ".png", ".webp"):
        candidate = parent / f"{stem}.thumb{ext}"
        if candidate.exists():
            logger.debug(f"[_find_disk_thumb] déjà renommé : {candidate.name}")
            return candidate

    # 2. Renommer le fichier exact attendu (external-preview de CET email)
    if expected_thumb is not None and expected_thumb.exists():
        renamed = parent / f"{stem}.thumb{expected_thumb.suffix.lower()}"
        if not renamed.exists():
            expected_thumb.rename(renamed)
            logger.info(f"[thumb] renommé {expected_thumb.name} → {renamed.name}")
        else:
            try:
                expected_thumb.unlink()
            except Exception:
                pass
        return renamed

    logger.debug(f"[_find_disk_thumb] aucun thumb trouvé pour {video_path.name}")
    return None


def _extract_frame(video_path: Path) -> Path | None:
    """Génère une miniature via ffmpeg à 5 secondes."""
    thumb_path = video_path.parent / f"{video_path.stem}.thumb.jpg"
    if thumb_path.exists():
        return thumb_path

    cmd = [
        "ffmpeg", "-y",
        "-ss", "5",
        "-i", str(video_path),
        "-vframes", "1",
        "-q:v", "2",
        str(thumb_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=60)
        if result.returncode == 0 and thumb_path.exists():
            return thumb_path
        logger.warning(f"[ffmpeg] retcode={result.returncode} pour {video_path.name}")
    except Exception as e:
        logger.error(f"[ffmpeg] {e}")
    return None


def _remove_stale_thumb(db: BiDiDB, email_id: int, save_dir: Path, keep: Path) -> None:
    """Supprime en DB les entrées thumbnail dont le path diffère de `keep`."""
    for mf in db.get_media_files(email_id):
        if mf.get("file_type") != "thumbnail":
            continue
        mf_path = (save_dir / mf["file_path"]).resolve()
        if mf_path != keep.resolve():
            logger.info(f"[thumb] suppression DB stale thumb id={mf['id']} ({mf['file_path']})")
            db.delete_media_file(mf["id"])


# ── Renommage de TOUS les fichiers media ──────────────────────────────────────

def _rename_media_file(
    old_path: Path,
    save_dir: Path,
    keyword: str,
    suffix: str,
) -> Path:
    """
    Renomme old_path en <keyword>_<suffix><ext> dans le même dossier.
    Si le fichier cible existe déjà, retourne old_path sans modifier.
    """
    new_name = f"{keyword}_{suffix}{old_path.suffix.lower()}"
    new_path = old_path.parent / new_name
    if new_path == old_path:
        return old_path
    if new_path.exists():
        logger.info(f"[rename] cible déjà existante : {new_name} → skip")
        return new_path
    try:
        old_path.rename(new_path)
        logger.info(f"[rename] {old_path.name} → {new_name}")
    except Exception as e:
        logger.warning(f"[rename] échec {old_path.name} → {new_name} : {e}")
        return old_path
    return new_path


def _rename_all_media_files(
    db: BiDiDB,
    email_id: int,
    save_dir: Path,
    keyword: str,
) -> None:
    """
    Renomme TOUS les fichiers media (vidéos + images + thumbnails) associés
    à l'email selon le schéma : <keyword>_<N><ext>
    Met à jour le file_path en DB pour chacun.

    Règles :
    - Les fichiers .thumb.* gardent le suffixe .thumb dans le nouveau nom.
    - Les vidéos/images sont numérotées dans l'ordre (1, 2, …).
    - Les hardlinks dans les dossiers keywords secondaires sont recréés
      après renommage.
    """
    media_files = db.get_media_files(email_id)
    if not media_files:
        return

    video_idx = 1
    thumb_idx = 1

    for mf in media_files:
        file_type = mf.get("file_type", "")
        old_rel   = mf["file_path"]
        old_abs   = save_dir / old_rel
        # Gérer les paths absolus en DB (cas rare)
        if not old_abs.exists():
            old_abs = Path(old_rel)
        if not old_abs.exists():
            logger.warning(f"[rename] fichier introuvable sur disque : {old_rel}")
            continue

        # Choisir le suffixe selon le type
        if file_type == "thumbnail":
            suffix = f"{thumb_idx}.thumb"
            thumb_idx += 1
        elif file_type == "video":
            suffix = str(video_idx)
            video_idx += 1
        elif file_type == "image":
            suffix = str(video_idx)
            video_idx += 1
        else:
            # Type inconnu → on renomme quand même pour cohérence
            suffix = str(video_idx)
            video_idx += 1

        new_abs = _rename_media_file(old_abs, save_dir, keyword, suffix)

        # Mettre à jour le path en DB si changé
        if new_abs != old_abs:
            try:
                new_rel = str(new_abs.relative_to(save_dir))
            except ValueError:
                new_rel = str(new_abs)
            # Utiliser as_posix() pour cohérence cross-platform
            new_rel = Path(new_rel).as_posix()
            try:
                db.update_media_file_path(mf["id"], new_rel)
                logger.info(f"[rename] DB mis à jour id={mf['id']} : {old_rel!r} → {new_rel!r}")
            except Exception as e:
                logger.error(f"[rename] échec MAJ DB id={mf['id']} : {e}")

        # Mettre à jour les hardlinks dans les dossiers keywords secondaires
        # (les hardlinks pointent vers le même inode → le rename du primaire
        #  les rend incohérents. On les recrée.)
        _refresh_hardlinks_for_file(new_abs, save_dir, old_abs.name)


def _refresh_hardlinks_for_file(
    new_file: Path,
    save_dir: Path,
    old_name: str,
) -> None:
    """
    Parcourt tous les sous-dossiers de save_dir et remplace les hardlinks
    portant l'ancien nom old_name par un hardlink vers new_file.
    """
    for kw_dir in save_dir.iterdir():
        if not kw_dir.is_dir() or kw_dir.name.startswith("_"):
            continue
        old_link = kw_dir / old_name
        if old_link.exists() and old_link.resolve() == new_file.resolve():
            # Hardlink avec l'ancien nom → remplacer
            new_link = kw_dir / new_file.name
            if new_link.exists():
                try:
                    old_link.unlink()
                except Exception:
                    pass
                continue
            try:
                import os
                os.link(new_file, new_link)
                old_link.unlink()
                logger.info(f"[hardlink] refreshed : {old_name} → {new_file.name} dans {kw_dir.name}/")
            except OSError:
                try:
                    import shutil
                    shutil.copy2(new_file, new_link)
                    old_link.unlink()
                    logger.info(f"[copy] refreshed : {old_name} → {new_file.name} dans {kw_dir.name}/")
                except Exception as e:
                    logger.warning(f"[hardlink] refresh échoué pour {old_name} dans {kw_dir.name}/ : {e}")


# ── Traitement principal d'un email ──────────────────────────────────────────

def process_thumb(db: BiDiDB, email: dict) -> str:
    """
    Traite un email en download_done → thumb_done.

    Étapes :
      1. Pour chaque fichier vidéo sans thumbnail → cherche/génère un thumbnail.
      2. Renomme TOUS les fichiers media selon le schéma <keyword>_<N><ext>.
      3. advance_step → thumb_done.
    """
    cfg      = get_config()
    save_dir = Path(cfg.get_save_dir())
    eid      = email["id"]

    subj = (email.get("subject") or "")[:50]
    logger.info(f"thumb {eid}: début traitement — {subj!r}")

    media_files = db.get_media_files(eid)
    logger.info(f"thumb {eid}: {len(media_files)} fichier(s) media en DB")
    for mf in media_files:
        logger.info(f"  └─ id={mf['id']} type={mf.get('file_type')!r:12} "
                    f"primary={mf.get('is_primary')} path={mf.get('file_path')!r}")

    # Keyword primaire pour le renommage
    known_kws = email.get("known_keywords") or []
    keyword   = known_kws[0] if known_kws else "download"

    # ── Étape 1 : S'assurer qu'un thumbnail existe pour chaque vidéo ──────────
    outcome = _ensure_thumbnails(db, eid, media_files, save_dir)

    # ── Étape 2 : Renommer TOUS les fichiers selon le keyword primaire ─────────
    logger.info(f"thumb {eid}: renommage fichiers avec keyword={keyword!r}")
    _rename_all_media_files(db, eid, save_dir, keyword)

    db.advance_step(eid, "thumb_done")
    logger.info(f"thumb {eid}: thumb_done (outcome={outcome!r})")
    return outcome


def _ensure_thumbnails(
    db: BiDiDB,
    eid: int,
    media_files: list[dict],
    save_dir: Path,
) -> str:
    """
    Pour chaque vidéo principale sans thumbnail associé,
    cherche un .thumb.* sur disque ou génère via ffmpeg.
    Retourne 'skip'|'disk'|'ffmpeg'|'failed'.
    """
    cfg = get_config()

    existing_thumbs = [mf for mf in media_files if mf.get("file_type") == "thumbnail"]
    videos = [
        mf for mf in media_files
        if mf.get("file_type") == "video"
    ]

    # Si pas de vidéo du tout
    if not videos:
        logger.info(f"thumb {eid}: aucune vidéo → skip thumbnails")
        return "skip"

    # Si un thumbnail existe déjà pour chaque vidéo → skip
    if len(existing_thumbs) >= len(videos):
        # Vérifier qu'ils sont bien sur disque
        all_ok = True
        for mf in existing_thumbs:
            p = save_dir / mf["file_path"]
            low = mf["file_path"].lower().replace("\\", "/")
            is_external = "external" in low or "preview" in low
            if not p.exists() or is_external:
                all_ok = False
                break
        if all_ok:
            logger.info(f"thumb {eid}: thumbnails déjà présents → skip")
            return "skip"

    last_outcome = "skip"

    # Pour chaque vidéo, s'assurer d'un thumbnail
    for video_mf in videos:
        video_path = save_dir / video_mf["file_path"]
        task_id    = video_mf.get("task_id")

        # Chercher un thumbnail existant lié à cette vidéo (même stem)
        stem = video_path.stem
        matching_thumb = next(
            (mf for mf in existing_thumbs
             if Path(mf["file_path"]).stem.startswith(stem)),
            None,
        )

        expected_thumb_path: Path | None = None
        if matching_thumb:
            thumb_raw = matching_thumb["file_path"]
            thumb_abs = save_dir / thumb_raw
            low = thumb_raw.lower().replace("\\", "/")
            is_external = "external" in low or "preview" in low
            if is_external:
                expected_thumb_path = thumb_abs
                # Supprimer l'entrée DB stale avant de recréer
                db.delete_media_file(matching_thumb["id"])
                existing_thumbs.remove(matching_thumb)
            elif thumb_abs.exists():
                logger.info(f"thumb {eid}: thumbnail ok pour {video_path.name} → skip")
                continue
            else:
                logger.warning(f"thumb {eid}: thumbnail en DB absent sur disque → re-scan")
                db.delete_media_file(matching_thumb["id"])
                existing_thumbs.remove(matching_thumb)

        # Chercher sur disque
        disk_thumb = _find_disk_thumb(video_path, expected_thumb=expected_thumb_path)
        if disk_thumb:
            try:
                rel = disk_thumb.relative_to(save_dir).as_posix()
            except ValueError:
                rel = str(disk_thumb)
            _remove_stale_thumb(db, eid, save_dir, disk_thumb)
            db.add_media_file(
                email_id=eid,
                task_id=task_id,
                file_path=rel,
                file_type="thumbnail",
                file_size=disk_thumb.stat().st_size,
                is_primary=False,
            )
            logger.info(f"thumb {eid}: thumbnail sur disque → {rel}")
            last_outcome = "disk"
            continue

        # ffmpeg en dernier recours
        if not video_path.exists():
            logger.warning(f"thumb {eid}: vidéo introuvable : {video_path}")
            last_outcome = "failed"
            continue

        logger.info(f"thumb {eid}: génération ffmpeg pour {video_path.name}")
        thumb = _extract_frame(video_path)
        if thumb:
            try:
                rel = thumb.relative_to(save_dir).as_posix()
            except ValueError:
                rel = str(thumb)
            db.add_media_file(
                email_id=eid,
                task_id=task_id,
                file_path=rel,
                file_type="thumbnail",
                file_size=thumb.stat().st_size,
                is_primary=False,
            )
            logger.info(f"thumb {eid}: frame extraite → {rel}")
            last_outcome = "ffmpeg"
        else:
            logger.warning(f"thumb {eid}: ffmpeg échoué pour {video_path.name}")
            last_outcome = "failed"

    return last_outcome


# ── count / run ───────────────────────────────────────────────────────────────

def count(db, cfg) -> int:
    n = len(db.get_emails_by_step("download_done", "ok"))
    logger.info(f"[thumb] count() → {n} email(s) en download_done/ok")
    return n


def run(db: BiDiDB, cfg, on_progress=None) -> dict:
    """Point d'entrée pipeline — traite tous les emails en download_done/ok."""
    all_dd = db.get_emails_by_step("download_done", step_status=None)
    logger.info(f"[thumb] download_done (tous statuts) : {len(all_dd)} email(s)")
    for e in all_dd:
        logger.info(f"  email {e['id']} step_status={e.get('step_status')!r} "
                    f"subject={str(e.get('subject', ''))[:40]!r}")

    emails = db.get_emails_by_step("download_done", "ok")
    stats  = {"processed": 0, "skipped": 0, "failed": 0}

    logger.info(f"[thumb] {len(emails)} email(s) à traiter")
    for email in emails:
        outcome = process_thumb(db, email)
        logger.info(f"[thumb] email {email['id']} → outcome={outcome!r}")
        if on_progress:
            on_progress()
        if outcome in ("skip", "disk", "ffmpeg"):
            stats["processed"] += 1
        elif outcome == "failed":
            stats["failed"] += 1
        else:
            stats["skipped"] += 1

    logger.info(f"[thumb] done — {stats}")
    return stats


def main() -> None:
    cfg = get_config()
    db  = BiDiDB(cfg.get_db_path())
    results = run(db, cfg)
    print(f"Résultat thumb: {results}")


if __name__ == "__main__":
    main()
