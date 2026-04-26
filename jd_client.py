"""
File: jd_client.py
Path: jd_client.py

Version: 1.0.0
Date: 2026-04-22

Changelog:
- 1.0.0 (2026-04-22): Création — wrapper myjdapi.
  add_download(), get_package_progress(), cleanup_package().
"""

import logging
import time
import uuid
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

VIDEO_EXT = {".mp4", ".m4v", ".webm", ".mkv", ".mov", ".avi", ".ts", ".gif"}
IMAGE_EXT = {".jpg", ".jpeg", ".png", ".webp", ".avif"}
MEDIA_EXT = VIDEO_EXT | IMAGE_EXT


def _connect(cfg):
    """Connecte à My.JDownloader et retourne le device. Lève une exception si échec."""
    import myjdapi
    jd = myjdapi.Myjdapi()
    jd.set_app_key("bidi_downloader")
    jd.connect(cfg.get_jd_email(), cfg.get_jd_password())
    jd.update_devices()
    device = jd.get_device(cfg.get_jd_device())
    logger.info(f"[JD] Connecté → device='{cfg.get_jd_device()}'")
    return device


def add_download(cfg, url: str, dest_dir: Path) -> tuple[str, str | None]:
    """
    Envoie une URL à JDownloader.
    Retourne (pkg_name, pkg_uuid_dans_linkgrabber | None).
    pkg_name est un identifiant unique stable (bidi + uuid court).
    """
    pkg_name = f"bidi{uuid.uuid4().hex[:12]}"
    dl_dir = str(dest_dir.resolve())

    device = _connect(cfg)
    try:
        device.linkgrabber.add_links([{
            "autostart": True,
            "links": url,
            "destinationFolder": dl_dir,
            "packageName": pkg_name,
            "overwritePackagizerRules": True,
        }])
        logger.info(f"[JD] add_links pkg='{pkg_name}' → {dl_dir}")
    except Exception as e:
        logger.error(f"[JD] add_links échoué: {e}")
        raise

    # Attendre que JD ait indexé le package (jusqu'à 30s)
    pkg_uuid = None
    for _ in range(30):
        time.sleep(1)
        try:
            pkgs = device.linkgrabber.query_packages([{
                "saveTo": True, "name": True, "maxResults": 100, "startAt": 0,
            }]) or []
            for p in pkgs:
                if p.get("name") == pkg_name:
                    pkg_uuid = str(p.get("uuid"))
                    logger.info(f"[JD] pkg indexé uuid={pkg_uuid}")
                    break
        except Exception as e:
            logger.debug(f"[JD] linkgrabber query: {e}")
        if pkg_uuid:
            break

    return pkg_name, pkg_uuid


def get_package_progress(cfg, pkg_name: str) -> dict:
    """
    Interroge JD pour l'état d'un package.
    Retourne:
      {
        "found": bool,
        "finished": bool,
        "pct": int,            # 0-100
        "loaded_mb": float,
        "total_mb": float,
        "save_to": str | None,
        "uuid": str | None,
        "files": [str],        # noms de fichiers si finished
      }
    """
    result = {
        "found": False, "finished": False, "pct": 0,
        "loaded_mb": 0.0, "total_mb": 0.0,
        "save_to": None, "uuid": None, "files": [],
    }
    try:
        device = _connect(cfg)
    except Exception as e:
        logger.warning(f"[JD] get_progress connexion: {e}")
        return result

    # Chercher dans downloads
    try:
        all_pkgs = device.downloads.query_packages([{
            "finished": True, "running": True,
            "bytesLoaded": True, "bytesTotal": True,
            "saveTo": True, "name": True,
            "maxResults": 200, "startAt": 0,
        }]) or []
    except Exception as e:
        logger.warning(f"[JD] query_packages: {e}")
        return result

    pkg = next((p for p in all_pkgs if p.get("name") == pkg_name), None)
    if not pkg:
        return result

    loaded = pkg.get("bytesLoaded") or 0
    total  = pkg.get("bytesTotal") or 0
    pct    = int(loaded / total * 100) if total else 0

    result.update({
        "found":      True,
        "finished":   bool(pkg.get("finished")),
        "pct":        pct,
        "loaded_mb":  round(loaded / 1_048_576, 1),
        "total_mb":   round(total  / 1_048_576, 1),
        "save_to":    pkg.get("saveTo"),
        "uuid":       str(pkg.get("uuid")) if pkg.get("uuid") else None,
    })

    if result["finished"]:
        try:
            links = device.downloads.query_links([{
                "packageUUIDs": [pkg.get("uuid")],
                "name": True, "finished": True,
                "bytesTotal": True, "maxResults": 50, "startAt": 0,
            }]) or []
            result["files"] = [lnk["name"] for lnk in links if lnk.get("name")]
        except Exception as e:
            logger.warning(f"[JD] query_links: {e}")

    return result


def get_all_active_packages(cfg) -> list[dict]:
    """
    Retourne tous les packages actifs dans JD (pour le panel de progression).
    Chaque élément : {name, uuid, pct, loaded_mb, total_mb, finished}.
    """
    try:
        device = _connect(cfg)
        all_pkgs = device.downloads.query_packages([{
            "finished": False, "running": True,
            "bytesLoaded": True, "bytesTotal": True,
            "name": True, "maxResults": 200, "startAt": 0,
        }]) or []
    except Exception as e:
        logger.warning(f"[JD] get_all_active: {e}")
        return []

    result = []
    for p in all_pkgs:
        loaded = p.get("bytesLoaded") or 0
        total  = p.get("bytesTotal") or 0
        pct    = int(loaded / total * 100) if total else 0
        result.append({
            "name":       p.get("name"),
            "uuid":       str(p.get("uuid")) if p.get("uuid") else None,
            "pct":        pct,
            "loaded_mb":  round(loaded / 1_048_576, 1),
            "total_mb":   round(total  / 1_048_576, 1),
            "finished":   bool(p.get("finished")),
        })
    return result


def cleanup_package(cfg, pkg_uuid: str) -> None:
    """Supprime un package terminé de la liste JD."""
    try:
        device = _connect(cfg)
        device.downloads.cleanup(
            "DELETE_FINISHED",
            "REMOVE_LINKS_AND_DELETE_FILES",
            "SELECTED",
            package_ids=[int(pkg_uuid)],
        )
        logger.info(f"[JD] cleanup pkg_uuid={pkg_uuid}")
    except Exception as e:
        logger.warning(f"[JD] cleanup échoué: {e}")
