"""
File: step_reparse.py
Path: steps/step_reparse.py

Version: 1.0.0
Date: 2026-04-22

Changelog:
- 1.0.0 (2026-04-22): Création.
  re-calcule known/unknown keywords depuis la config courante.
  _sync_hardlinks() : crée/supprime les hardlinks en fonction du delta.
  Si primary kw change → déplace les fichiers vers le nouveau répertoire.
  Si plus de kw known → déplace tout vers save_dir/download/.
"""

import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config_manager import get_config
from database import BiDiDB

logger = logging.getLogger(__name__)

# Steps ayant des fichiers sur disque (éligibles pour sync hardlinks)
_STEPS_WITH_FILES = {
    "download_done", "thumb_done", "llm_done", "done",
}


def _get_all_keywords(cfg) -> set[str]:
    """Retourne tous les keywords connus définis dans la config."""
    kws = cfg.get("keywords", "known", default=[])
    if isinstance(kws, str):
        import json
        kws = json.loads(kws)
    return {k.lower().strip() for k in (kws or []) if k}


def _classify_keywords(
    source_url: str,
    subject: str,
    body_text: str,
    known_kws_config: set[str],
) -> tuple[list[str], list[str]]:
    """
    Re-classe les mots-clés d'un email selon la config courante.
    Retourne (known_list, unknown_list).
    """
    # Importer email_parser pour réutiliser la logique de détection
    import importlib
    ep = importlib.import_module("email_parser")

    text = " ".join(filter(None, [source_url, subject, body_text])).lower()
    known, unknown = [], []
    for kw in known_kws_config:
        if kw in text:
            known.append(kw)
        else:
            unknown.append(kw)
    return known, unknown


def _get_email_files(db: BiDiDB, email_id: int, save_dir: Path) -> list[Path]:
    """Retourne les fichiers primaires (non-thumbs) d'un email."""
    rows = db.get_media_files(email_id)
    files = []
    for r in rows:
        if r.get("is_thumb"):
            continue
        p = save_dir / r["file_path"] if not Path(r["file_path"]).is_absolute() else Path(r["file_path"])
        if p.exists():
            files.append(p)
    return files


def _sync_hardlinks(
    files: list[Path],
    old_kws: list[str],
    new_kws: list[str],
    save_dir: Path,
    db: BiDiDB,
    email_id: int,
) -> dict:
    """
    Synchronise les hardlinks sur disque après changement de keywords.

    Règles :
    - Primary kw changé → déplace fichiers vers nouveau répertoire primary.
    - kw ajouté dans new_kws (secondaire) → crée hardlink.
    - kw retiré de new_kws → supprime hardlink, rmdir si vide.
    - Plus aucun kw → déplace vers save_dir/download/.
    """
    result = {"moved": 0, "links_added": 0, "links_removed": 0}
    if not files:
        return result

    old_primary = old_kws[0] if old_kws else "download"
    new_primary = new_kws[0] if new_kws else "download"
    old_secondary = set(old_kws[1:]) if len(old_kws) > 1 else set()
    new_secondary = set(new_kws[1:]) if len(new_kws) > 1 else set()

    # ── Déplacement si primary a changé ───────────────────────────────────
    if old_primary != new_primary:
        new_dest = save_dir / new_primary
        new_dest.mkdir(parents=True, exist_ok=True)
        moved_files = []
        for f in files:
            target = new_dest / f.name
            if target.exists():
                logger.info(f"[reparse] déjà présent : {target}")
                moved_files.append(target)
            else:
                try:
                    shutil.move(str(f), str(target))
                    logger.info(f"[reparse] déplacé : {f.name} → {new_primary}/")
                    result["moved"] += 1
                    moved_files.append(target)
                except Exception as e:
                    logger.error(f"[reparse] move {f.name}: {e}")
                    moved_files.append(f)  # keep old path si échec

        # Màj DB media_files
        for old_f, new_f in zip(files, moved_files):
            rows = db.get_media_files(email_id)
            for row in rows:
                fp = Path(row["file_path"])
                if fp.name == old_f.name:
                    try:
                        new_rel = str(new_f.relative_to(save_dir))
                    except ValueError:
                        new_rel = str(new_f)
                    db.update_media_file_path(row["id"], new_rel)

        files = moved_files

    # ── Hardlinks à ajouter (nouveaux kw secondaires) ─────────────────────
    for kw in (new_secondary - old_secondary):
        kw_dir = save_dir / kw
        kw_dir.mkdir(parents=True, exist_ok=True)
        for f in files:
            link = kw_dir / f.name
            if link.exists():
                continue
            try:
                os.link(f, link)
                logger.info(f"[reparse] hardlink : {f.name} → {kw}/")
                result["links_added"] += 1
            except OSError:
                shutil.copy2(f, link)
                logger.info(f"[reparse] copy (pas hardlink) : {f.name} → {kw}/")
                result["links_added"] += 1

    # ── Hardlinks à supprimer (kw retirés) ────────────────────────────────
    kws_removed = old_secondary - new_secondary
    # Si old_primary retiré et n'est pas new_primary → supprimer aussi ses hardlinks
    if old_primary not in new_kws and old_primary != new_primary:
        kws_removed.add(old_primary)

    for kw in kws_removed:
        kw_dir = save_dir / kw
        for f in files:
            link = kw_dir / f.name
            if link.exists():
                try:
                    link.unlink()
                    logger.info(f"[reparse] supprimé hardlink : {f.name} dans {kw}/")
                    result["links_removed"] += 1
                except Exception as e:
                    logger.warning(f"[reparse] unlink {link}: {e}")
        # Supprimer le répertoire si vide
        try:
            if kw_dir.exists() and not any(kw_dir.iterdir()):
                kw_dir.rmdir()
                logger.info(f"[reparse] répertoire vide supprimé : {kw}/")
        except Exception:
            pass

    return result


def run(
    db: BiDiDB,
    cfg,
    email_id: Optional[int] = None,
    on_progress=None,
) -> dict:
    """
    Re-calcule les keywords pour un email (ou tous) et synchronise les fichiers.

    Si email_id fourni → traite uniquement cet email.
    Sinon → traite tous les emails parsed et au-delà.
    """
    save_dir    = Path(cfg.get_save_dir())
    config_kws  = _get_all_keywords(cfg)
    stats       = {"processed": 0, "kws_changed": 0, "files_synced": 0, "errors": 0}

    # Sélection des emails
    if email_id:
        email = db.get_email(email_id)
        emails = [email] if email else []
    else:
        # Tous les emails avec une source_url
        emails = []
        for step in ["parsed", "meta_done", "download_sent", "download_done",
                     "thumb_done", "llm_done", "done"]:
            emails.extend(db.get_emails_by_step(step, step_status=None))

    for email in emails:
        eid        = email["id"]
        source_url = email.get("source_url") or ""
        subject    = email.get("subject") or ""
        body_text  = email.get("body_text") or ""
        old_known  = email.get("known_keywords") or []
        old_unknown = email.get("unknown_keywords") or []

        # Re-classifier : mots de la config présents dans le texte de l'email
        text = " ".join(filter(None, [source_url, subject, body_text])).lower()
        new_known   = [kw for kw in config_kws if kw in text]
        new_unknown = [kw for kw in config_kws if kw not in text]

        # Trier pour stabilité (même ordre = même primary kw)
        # Conserver l'ordre de config_kws pour la cohérence
        all_kws_ordered = cfg.get("keywords", "known", default=[]) or []
        if isinstance(all_kws_ordered, str):
            import json
            all_kws_ordered = json.loads(all_kws_ordered)
        new_known   = [kw for kw in all_kws_ordered if kw in new_known]
        new_unknown = [kw for kw in all_kws_ordered if kw in new_unknown]

        kws_changed = (sorted(old_known) != sorted(new_known))

        # Màj DB keywords
        db.set_parse_data(
            eid,
            source_url=source_url or None,
            known_keywords=new_known,
            unknown_keywords=new_unknown,
        )

        stats["processed"] += 1

        if kws_changed:
            stats["kws_changed"] += 1
            logger.info(
                f"[reparse] email={eid} kws: {old_known} → {new_known}"
            )
            # Sync fichiers uniquement si l'email a des fichiers
            if email.get("step") in _STEPS_WITH_FILES:
                files = _get_email_files(db, eid, save_dir)
                if files:
                    try:
                        sync_result = _sync_hardlinks(
                            files, old_known, new_known, save_dir, db, eid
                        )
                        stats["files_synced"] += sync_result.get("moved", 0)
                        logger.info(f"[reparse] email={eid} sync: {sync_result}")
                    except Exception as e:
                        logger.error(f"[reparse] email={eid} sync error: {e}")
                        stats["errors"] += 1
        else:
            logger.debug(f"[reparse] email={eid} kws inchangés")

        if on_progress:
            on_progress()

    logger.info(f"[reparse] terminé — {stats}")
    return stats


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="BiDi step_reparse")
    parser.add_argument("--email-id", type=int, default=None,
                        help="Traiter uniquement cet email")
    args = parser.parse_args()
    cfg = get_config()
    db  = BiDiDB(cfg.get_db_path())
    stats = run(db, cfg, email_id=args.email_id)
    print(f"Résultat reparse: {stats}")
