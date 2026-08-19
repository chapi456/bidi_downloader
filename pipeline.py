"""
File: pipeline.py
Path: pipeline.py

Version: 4.0.0
Date: 2026-04-22

Changelog:
- 4.0.0 (2026-04-22): Ajout steps "reparse" et "remeta" dans _STEP_MODULE.
  reset_step("reparse") et reset_step("remeta") opérationnels.
- 3.1.0 (2026-04-21): Ajout on_progress callback dans _call_run. Ajout _call_count pour appeler count() de chaque step avec la bonne signature.
- 5.1.0 (2026-04-19): Correction critique — appel run(db, cfg) sur chaque step
  (les steps n'ont pas de main(), ils exposent run()). Ajout signature
  adaptative : run(db, cfg) ou run(db, keywords) selon l'introspection.
- 5.0.0 (2026-04-19): Version initiale pipeline centralisé.
"""

import importlib
import inspect
import logging
import sys
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config_manager import get_config
from database import BiDiDB

logger = logging.getLogger(__name__)

# Ordre canonique des steps
PIPELINE: list[str] = ["fetch", "parse", "meta", "send", "check", "thumb", "llm"]

# Mapping step → module (chemin relatif à ROOT)
_STEP_MODULE: dict[str, str] = {
    "fetch":   "steps.step_fetch",
    "parse":   "steps.step_parse",
    "meta":    "steps.step_meta",
    "send":    "steps.step_send",
    "check":   "steps.step_check",
    "thumb":   "steps.step_thumb",
    "llm":     "steps.step_llm",
    "reparse": "steps.step_reparse",
    "remeta":  "steps.step_meta",   # alias : remeta relance step_meta
}

# FIX : PIPELINE utilise des noms d'actions CLI (fetch/parse/meta/...) mais la
# state machine DB (database.py STEPS) utilise des noms d'états différents.
# reset_step() confondait les deux, causant systématiquement une ValueError
# ("Step inconnu : 'fetch'") dès qu'on tentait un reset avec email_id.
_STEP_INPUT_STATE: dict[str, str] = {
    # État DB requis en entrée de chaque step CLI (= où le reset doit ramener)
    "fetch": "new",
    "parse": "new",
    "meta": "parsed",
    "send": "meta_done",
    "check": "download_sent",
    "thumb": "download_done",
    "llm": "thumb_done",
}
_STEP_OUTPUT_STATE: dict[str, str] = {
    # État DB produit en sortie de chaque step CLI (= où chercher les emails
    # "actuellement à ce step" pour un reset en masse sans email_id)
    "parse": "parsed",
    "meta": "meta_done",
    "send": "download_sent",
    "check": "download_done",
    "thumb": "thumb_done",
    "llm": "llm_done",
}

def _load_run(step: str):
    """Importe le module du step et retourne sa fonction run()."""
    module_path = _STEP_MODULE.get(step)
    if not module_path:
        raise ValueError(f"Step inconnu : {step!r}")

    # Ajoute steps/ au path si besoin
    steps_dir = ROOT / "steps"
    if steps_dir.exists() and str(steps_dir) not in sys.path:
        sys.path.insert(0, str(steps_dir))

    try:
        mod = importlib.import_module(module_path)
    except ModuleNotFoundError:
        # Fallback : import direct par nom de fichier
        mod_name = module_path.split(".")[-1]
        mod = importlib.import_module(mod_name)

    if not hasattr(mod, "run"):
        raise AttributeError(f"{module_path} n'expose pas de fonction run()")
    return mod.run



def _call_count(step: str, count_fn, db: BiDiDB, cfg) -> int:
    """
    Appelle count_fn du step avec la bonne signature (même logique que _call_run).
    Retourne 0 si count_fn absent ou si erreur.
    """
    try:
        sig    = inspect.signature(count_fn)
        params = list(sig.parameters.keys())
        if len(params) >= 2:
            second = params[1]
            if second in ("keywords", "kws"):
                return int(count_fn(db, cfg.get_keywords()))
            else:
                return int(count_fn(db, cfg))
        elif len(params) == 1:
            return int(count_fn(db))
        else:
            return int(count_fn())
    except Exception as e:
        logger.warning(f"[pipeline] count() {step} : {e}")
        return 0

def _call_run(step: str, run_fn, db: BiDiDB, cfg, on_progress=None) -> dict:
    """
    Appelle run_fn avec la bonne signature.
    Les steps utilisent soit run(db, cfg) soit run(db, keywords).
    on_progress est passé si le step le supporte (détecté par inspection).
    """
    sig    = inspect.signature(run_fn)
    params = list(sig.parameters.keys())
    has_progress = "on_progress" in sig.parameters

    if len(params) >= 2:
        second = params[1]
        if second in ("keywords", "kws"):
            kw_args = {"on_progress": on_progress} if has_progress else {}
            return run_fn(db, cfg.get_keywords(), **kw_args)
        else:
            kw_args = {"on_progress": on_progress} if has_progress else {}
            return run_fn(db, cfg, **kw_args)
    elif len(params) == 1:
        return run_fn(db)
    else:
        return run_fn()


def run_step(step: str, on_progress=None) -> dict:
    """Lance un seul step. Retourne le dict résultat du step."""
    cfg = get_config()
    db  = BiDiDB(cfg.get_db_path())
    logger.info(f"[pipeline] → {step}")
    try:
        run_fn = _load_run(step)
        result = _call_run(step, run_fn, db, cfg, on_progress=on_progress)
        logger.info(f"[pipeline] ✓ {step} : {result}")
        return result or {}
    except Exception as e:
        logger.error(f"[pipeline] ✗ {step} : {e}")
        raise


def count_step(step: str) -> int:
    """Retourne le nombre d'emails que run_step(step) traitera.
    Importe le module du step directement (même mécanique que _load_run)
    puis appelle count() avec _call_count pour garantir la même signature."""
    cfg = get_config()
    db  = BiDiDB(cfg.get_db_path())
    module_path = _STEP_MODULE.get(step)
    if not module_path:
        logger.warning(f"[pipeline] count_step({step}) : step inconnu")
        return 0
    try:
        steps_dir = ROOT / "steps"
        if steps_dir.exists() and str(steps_dir) not in sys.path:
            sys.path.insert(0, str(steps_dir))
        mod = importlib.import_module(module_path)
        importlib.reload(mod)
        count_fn = getattr(mod, "count", None)
        if count_fn is None:
            logger.warning(f"[pipeline] count_step({step}) : pas de count() dans {module_path}")
            return 0
        logger.info(f"[pipeline] count_step({step}) : appel count()…")
        n = _call_count(step, count_fn, db, cfg)
        logger.info(f"[pipeline] count_step({step}) → {n}")
        return n
    except Exception as e:
        logger.warning(f"[pipeline] count_step({step}) : {e}")
        return 0


def run_all() -> dict:
    """Lance tous les steps dans l'ordre canonique."""
    results = {}
    for step in PIPELINE:
        try:
            results[step] = run_step(step)
        except Exception as e:
            results[step] = {"error": str(e)}
            logger.error(f"[pipeline] run_all — arrêt sur {step} : {e}")
            break
    return results


def reset_step(step: str, *, email_id: Optional[int] = None,
                run_after: bool = False) -> dict:
    """
    Remet les emails du step donné à l'état DB précédent (état d'entrée du step).
    Si email_id fourni, ne remet que cet email.
    """
    if step not in PIPELINE:
        raise ValueError(f"Step inconnu : {step!r}")

    cfg = get_config()
    db = BiDiDB(cfg.get_db_path())

    target = _STEP_INPUT_STATE.get(step)
    if target is None:
        raise ValueError(f"Reset non supporté pour le step {step!r}")

    if email_id:
        emails = [db.get_email(email_id)]
        if not emails[0]:
            raise ValueError(f"Email #{email_id} introuvable")
    else:
        # FIX : recherche par état DB de sortie du step, pas par nom CLI
        # (get_emails_by_step("parse", ...) ne matchait jamais rien avant).
        output_state = _STEP_OUTPUT_STATE.get(step)
        if output_state is None:
            raise ValueError(
                f"Reset en masse non supporté pour le step {step!r} "
                f"(fournir email_id pour un reset ciblé)"
            )
        emails = db.get_emails_by_step(output_state, step_status=None)

    # FIX : nettoyer download_tasks/media_files si on repasse avant 'meta_done'
    # (sinon doublons de tâches et fichiers fantômes au prochain passage).
    _CLEANUP_TARGETS = {"new", "parsed"}
    count = 0
    for e in emails:
        if e:
            if target in _CLEANUP_TARGETS:
                db.clear_email_downloads(e["id"])
            db.advance_step(e["id"], target)
            count += 1

    result = {"reset": count, "target": target, "step": step}
    logger.info(f"[pipeline] reset {step} → {target} : {count} email(s)")

    if run_after and count:
        try:
            result["run_result"] = run_step(step)
        except Exception as ex:
            result["run_error"] = str(ex)

    return result


def reset_failed(*, step: Optional[str] = None,
                 email_id: Optional[int] = None,
                 run_after: bool = False) -> dict:
    """Remet tous les emails failed (ou d'un step précis) à l'état ok."""
    cfg = get_config()
    db  = BiDiDB(cfg.get_db_path())

    if email_id:
        emails = [db.get_email(email_id)]
        if not emails[0]:
            raise ValueError(f"Email #{email_id} introuvable")
    elif step:
        emails = db.get_emails_by_step(step, step_status="failed")
    else:
        # Tous les failed, tous steps
        emails = []
        for s in PIPELINE:
            emails += db.get_emails_by_step(s, step_status="failed")

    count = 0
    for e in emails:
        if e and e.get("step_status") == "failed":
            db.advance_step(e["id"], e["step"])
            count += 1

    result = {"reset": count, "step": step or "all"}
    logger.info(f"[pipeline] reset_failed : {count} email(s)")

    if run_after and count and step:
        try:
            result["run_result"] = run_step(step)
        except Exception as ex:
            result["run_error"] = str(ex)

    return result
