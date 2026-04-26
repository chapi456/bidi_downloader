"""
File: step_llm.py
Path: steps/step_llm.py

Version: 2.2.0
Date: 2026-04-21

Changelog:
- 2.2.0 (2026-04-21): Correction structure fichier (count() était avant le docstring).
  Ajout run(db, cfg, on_progress) + count(db, cfg) corrects.
  Logs détaillés : entrée count, entrée run, résultat par email.
- 2.0.0 (2026-04-19): Idempotence corrigée.
  Si llm.enabled=false ou auto_process=false → reste à thumb_done/ok (aucune écriture).
  Si Ollama fail → thumb_done/failed (retry via reset-failed).
  Succès → llm_done puis done.
- 1.0.0 (2026-04-16): Version initiale.
"""

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config_manager import get_config
from database import BiDiDB

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(name)s] %(message)s")
logger = logging.getLogger("step_llm")

_MAX_BODY = 1500


def _build_prompt(email: dict) -> str:
    parts = []
    if email.get("title"):
        parts.append(f"Titre : {email['title']}")
    if email.get("author"):
        parts.append(f"Auteur : {email['author']}")
    if email.get("platform"):
        parts.append(f"Plateforme : {email['platform']}")
    kws = email.get("known_keywords") or []
    if kws:
        parts.append(f"Mots-clés : {', '.join(kws)}")
    body = (email.get("body_text") or "").strip()
    if body:
        parts.append(f"Contenu :\n{body[:_MAX_BODY]}")
    parts.append(
        "En une phrase, résume ce contenu multimédia de façon neutre et factuelle."
    )
    return "\n".join(parts)


def _call_ollama(host: str, model: str, prompt: str, timeout: int = 60) -> str | None:
    try:
        import json, urllib.request
        payload = json.dumps({"model": model, "prompt": prompt, "stream": False}).encode()
        req = urllib.request.Request(
            f"{host.rstrip('/')}/api/generate",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode())
            return data.get("response", "").strip() or None
    except Exception as e:
        logger.warning(f"[ollama] {e}")
        return None


def process_llm(db: BiDiDB, email: dict) -> str:
    """
    Traite un email en thumb_done/ok.

    Règles d'idempotence :
      - disabled ou auto_process=false → aucune écriture, "skipped"
      - Ollama fail → thumb_done/failed, "failed"
      - Succès      → llm_done → done, "ok"
    """
    cfg = get_config()
    eid = email["id"]
    subj = (email.get("subject") or "")[:50]
    logger.info(f"llm {eid}: traitement — {subj!r}")

    if not cfg.get_llm_enabled() or not cfg.get_llm_auto_process():
        logger.info(f"llm {eid}: LLM désactivé → conservé à thumb_done")
        return "skipped"

    summary = _call_ollama(cfg.get_llm_host(), cfg.get_llm_model(), _build_prompt(email))

    if summary:
        db.update_email_llm(
            eid,
            llm_summary=summary,
            llm_workflow="auto",
            llm_params={"model": cfg.get_llm_model(), "host": cfg.get_llm_host()},
        )
        db.advance_step(eid, "llm_done")
        db.advance_step(eid, "done")
        logger.info(f"llm {eid}: ✓ ({len(summary)} chars) → done")
        return "ok"
    else:
        db.mark_failed(eid, "thumb_done", "Ollama injoignable ou réponse vide")
        logger.warning(f"llm {eid}: Ollama KO → thumb_done/failed")
        return "failed"


def count(db: BiDiDB, cfg) -> int:
    """Retourne le nombre d'emails à traiter par LLM (même sélection que run)."""
    emails = db.get_emails_by_step("thumb_done", "ok")
    n = len(emails)
    logger.info(f"[llm] count() → {n} email(s) à traiter")
    return n


def run(db: BiDiDB, cfg, on_progress=None) -> dict:
    """Point d'entrée pipeline — traite tous les emails en thumb_done/ok."""
    emails = db.get_emails_by_step("thumb_done", "ok")
    m = len(emails)
    logger.info(f"[llm] run() — {m} email(s) à traiter")
    results: dict[str, int] = {"ok": 0, "skipped": 0, "failed": 0}

    for email in emails:
        outcome = process_llm(db, email)
        results[outcome] = results.get(outcome, 0) + 1
        if on_progress:
            on_progress()

    logger.info(f"[llm] done — {results}")
    return results


def main() -> None:
    cfg = get_config()
    db  = BiDiDB(cfg.get_db_path())
    results = run(db, cfg)
    print(f"Résultat llm: {results}")


if __name__ == "__main__":
    main()
