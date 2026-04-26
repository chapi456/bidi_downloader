"""
File: api_steps.py
Path: api_steps.py

Version: 3.0.0
Date: 2026-04-22

Changelog:
- 3.0.0 (2026-04-22): +DELETE /api/email/{id}, +POST /api/reparse,
  +POST /api/remeta. SSE enrichi avec tasks_progress (JD + actifs).
  Import step_reparse.
- 2.2.0 (2026-04-21): SSE 500ms, logs count_step/on_progress/SSE payload.
- 2.1.0 (2026-04-21): Ajout n/m dans _running_steps via count_step + on_progress callback.
  SSE expose running_steps (avec n,m) au lieu de juste la liste des noms.
- 2.0.0 (2026-04-21): Délègue run/reset à pipeline.py (plus de subprocess).
  Ajout POST /api/reset/step/{step} et POST /api/reset/failed.
  GET /api/status retourne {ok, stats, running_tasks, recent_logs}.
  GET /api/emails retourne {ok, emails, count} (cohérence avec bidi_cli_old).
- 1.0.0 (2026-04-20): Extraction endpoints pipeline depuis app_web.py.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from pathlib import Path
from typing import Optional
import sys

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from fastapi.responses import StreamingResponse

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config_manager import get_config
from database import BiDiDB
from pipeline import PIPELINE, run_step, run_all, reset_step, reset_failed, count_step
from steps.step_reparse import run as _step_reparse_run
from steps.step_meta    import run as _step_meta_run

logger = logging.getLogger(__name__)
router = APIRouter()

# ── État thread-safe ──────────────────────────────────────────────────────────

_lock = threading.Lock()
_running_steps: dict[str, dict] = {}
_log_lines: list[str] = []
MAX_LOGS = 200


def _append_log(line: str) -> None:
    with _lock:
        _log_lines.append(line)
        if len(_log_lines) > MAX_LOGS:
            _log_lines.pop(0)


def _set_running(step: str, status: str = "running", n: int = 0, m: int = 0) -> None:
    with _lock:
        if status == "done":
            _running_steps.pop(step, None)
        else:
            _running_steps[step] = {"status": status, "n": n, "m": m}


def _inc_progress(step: str) -> None:
    """Incrémente n pour le step en cours (appelé par on_progress callback)."""
    with _lock:
        if step in _running_steps:
            _running_steps[step]["n"] += 1


def _run_step_thread(step: str) -> None:
    _append_log(f"[{step}] Démarrage…")
    m = count_step(step)
    logger.info(f"[api_steps] count_step({step}) → m={m}")
    _set_running(step, n=0, m=m)
    _append_log(f"[{step}] {m} élément(s) à traiter")

    def on_progress():
        _inc_progress(step)
        with _lock:
            info = _running_steps.get(step, {})
        n_val = info.get('n', '?')
        m_val = info.get('m', '?')
        logger.info(f"[api_steps] on_progress {step} → {n_val}/{m_val}")
        _append_log(f"[{step}] avancement {n_val}/{m_val}")

    try:
        result = run_step(step, on_progress=on_progress)
        msg = "OK" if not result.get("error") else f"ERREUR : {result['error']}"
        _append_log(f"[{step}] Terminé – {msg}")
    except Exception as e:
        _append_log(f"[{step}] ERREUR : {e}")
    finally:
        _set_running(step, "done")


def _run_all_thread() -> None:
    _append_log("[all] Lancement pipeline complet…")
    _set_running("all", n=0, m=len(PIPELINE))
    for step in PIPELINE:
        with _lock:
            all_still_running = "all" in _running_steps
        if not all_still_running:
            break
        m = count_step(step)
        _set_running(step, n=0, m=m)
        _append_log(f"[{step}] Démarrage… ({m} élément(s))")

        def _make_cb(s):
            def on_progress():
                _inc_progress(s)
                with _lock:
                    info = _running_steps.get(s, {})
                _append_log(f"[{s}] {info.get('n','?')}/{info.get('m','?')}")
            return on_progress

        try:
            result = run_step(step, on_progress=_make_cb(step))
            msg = "OK" if not result.get("error") else f"ERREUR : {result['error']}"
            _append_log(f"[{step}] Terminé – {msg}")
            with _lock:
                if "all" in _running_steps:
                    _running_steps["all"]["n"] += 1
        except Exception as e:
            _append_log(f"[{step}] ERREUR : {e}")
            _append_log(f"[all] Arrêt pipeline sur erreur {step}.")
            break
        finally:
            _set_running(step, "done")
    _set_running("all", "done")
    _append_log("[all] Pipeline complet terminé.")


# ── Routes : run ──────────────────────────────────────────────────────────────

@router.post("/run/{step}")
async def run_step_endpoint(step: str):
    if step not in set(PIPELINE) | {"all"}:
        raise HTTPException(400, f"Step inconnu : {step!r}. Valides : {sorted(PIPELINE) + ['all']}")

    with _lock:
        busy = bool(_running_steps)
    if busy:
        return {"ok": False, "error": "Un traitement est déjà en cours"}

    if step == "all":
        _set_running("all")
        threading.Thread(target=_run_all_thread, daemon=True).start()
    else:
        threading.Thread(target=_run_step_thread, args=(step,), daemon=True).start()

    return {"ok": True, "step": step, "status": "running"}


# ── Routes : reset ────────────────────────────────────────────────────────────

@router.post("/reset/step/{step}")
async def reset_step_endpoint(
    step: str,
    email_id: Optional[int] = Query(None),
    run: str = Query("1"),
):
    if step not in PIPELINE:
        raise HTTPException(400, f"Step inconnu : {step!r}")
    run_after = run != "0"
    try:
        result = reset_step(step, email_id=email_id, run_after=run_after)
        return {"ok": True, **result}
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/reset/failed")
async def reset_failed_endpoint(
    step: Optional[str] = Query(None),
    email_id: Optional[int] = Query(None),
    run: str = Query("1"),
):
    run_after = run != "0"
    try:
        result = reset_failed(step=step, email_id=email_id, run_after=run_after)
        return {"ok": True, **result}
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, str(e))


# ── Routes : status ───────────────────────────────────────────────────────────

@router.get("/status")
async def get_status():
    db = BiDiDB(get_config().get_db_path())
    stats = db.get_stats()
    with _lock:
        running       = list(_running_steps.keys())
        running_steps = {k: dict(v) for k, v in _running_steps.items()}
        logs          = list(_log_lines[-50:])
    return {"ok": True, "stats": stats, "running_tasks": running,
            "running_steps": running_steps, "recent_logs": logs}


@router.get("/status/stream")
async def status_stream():
    """Server-Sent Events : stats + logs toutes les 2 s."""
    async def generator():
        db = BiDiDB(get_config().get_db_path())
        last_idx = 0
        while True:
            stats = db.get_stats()
            with _lock:
                running       = list(_running_steps.keys())
                running_steps = {k: dict(v) for k, v in _running_steps.items()}
                new_logs      = list(_log_lines[last_idx:])
                last_idx      = len(_log_lines)
            # Progression des tasks actives (JD + gallery-dl/yt-dlp)
            tasks_progress = []
            try:
                for t in db.get_tasks_by_status("sent"):
                    tasks_progress.append({
                        "task_id":    t["id"],
                        "email_id":   t["email_id"],
                        "downloader": t.get("downloader", ""),
                        "pct":        t.get("progress_pct", 0),
                        "pkg_name":   t.get("jd_package_name"),
                    })
            except Exception:
                pass
            payload = json.dumps(
                {"stats": stats, "running": running,
                 "running_steps": running_steps, "logs": new_logs,
                 "tasks_progress": tasks_progress},
                ensure_ascii=False,
            )
            logger.debug(
                f"[SSE] payload running={running} "
                f"running_steps={running_steps} logs_new={len(new_logs)}"
            )
            yield f"data: {payload}\n\n"
            await asyncio.sleep(0.5)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Routes : delete / reparse / remeta ────────────────────────────────────────

@router.delete("/email/{email_id}")
async def delete_email_endpoint(email_id: int):
    """Supprime un email et tous ses fichiers associés de la DB."""
    cfg = get_config()
    db  = BiDiDB(cfg.get_db_path())
    email = db.get_email(email_id)
    if not email:
        raise HTTPException(404, f"Email {email_id} introuvable")
    try:
        db.delete_email(email_id)
        _broadcast_log(f"[api] email={email_id} supprimé")
        return {"ok": True, "deleted": email_id}
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/reparse")
async def reparse_endpoint(
    email_id: Optional[int] = Query(None),
    background_tasks: BackgroundTasks = None,
):
    """Re-calcule les keywords et synchronise les hardlinks."""
    cfg = get_config()
    db  = BiDiDB(cfg.get_db_path())

    def _run():
        try:
            stats = _step_reparse_run(db, cfg, email_id=email_id,
                                      on_progress=_broadcast_log)
            _broadcast_log(f"[reparse] {stats}")
        except Exception as e:
            _broadcast_log(f"[reparse] erreur: {e}")

    if background_tasks:
        background_tasks.add_task(_run)
    else:
        _run()
    return {"ok": True, "queued": True}


@router.post("/remeta")
async def remeta_endpoint(
    email_id: Optional[int] = Query(None),
    background_tasks: BackgroundTasks = None,
):
    """Relance step_meta sur un email (ou tous les emails parsed)."""
    cfg = get_config()
    db  = BiDiDB(cfg.get_db_path())

    target_emails: list = []
    if email_id:
        e = db.get_email(email_id)
        if not e:
            raise HTTPException(404, f"Email {email_id} introuvable")
        target_emails = [e]
    else:
        target_emails = db.get_emails_by_step("parsed", step_status=None)

    def _run():
        try:
            stats = _step_meta_run(db, cfg, on_progress=_broadcast_log)
            _broadcast_log(f"[remeta] {stats}")
        except Exception as e:
            _broadcast_log(f"[remeta] erreur: {e}")

    if background_tasks:
        background_tasks.add_task(_run)
    else:
        _run()
    return {"ok": True, "queued": True, "emails": len(target_emails)}
