"""
File: app_web.py
Path: app_web.py

Version: 3.3.0
Date: 2026-04-24

Changelog:
- 3.3.0 (2026-04-24): Fix _media_url() Windows : Path.relative_to() + as_posix()
                       remplace le replace("\\","/") qui échouait selon le format
                       du chemin en DB → thumbnails s'affichent en galerie.
                       Ajout champ mediaitems dans _serialize_email() (attendu par app.js).
                       Cohérence réponse API : ok/email au lieu de status/data.
- 3.2.0 (2026-04-21): Fermeture forcée timeout=0.1s, signal SIGTERM/SIGINT.
- 3.1.0 (2026-04-21): Lancement uvicorn depuis config host/port.
- 3.0.0 (2026-04-21): Cohérence réponses API avec bidi_cli.
- 2.0.0 (2026-04-20): Refonte email-centrique BiDiDB v7. Templates externes.
- 1.0.0 (2026-03-15): Version initiale.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config_manager import get_config
from database import BiDiDB
from api_steps import router as steps_router

logger = logging.getLogger(__name__)
VERSION = "3.3.0"

cfg     = get_config()
db      = BiDiDB(cfg.get_db_path())
app     = FastAPI(title="BiDi Media Manager", version=VERSION)

templates = Jinja2Templates(directory=str(ROOT / "templates"))
app.mount("/static", StaticFiles(directory=str(ROOT / "templates" / "static")), name="static")

save_dir = Path(cfg.get_save_dir())
if save_dir.exists():
    app.mount("/media", StaticFiles(directory=str(save_dir)), name="media")

app.include_router(steps_router, prefix="/api")

# ── Constantes ────────────────────────────────────────────────────────────────

VIDEO_EXT = {".mp4", ".m4v", ".webm", ".mkv", ".mov", ".avi", ".ts"}
IMAGE_EXT = {".jpg", ".jpeg", ".png", ".gif", ".webp"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _file_type(filepath: str) -> str:
    ext = Path(filepath).suffix.lower()
    if ext in VIDEO_EXT:
        return "video"
    if ext in IMAGE_EXT:
        return "image"
    return "other"


def _media_url(filepath: str) -> str:
    """
    Construit l'URL /media/<rel> à partir d'un filepath DB.
    Utilise Path.relative_to() + as_posix() pour être robuste
    sur Windows (backslashes) et Linux.
    """
    fp = Path(filepath)
    if fp.is_absolute():
        try:
            rel = fp.relative_to(save_dir)
        except ValueError:
            rel = fp
    else:
        rel = fp
    # as_posix() → forward-slashes même sous Windows
    return "/media/" + rel.as_posix().lstrip("/")


def _serialize_email(email: dict) -> dict:
    """Enrichit un email avec media_items, download_tasks, media_count."""
    files = db.get_media_files(email["id"])
    tasks = db.get_download_tasks(email["id"])

    media_items = []
    for f in files:
        ft = f.get("file_type") or _file_type(f.get("filepath", "") or f.get("file_path", ""))
        fp = f.get("filepath") or f.get("file_path", "")
        media_items.append({
            "id":         f["id"],
            "url":        _media_url(fp),
            "file_type":  ft,
            "filetype":   ft,
            "file_path":  fp,
            "file_size":  f.get("filesize") or f.get("file_size"),
            "filesize":   f.get("filesize") or f.get("file_size"),
            "is_primary": bool(f.get("is_primary")),
        })

    email["media_items"]     = media_items
    email["mediaitems"]      = media_items   # alias app.js v3
    email["media_files"]     = media_items   # alias attendu par buildCard() / renderModal()
    email["media_count"]     = len(media_items)
    email["download_tasks"]  = tasks
    return email


# ── API emails ────────────────────────────────────────────────────────────────

@app.get("/api/emails")
async def list_emails(
    step:   Optional[str] = Query(None),
    limit:  int           = Query(50, ge=1, le=500),
    offset: int           = Query(0, ge=0),
    search: Optional[str] = Query(None),
):
    try:
        emails = db.list_emails(step=step, limit=200, offset=offset)
        if search:
            q = search.strip().lower()
            emails = [
                e for e in emails
                if q in (e.get("subject")  or "").lower()
                or q in (e.get("source_url") or "").lower()
                or q in (e.get("title")    or "").lower()
                or q in (e.get("platform") or "").lower()
            ]
        emails = emails[:limit]
        return {"ok": True, "emails": emails, "count": len(emails), "offset": offset}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/emails/{email_id}")
async def get_email(email_id: int):
    email = db.get_email(email_id)
    if not email:
        raise HTTPException(status_code=404, detail="Email introuvable")
    return {"ok": True, "email": _serialize_email(email)}


@app.get("/api/emails/{email_id}/media")
async def get_email_media(email_id: int):
    if not db.get_email(email_id):
        raise HTTPException(status_code=404, detail="Email introuvable")
    files = db.get_media_files(email_id)
    items = []
    for f in files:
        fp = f.get("filepath") or f.get("file_path", "")
        ft = f.get("file_type") or _file_type(fp)
        items.append({
            "id":         f["id"],
            "url":        _media_url(fp),
            "file_type":  ft,
            "filetype":   ft,
            "is_primary": bool(f.get("is_primary")),
            "filesize":   f.get("filesize") or f.get("file_size"),
        })
    return {"ok": True, "data": items, "count": len(items)}


@app.post("/api/emails/{email_id}/rating")
async def rate_email(email_id: int, body: dict):
    rating = body.get("rating")
    if not isinstance(rating, int) or not (0 <= rating <= 5):
        raise HTTPException(status_code=400, detail="rating doit être entre 0 et 5")
    if not db.get_email(email_id):
        raise HTTPException(status_code=404, detail="Email introuvable")
    db.set_rating(email_id, rating)
    return {"ok": True}


# ── API stats ─────────────────────────────────────────────────────────────────

@app.get("/api/stats")
async def get_stats():
    try:
        return {"ok": True, "data": db.get_stats()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Interface web ─────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/health")
async def health():
    return {"ok": True, "status": "healthy"}


# ── Lancement ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s [%(name)s] %(message)s")
    host         = cfg.get_server_host()
    port         = cfg.get_server_port()
    display_host = "localhost" if host in ("0.0.0.0", "") else host

    print("=" * 60)
    print(f" BiDi Media Manager v{VERSION}")
    print(f" Interface : http://{display_host}:{port}")
    print(f" API docs  : http://{display_host}:{port}/docs")
    print(f" Config    : {cfg.path or 'defaults'}")
    print("=" * 60)

    def _force_exit(signum, frame):
        print("bidi: Arrêt forcé (signal reçu).")
        os._exit(0)

    signal.signal(signal.SIGTERM, _force_exit)
    signal.signal(signal.SIGINT,  _force_exit)

    import logging

    class _SuppressStatusOK(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            msg = record.getMessage()
            return not ('"GET /api/status' in msg and '" 200 ' in msg)

    # Appliquer le filtre AVANT uvicorn.run() ET via log_config
    logging.getLogger("uvicorn.access").addFilter(_SuppressStatusOK())

    uvicorn.run(app, host=host, port=port, log_level="info",
                timeout_graceful_shutdown=1,
                access_log=True)

    # Re-appliquer après run (au cas où uvicorn réinitialise le logger)
    logging.getLogger("uvicorn.access").addFilter(_SuppressStatusOK())
    
    os._exit(0)
