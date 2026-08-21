"""
File: app_web.py
Path: app_web.py

Version: 3.4.0
Date: 2026-08-05

Changelog:
- 3.4.0 (2026-08-05): FIX log /api/status supprimé via on_startup (le filtre
  ajouté avant run() était écrasé par l'init interne uvicorn).
  FIX list_emails sérialise maintenant les médias (_serialize_email) :
  thumbnails et vidéos visibles dans la galerie web.
- 3.3.0 (2026-04-26): ...
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
from database import get_db # remplace "from database import BiDiDB"
from api_steps import router as steps_router

logger = logging.getLogger(__name__)
VERSION = "3.4.0"

cfg = get_config()
db  = get_db()
app = FastAPI(title="BiDi Media Manager", version=VERSION)

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
    fp = Path(filepath)
    if fp.is_absolute():
        rel = Path(_safe_relative_path(fp, save_dir))
    else:
        rel = fp
    return "/media/" + rel.as_posix().lstrip("/")


def _serialize_email(email: dict) -> dict:
    """Enrichit un email avec media_items et download_tasks."""
    files = db.get_media_files(email["id"])
    tasks = db.get_download_tasks(email["id"])

    media_items = []
    for f in files:
        fp = f.get("filepath") or f.get("file_path", "")
        ft = f.get("file_type") or _file_type(fp)
        media_items.append({
            "id":         f["id"],
            "url":        _media_url(fp),
            "file_type":  ft,
            "filetype":   ft,        # alias app.js
            "file_path":  fp,
            "filesize":   f.get("filesize") or f.get("file_size"),
            "is_primary": bool(f.get("is_primary")),
        })

    email["media_items"]    = media_items
    email["mediaitems"]     = media_items   # alias app.js v3
    email["media_files"]    = media_items   # alias buildCard()/renderModal()
    email["media_count"]    = len(media_items)
    email["download_tasks"] = tasks
    return email


# ── FIX log /api/status : filtre appliqué après init uvicorn via on_startup ──

from starlette.middleware.base import BaseHTTPMiddleware

# FIX définitif : le filtre sur uvicorn.access ne tenait pas de façon fiable
# (dépend trop des internals uvicorn selon version). On désactive complètement
# le logging d'accès natif d'uvicorn (access_log=False dans uvicorn.run) et on
# logge nous-mêmes via ce middleware, qu'on contrôle entièrement.
_SILENT_PREFIXES = ("/api/status", "/api/emails", "/api/stats", "/static/")

class _AccessLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        path = request.url.path
        is_silent_poll = (
            request.method == "GET"
            and response.status_code in (200, 304)
            and any(path == p or path.startswith(p) for p in _SILENT_PREFIXES)
        )
        if not is_silent_poll:
            logger.info(f'"{request.method} {path}" {response.status_code}')
        return response

app.add_middleware(_AccessLogMiddleware)

# ── API emails ────────────────────────────────────────────────────────────────

@app.get("/api/emails")
async def list_emails(
    step:     Optional[str] = Query(None),
    platform: Optional[str] = Query(None),
    limit:    int           = Query(50, ge=1, le=500),
    offset:   int           = Query(0, ge=0),
    search:   Optional[str] = Query(None),
):
    try:
        # FIX : filtre platform + search en SQL (via db.list_emails).
        # On demande limit+1 lignes pour savoir s'il reste une page suivante
        # sans dépendre de count==limit (cassé si un filtre réduit le résultat).
        rows = db.list_emails(
            step=step, limit=limit + 1, offset=offset,
            platform=platform, search=search,
        )
        has_more = len(rows) > limit
        emails = rows[:limit]
        emails = [_serialize_email(e) for e in emails]
        return {
            "ok": True,
            "emails": emails,
            "count": len(emails),
            "has_more": has_more,
            "offset": offset,
        }
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

def _safe_relative_path(fpath: Path, base: Path) -> str:
    """
    Calcule un chemin relatif fiable même en cas de différence de casse du
    lecteur Windows entre le chemin réel (ex: JD renvoie 'd:\\...') et
    save_dir configuré ('D:\\...') — Path.relative_to() est sensible à la
    casse et levait ValueError, faisant fuir le chemin ABSOLU en DB →
    URL /media/ cassée (vidéo non jouable côté web).
    """
    try:
        return str(fpath.relative_to(base))
    except ValueError:
        pass
    import os
    try:
        rel = os.path.relpath(str(fpath), str(base))
        if not rel.startswith(".."):
            return rel
    except ValueError:
        pass
    return str(fpath)

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
    print("=" * 60)

    def _force_exit(signum, frame):
        print("bidi: Arrêt forcé.")
        os._exit(0)

    signal.signal(signal.SIGTERM, _force_exit)
    signal.signal(signal.SIGINT,  _force_exit)

    uvicorn.run(app, host=host, port=port, log_level="info",
                access_log=False,  # FIX : remplacé par _AccessLogMiddleware
                timeout_graceful_shutdown=1)
    os._exit(0)