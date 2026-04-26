"""
File: bidi_cli.py
Path: bidi_cli.py

Version: 8.0.0
Date: 2026-04-22

Changelog:
- 8.0.0 (2026-04-22): +commandes delete, reparse, remeta.
  TUI : barre de progression JD dans le panel email sélectionné.
  SSE : consommation de tasks_progress.
- 7.0.0 (2026-04-21): Fusion TUI curses (bidi_cli_old v4) + CLI argparse (v6).
  Mode par défaut (sans argument) = TUI plein écran avec auto-refresh 5s,
  navigation ↑↓ emails, menu steps ←→, reset-step, reset-failed, filtre.
  Mode CLI (avec argument) = sous-commandes argparse : status, list, show,
  run, logs, server-status. Adapté aux nouvelles réponses API v3
  {ok, emails, ...} / {ok, stats, running_tasks, recent_logs}.
  Client REST pur : aucun accès DB direct.
- 6.0.0 (2026-04-20): CLI argparse. Pagination, SSE logs.
- 4.0.0 (2026-04-19): TUI curses avec fallback texte.
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from pathlib import Path

from openai import base_url

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config_manager import get_config

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

STEPS = ["fetch", "parse", "meta", "send", "check", "thumb", "llm"]
STEPS_EXTRA = ["reparse", "remeta"]  # actions spéciales sans step DB direct

# ── Couleurs ANSI (mode CLI) ──────────────────────────────────────────────────

def _c(text: str, code: str) -> str:
    return f"\033[{code}m{text}\033[0m" if sys.stdout.isatty() else text

def green(t):  return _c(t, "32")
def red(t):    return _c(t, "31")
def yellow(t): return _c(t, "33")
def cyan(t):   return _c(t, "36")
def bold(t):   return _c(t, "1")
def dim(t):    return _c(t, "2")

STEP_COLOR = {
    "new": dim, "parsed": yellow, "meta_done": yellow,
    "download_sent": cyan, "download_done": cyan,
    "thumb_done": green, "llm_done": green, "done": green,
}
STATUS_COLOR = {"ok": green, "running": cyan, "failed": red, "pending": dim}

# ── HTTP ──────────────────────────────────────────────────────────────────────

def _base() -> str:
    return get_config().get_server_url().rstrip("/")

def _get(path: str, params: dict | None = None) -> dict | None:
    import urllib.request, urllib.error, urllib.parse
    url = _base() + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None

def _post(path: str, payload: dict | None = None) -> dict | None:
    import urllib.request, urllib.error
    url = _base() + path
    data = json.dumps(payload or {}).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None

def _require_server(result: dict | None) -> dict:
    if result is None:
        print(red(f"✗ Serveur inaccessible : {_base()}"), file=sys.stderr)
        print(dim("  Démarrez : python app_web.py"), file=sys.stderr)
        sys.exit(1)
    return result

# ── État TUI partagé entre threads ────────────────────────────────────────────

_state: dict = {
    "emails": [],
    "stats": {},
    "logs": [],
    "filter_step": "",
    "selected": 0,
    "dirty": True,
    "progress": "",   # ex: "fetch  3/10" ou "fetch  terminé"
}
_lock = threading.Lock()


def _add_log(msg: str) -> None:
    with _lock:
        _state["logs"].append(msg)
        if len(_state["logs"]) > 200:
            _state["logs"] = _state["logs"][-200:]
        _state["dirty"] = True


def _refresh() -> None:
    step = _state["filter_step"]
    params: dict = {"limit": 100}
    if step:
        params["step"] = step
    data = _get("/api/emails", params)
    status = _get("/api/status")
    with _lock:
        _state["emails"] = (data or {}).get("emails", [])
        _state["stats"] = (status or {}).get("stats", {})
        _state["dirty"] = True


def _poller() -> None:
    while True:
        _refresh()
        time.sleep(5)


def _run_step(step: str) -> None:
    _add_log(f"→ run {step} …")
    r = _post(f"/api/run/{step}")
    if not (r and r.get("ok")):
        _add_log(f"✗ {(r or {}).get('error', 'serveur inaccessible')}")
        time.sleep(1)
        _refresh()
        return
    _add_log(f"✓ {step} démarré")
    while True:
        time.sleep(1)
        status = _get("/api/status")
        if not status:
            break
        rs = (status.get("running_steps") or {})
        if step in rs:
            info = rs[step]
            n, m = info.get("n", "?"), info.get("m", "?")
            with _lock:
                _state["progress"] = f"{step}  {n}/{m}"
                _state["dirty"] = True
        else:
            with _lock:
                _state["progress"] = f"{step}  terminé"
                _state["dirty"] = True
            break
    _refresh()


def _reset_step(step: str, email_id: int | None = None) -> None:
    path = f"/api/reset/step/{step}"
    if email_id:
        path += f"?email_id={email_id}"
    _add_log(f"→ reset step {step}" + (f" email#{email_id}" if email_id else ""))
    r = _post(path)
    if r and r.get("ok"):
        _add_log(f"✓ reset {step} : {r.get('reset', 0)} email(s) → {r.get('target', '?')}")
    else:
        _add_log(f"✗ {(r or {}).get('error', 'serveur inaccessible')}")
    time.sleep(1)
    _refresh()


def _reset_failed(email_id: int | None = None) -> None:
    path = "/api/reset/failed"
    if email_id:
        path += f"?email_id={email_id}"
    _add_log(f"→ reset failed" + (f" email#{email_id}" if email_id else ""))
    r = _post(path)
    if r and r.get("ok"):
        _add_log(f"✓ reset failed : {r.get('reset', 0)} email(s)")
    else:
        _add_log(f"✗ {(r or {}).get('error', 'serveur inaccessible')}")
    time.sleep(1)
    _refresh()

# ── TUI curses ────────────────────────────────────────────────────────────────

def _run_tui() -> None:
    import curses

    STEP_COLORS = {"ok": 2, "failed": 3, "running": 4}

    def _badge(s: str) -> str:
        return {"ok": "✓", "failed": "✗", "running": "…"}.get(s, s[:1])

    def _main(stdscr):
        curses.curs_set(0)
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_CYAN,    -1)
        curses.init_pair(2, curses.COLOR_GREEN,   -1)
        curses.init_pair(3, curses.COLOR_RED,     -1)
        curses.init_pair(4, curses.COLOR_YELLOW,  -1)
        curses.init_pair(5, curses.COLOR_WHITE,   -1)
        stdscr.nodelay(True)
        stdscr.keypad(True)

        menu_items = [*STEPS, "reparse", "remeta", "reset-step", "reset-failed", "filter", "quit"]
        menu_idx = 0

        while True:
            with _lock:
                dirty  = _state["dirty"]
                emails = list(_state["emails"])
                stats  = dict(_state["stats"])
                logs   = list(_state["logs"])
                sel    = _state["selected"]
                fstep  = _state["filter_step"]
                _state["dirty"] = False

            H, W = stdscr.getmaxyx()
            LOG_H   = 6
            MENU_H  = 3
            LIST_H  = H - LOG_H - MENU_H - 2

            if dirty:
                stdscr.erase()

                # Header
                total   = stats.get("total_emails", 0)
                files   = stats.get("total_media_files", 0)
                pending = stats.get("pending_download_tasks", 0)
                hdr = f" BiDi  emails:{total}  fichiers:{files}  tasks:{pending}"
                if fstep:
                    hdr += f"  [filtre:{fstep}]"
                stdscr.addnstr(0, 0, hdr.ljust(W), W,
                               curses.color_pair(1) | curses.A_BOLD)

                # Liste emails
                for i, e in enumerate(emails[:LIST_H]):
                    y    = i + 1
                    subj = (e.get("subject") or "")[:40]
                    step = (e.get("step") or "")[:14]
                    st   = e.get("step_status", "")
                    plat = (e.get("platform") or "-")[:8]
                    line = f" {e['id']:>4}  {step:<16} {_badge(st)}  {subj:<40}  {plat:<8}"
                    attr  = curses.A_REVERSE if i == sel else 0
                    color = curses.color_pair(STEP_COLORS.get(st, 5))
                    stdscr.addnstr(y, 0, line[:W], W, color | attr)

                # Menu
                menu_y = LIST_H + 1
                stdscr.addnstr(menu_y, 0, "─" * W, W, curses.color_pair(1))
                mx = 1
                for j, item in enumerate(menu_items):
                    label = f"[{item}]"
                    attr  = curses.A_REVERSE if j == menu_idx else 0
                    if mx + len(label) < W:
                        stdscr.addnstr(menu_y + 1, mx, label, W - mx, attr)
                    mx += len(label) + 1
                stdscr.addnstr(menu_y + 2, 0,
                    " ↑↓:email  ←→:menu  ENTER:action  r:refresh  q:quit",
                    W, curses.color_pair(4))

                # Zone progression (bleue, fixe)
                prog_y = menu_y + MENU_H
                with _lock:
                    prog_txt = _state.get("progress", "")
                prog_line = f" ⏳ {prog_txt}" if prog_txt else ""
                stdscr.addnstr(prog_y, 0, prog_line.ljust(W), W, curses.color_pair(1))

                # Log
                log_y = H - LOG_H
                stdscr.addnstr(log_y, 0, "─" * W, W, curses.color_pair(1))
                for i, line in enumerate(logs[-(LOG_H - 1):]):
                    color = (curses.color_pair(3) if "✗" in line else
                             curses.color_pair(2) if "✓" in line else
                             curses.color_pair(4))
                    stdscr.addnstr(log_y + 1 + i, 1, line[:W - 2], W - 2, color)

                stdscr.refresh()

            # Input
            key = stdscr.getch()
            if key == -1:
                time.sleep(0.1)
                continue

            if key in (ord("q"), ord("Q")):
                break
            elif key in (ord("r"), ord("R")):
                threading.Thread(target=_refresh, daemon=True).start()
            elif key == curses.KEY_UP:
                with _lock:
                    _state["selected"] = max(0, sel - 1)
                    _state["dirty"] = True
            elif key == curses.KEY_DOWN:
                with _lock:
                    _state["selected"] = min(max(0, len(emails) - 1), sel + 1)
                    _state["dirty"] = True
            elif key == curses.KEY_LEFT:
                menu_idx = (menu_idx - 1) % len(menu_items)
                with _lock:
                    _state["dirty"] = True
            elif key == curses.KEY_RIGHT:
                menu_idx = (menu_idx + 1) % len(menu_items)
                with _lock:
                    _state["dirty"] = True
            elif key in (curses.KEY_ENTER, 10, 13):
                action = menu_items[menu_idx]
                em = emails[sel] if emails else None

                if action == "quit":
                    break
                elif action == "filter":
                    curses.echo()
                    curses.curs_set(1)
                    stdscr.addnstr(LIST_H + 1, 0, "Filtrer (step ou vide): ", W)
                    inp = stdscr.getstr(LIST_H + 1, 24, 20).decode().strip()
                    curses.noecho()
                    curses.curs_set(0)
                    with _lock:
                        _state["filter_step"] = inp
                        _state["dirty"] = True
                    _refresh()
                elif action == "reset-step":
                    curses.echo()
                    curses.curs_set(1)
                    label = f"Reset step ({'/'.join(STEPS)}): "
                    stdscr.addnstr(LIST_H + 1, 0, label, W)
                    inp = stdscr.getstr(LIST_H + 1, len(label), 12).decode().strip()
                    curses.noecho()
                    curses.curs_set(0)
                    if inp in STEPS:
                        eid = em["id"] if em else None
                        threading.Thread(
                            target=_reset_step, args=(inp, eid), daemon=True
                        ).start()
                elif action == "reset-failed":
                    eid = em["id"] if em else None
                    threading.Thread(
                        target=_reset_failed, args=(eid,), daemon=True
                    ).start()
                elif action == "reparse":
                    eid = em["id"] if em else None
                    threading.Thread(target=lambda: _post(f"/api/reparse" + (f"?email_id={eid}" if eid else ""), {}), daemon=True).start()
                    _add_log(f"reparse lancé (email={eid or 'tous'})")
                elif action == "remeta":
                    eid = em["id"] if em else None
                    threading.Thread(target=lambda: _post(f"/api/remeta" + (f"?email_id={eid}" if eid else ""), {}), daemon=True).start()
                    _add_log(f"remeta lancé (email={eid or 'tous'})")
                elif action in STEPS:
                    threading.Thread(target=_run_step, args=(action,), daemon=True).start()

    curses.wrapper(_main)

# ── Fallback menu texte ────────────────────────────────────────────────────────

def _run_text() -> None:
    print(f"BiDi CLI — mode texte  (serveur : {_base()})")
    _refresh()

    while True:
        with _lock:
            emails = list(_state["emails"])
            stats  = dict(_state["stats"])
            logs   = list(_state["logs"])

        print(f"\n emails:{stats.get('total_emails', 0)}"
              f"  fichiers:{stats.get('total_media_files', 0)}"
              f"  tasks:{stats.get('pending_download_tasks', 0)}")
        for e in emails[:20]:
            print(f"  {e['id']:>4}  {e.get('step',''):<18}"
                  f"  {e.get('step_status',''):<8}"
                  f"  {(e.get('subject') or '')[:50]}")
        if logs:
            print(" Log:", logs[-1])

        choices = "  ".join(f"[{i+1}]{s}" for i, s in enumerate(STEPS))
        print(f"\n {choices}")
        print("  [r]refresh  [R]reset-step  [F]reset-failed  [q]quit")
        try:
            c = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if c == "q":
            break
        elif c == "r":
            _refresh()
        elif c == "R":
            step = input("  Step : ").strip()
            eid_s = input("  Email id (vide=tous) : ").strip()
            eid = int(eid_s) if eid_s.isdigit() else None
            if step in STEPS:
                _reset_step(step, eid)
        elif c == "F":
            eid_s = input("  Email id (vide=tous) : ").strip()
            eid = int(eid_s) if eid_s.isdigit() else None
            _reset_failed(eid)
        elif c.isdigit():
            idx = int(c) - 1
            if 0 <= idx < len(STEPS):
                _run_step(STEPS[idx])

# ── Commandes CLI (mode argparse) ─────────────────────────────────────────────

def cmd_status(args):
    data = _require_server(_get("/api/status"))
    stats = data.get("stats", {})
    print(bold("BiDi – Statut"))
    print(f"  Emails         {bold(str(stats.get('total_emails', 0)))}")
    print(f"  Fichiers media {stats.get('total_media_files', 0)}")
    print(f"  Tâches DL      {stats.get('total_download_tasks', 0)}"
          f"  (en attente {yellow(str(stats.get('pending_download_tasks', 0)))})")
    steps = stats.get("steps", {})
    if steps:
        print()
        print(bold("Progression par étape :"))
        for step, counts in steps.items():
            col = STEP_COLOR.get(step, str)
            parts = [f"{STATUS_COLOR.get(s, str)(s)}:{n}" for s, n in counts.items()]
            print(f"  {col(f'{step:<20}')} {' '.join(parts)}")


def cmd_list(args):
    params = {"limit": args.limit, "offset": args.offset}
    if args.step:
        params["step"] = args.step
    data = _require_server(_get("/api/emails", params))
    emails = data.get("emails", [])
    if not emails:
        print(dim("Aucun email trouvé."))
        return
    print(bold(f"{'ID':>5}  {'STEP':<20}  {'STATUS':<10}  {'SUJET':<46}  {'DATE':<16}"))
    print(dim("-" * 100))
    for e in emails:
        step_col   = STEP_COLOR.get(e.get("step", ""), str)
        status_col = STATUS_COLOR.get(e.get("step_status", ""), str)
        subject    = (e.get("subject") or "(sans sujet)")[:44]
        date       = (e.get("received_at") or "")[:16]
        media_icon = (" [" + str(e.get("media_count", 0)) + "m]") if e.get("media_count") else ""
        fail_tag   = red(" FAIL") if e.get("step_status") == "failed" else ""
        e_step   = "{:<20}".format(e.get("step", ""))
        e_status = "{:<10}".format(e.get("step_status", ""))
        print("  {:>5}  ".format(e["id"])
              + step_col(e_step)
              + status_col(e_status)
              + "  {:<46}  {}{}{}".format(subject, dim(date), media_icon, fail_tag))
    if len(emails) == args.limit:
        nxt = args.offset + args.limit
        print(dim(f"\n → Suivant : bidi list --offset {nxt} --limit {args.limit}"))


def cmd_show(args):
    data = _require_server(_get(f"/api/emails/{args.id}"))
    email = data.get("email", {})
    if not email:
        print(red(f"Email {args.id} introuvable."))
        sys.exit(1)
    step_col = STEP_COLOR.get(email.get("step", ""), str)
    print(bold(f"── Email #{email['id']} " + "─" * 50))
    for label, key in [("Sujet", "subject"), ("De", "sender"),
                       ("Reçu le", "received_at"), ("Message-ID", "message_id")]:
        print(f"  {label:<12} {email.get(key) or dim('-')}")
    print()
    print(f"  Étape        {step_col(email.get('step',''))}"
          f" {STATUS_COLOR.get(email.get('step_status',''), str)(email.get('step_status',''))}")
    if email.get("step_error"):
        print(f"  Erreur       {red(email['step_error'])}")
    for label, key in [("URL source", "source_url"), ("Titre", "title"),
                       ("Plateforme", "platform"), ("Auteur", "author"),
                       ("Durée", "duration")]:
        if email.get(key):
            print(f"  {label:<12} {email[key]}")
    files = email.get("media_files") or email.get("mediaitems") or []
    media_files  = [f for f in files if f.get("file_type") != "thumbnail"]
    thumb_files  = [f for f in files if f.get("file_type") == "thumbnail"]

    if media_files:
        print(bold(f"\n  Fichiers media ({len(media_files)}) :"))
        for f in media_files:
            primary  = yellow(" ★") if f.get("is_primary") else ""
            size_raw = f.get("file_size") or f.get("filesize") or 0
            size     = f"{size_raw//1024}KB" if size_raw else ""
            fname    = str(f.get("file_path") or f.get("filepath") or "–")
            url      = f.get("url") or ""
            print(f"  {(f.get('file_type','')):<10} {fname[:55]}"
                  f"  {dim(size)}{primary}")
            if url:
                print(f"  {'':10} {dim(url)}")

    if thumb_files:
        print(bold(f"\n  Thumbnails ({len(thumb_files)}) :"))
        for f in thumb_files:
            fname = str(f.get("file_path") or f.get("filepath") or "–")
            url   = f.get("url") or ""
            print(f"  {'thumbnail':<10} {fname[:55]}")
            if url:
                print(f"  {'':10} {dim(url)}")

    post_body = email.get("post_body") or ""
    post_comments = email.get("post_comments") or []
    if post_body or post_comments:
        n = len(post_comments) if isinstance(post_comments, list) else 0
        print(bold(f"\n  Reddit ({n} comment{'s' if n != 1 else ''}) :"))
        if post_body:
            preview = post_body[:200].replace("\n", " ")
            ellipsis = "…" if len(post_body) > 200 else ""
            print(f"  {'Post':<12} {dim(preview)}{ellipsis}")
        for c in (post_comments[:3] if isinstance(post_comments, list) else []):
            author = c.get("author", "?")
            body   = (c.get("body") or "")[:80].replace("\n", " ")
            score  = c.get("score", "")
            print(f"  {cyan(author):<20} [{score}] {dim(body)}")
        if isinstance(post_comments, list) and len(post_comments) > 3:
            print(dim(f"  … {len(post_comments) - 3} commentaire(s) supplémentaire(s)"))


def cmd_run(args):
    valid = set(STEPS) | {"all"}
    if args.step not in valid:
        print(red(f"Step inconnu : {args.step!r}"))
        sys.exit(1)
    print(f"  Lancement du step {cyan(args.step)}…")
    result = _require_server(_post(f"/api/run/{args.step}"))
    if result.get("ok"):
        print(green(f"  ✓ Step {args.step!r} démarré."))
        print(dim("  Suivez : bidi logs --follow"))
    else:
        print(red(f"  ✗ {result.get('error', 'Erreur inconnue')}"))
        sys.exit(1)


def cmd_server_status(args):
    data = _require_server(_get("/api/status"))
    stats   = data.get("stats", {})
    running = data.get("running_tasks", [])
    logs    = data.get("recent_logs", [])
    print(bold("Serveur BiDi"))
    print(f"  Emails  {stats.get('total_emails', 0)}")
    print(f"  Médias  {stats.get('total_media_files', 0)}")
    print(f"  DL      {stats.get('total_download_tasks', 0)}"
          f"  (attente {yellow(str(stats.get('pending_download_tasks', 0)))})")
    if running:
        print(bold("\n  Tâches en cours :"))
        for t in running:
            print(f"    {cyan(t)}")
    else:
        print(dim("\n  aucune tâche en cours"))
    if logs:
        print(bold(f"\n  Derniers logs :"))
        for line in logs[-20:]:
            print(f"    {dim(line)}")


def cmd_logs(args):
    import urllib.request, urllib.error
    url = _base() + "/api/status/stream"
    print(f"{dim('Connexion à')} {url}  (Ctrl+C pour quitter)\n")
    try:
        with urllib.request.urlopen(url, timeout=None) as resp:
            while True:
                raw = resp.readline()
                if not raw:
                    time.sleep(0.3)
                    continue
                line = raw.decode("utf-8").strip()
                if not line.startswith("data:"):
                    continue
                try:
                    payload = json.loads(line[5:].strip())
                except json.JSONDecodeError:
                    continue
                for log_line in payload.get("logs", []):
                    col = (red   if "ERREUR" in log_line or "Exception" in log_line else
                           green if "Terminé" in log_line and "OK" in log_line else
                           cyan  if "Démarrage" in log_line or "→" in log_line else str)
                    print(col(log_line))
                running = payload.get("running", [])
                if not args.follow and not running:
                    break
    except KeyboardInterrupt:
        print(dim("\nArrêt."))
    except Exception as e:
        print(red(f"Serveur inaccessible : {e}"), file=sys.stderr)
        sys.exit(1)



# ── Commandes : delete / reparse / remeta ─────────────────────────────────────

def cmd_delete(args) -> None:
    """Supprime un email (DELETE /api/emails/{id})."""
    import urllib.request
    url = f"{_base()}/api/emails/{args.id}"
    try:
        r = urllib.request.urlopen(
            urllib.request.Request(url, method="DELETE"),
            timeout=15,
        )
        data = json.loads(r.read())
        if data.get("ok"):
            print(green(f"Email {args.id} supprimé."))
        else:
            print(red(f"Erreur: {data}"))
    except Exception as e:
        print(red(f"Erreur delete: {e}"), file=sys.stderr)
        sys.exit(1)


def cmd_reparse(args) -> None:
    """Re-calcule les keywords (POST /api/reparse)."""
    import urllib.request
    email_id = getattr(args, "id", None)
    url = f"{_base()}/api/reparse" + (f"?email_id={email_id}" if email_id else "")
    try:
        r = urllib.request.urlopen(
            urllib.request.Request(url, data=b"", method="POST"),
            timeout=30,
        )
        data = json.loads(r.read())
        print(green("Reparse lancé."), data)
    except Exception as e:
        print(red(f"Erreur reparse: {e}"), file=sys.stderr)
        sys.exit(1)


def cmd_remeta(args) -> None:
    """Relance step_meta (POST /api/remeta)."""
    import urllib.request
    email_id = getattr(args, "id", None)
    url = f"{_base()}/api/remeta" + (f"?email_id={email_id}" if email_id else "")
    try:
        r = urllib.request.urlopen(
            urllib.request.Request(url, data=b"", method="POST"),
            timeout=30,
        )
        data = json.loads(r.read())
        print(green("Remeta lancé."), data)
    except Exception as e:
        print(red(f"Erreur remeta: {e}"), file=sys.stderr)
        sys.exit(1)


# ── Parser argparse ───────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bidi",
        description="BiDi CLI/TUI — gestion des téléchargements media\n"
                    "Sans argument : lance le TUI interactif (curses).",
    )
    parser.add_argument("--config", type=Path, metavar="FILE")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("status", help="Statut serveur (stats + logs récents)")

    pl = sub.add_parser("list", help="Liste les emails")
    pl.add_argument("--step", metavar="STEP")
    pl.add_argument("--limit",  type=int, default=50)
    pl.add_argument("--offset", type=int, default=0)

    ps = sub.add_parser("show", help="Détail d'un email")
    ps.add_argument("id", type=int)

    pr = sub.add_parser("run", help="Lance un step (serveur requis)")
    pr.add_argument("step", metavar="STEP",
                    help="fetch|parse|meta|send|check|thumb|llm|all")

    plogs = sub.add_parser("logs", help="Logs temps réel (SSE)")
    plogs.add_argument("--follow", "-f", action="store_true",
                       help="Suivre en continu")

    sub.add_parser("server-status", help="État détaillé du serveur")

    pd = sub.add_parser("delete",  help="Supprime un email de la DB")
    pd.add_argument("id", type=int, help="ID de l'email")

    prp = sub.add_parser("reparse", help="Re-calcule keywords + sync hardlinks")
    prp.add_argument("--id", type=int, default=None, metavar="EMAIL_ID",
                     help="Email ciblé (défaut: tous)")

    prm = sub.add_parser("remeta",  help="Relance step_meta (ré-extraction métadonnées)")
    prm.add_argument("--id", type=int, default=None, metavar="EMAIL_ID",
                     help="Email ciblé (défaut: tous parsed)")

    return parser

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command and hasattr(args, "config") and args.config:
        from config_manager import reset_config
        reset_config()
        get_config(args.config)

    if args.command is None:
        # Mode TUI par défaut
        threading.Thread(target=_poller, daemon=True).start()
        try:
            import curses
            _run_tui()
        except Exception:
            _run_text()
        return

    dispatch = {
        "status":        cmd_status,
        "list":          cmd_list,
        "show":          cmd_show,
        "run":           cmd_run,
        "logs":          cmd_logs,
        "server-status": cmd_server_status,
        "delete":        cmd_delete,
        "reparse":       cmd_reparse,
        "remeta":        cmd_remeta,
    }
    fn = dispatch.get(args.command)
    if fn is None:
        build_parser().print_help()
        sys.exit(1)
    fn(args)


if __name__ == "__main__":
    main()
