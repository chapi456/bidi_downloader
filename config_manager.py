"""
File: config_manager.py
Path: config_manager.py

Version: 5.4.0
Date: 2026-04-24

Changelog:
- 5.4.0 (2026-04-24): Ajout section reddit dans DEFAULTS. get_reddit_cookies_path() résout le
                      chemin du fichier cookies relatif au fichier config ou en absolu.
                      Compatibilité : get_reddit_cookies() conservé comme alias.
- 5.3.0 (2026-04-22): poll_interval dans server. get_poll_interval().
- 5.2.0 (2026-04-21): Port défaut 8000. Log config trouvée/non trouvée avec chemin absolu résolu et CWD.
- 5.1.0 (2026-04-19): Ajout champs server host/cors/auth.
- 5.0.1 (2026-04-16): Support YAML (pyyaml).
- 5.0.0 (2026-04-16): Création.
"""

import json
import logging
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


def config_search_paths() -> list:
    """Cherche config.yaml dans dossier du module, CWD, puis home."""
    module_dir = Path(__file__).resolve().parent
    return [
        module_dir / "config.yaml",
        module_dir / "config.json",
        Path("config.yaml").resolve(),
        Path("config.json").resolve(),
        Path("~/.config/bidi/config.yaml").expanduser(),
        Path("~/.config/bidi/config.json").expanduser(),
    ]


DEFAULTS: dict = {
    "general": {
        "save_dir": "save",
        "db_path": "bidi.db",
        "log_level": "INFO",
    },
    "server": {
        "host": "0.0.0.0",
        "port": 8000,
        "cors_enabled": False,
        "cors_origins": [],
        "auth_enabled": False,
        "auth_user": "bidi",
        "auth_password": "",
        "poll_interval": 5,
    },
    "imap": {
        "server": "",
        "port": 993,
        "use_ssl": True,
        "user": "",
        "password": "",
        "folder": "INBOX",
        "max_emails": 50,
    },
    "jdownloader": {
        "enabled": True,
        "email": "",
        "password": "",
        "device": "",
        "timeout": 600,
        "max_parallel": 10,
        "watch_dir": "",
        "use_api": True,
    },
    "gallery_dl": {
        "enabled": True,
        "max_parallel": 1,
        "extra_args": [],
        "timeout": 180,
    },
    "yt_dlp": {
        "enabled": True,
        "max_parallel": 1,
        "extra_args": [],
        "timeout": 300,
    },
    "llm": {
        "enabled": False,
        "host": "http://localhost:11434",
        "model": "llama3.2:latest",
        "auto_process": False,
    },
    # Section Reddit : cookies Netscape pour gallery-dl + API Reddit
    # Le chemin peut être absolu ou relatif au fichier config.yaml
    # Exemple config.yaml :
    #   reddit:
    #     cookies_path: www.reddit.com_cookies.txt
    "reddit": {
        "cookies_path": "",
    },
    "keywords": [],
}


def deep_merge(base: dict, override: dict) -> dict:
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def load_file(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    if path.suffix in (".yaml", ".yml"):
        try:
            import yaml
            return yaml.safe_load(text) or {}
        except ImportError:
            raise ImportError("PyYAML requis : pip install pyyaml")
    return json.loads(text)


def save_file(data: dict, path: Path) -> None:
    if path.suffix in (".yaml", ".yml"):
        try:
            import yaml
            path.write_text(
                yaml.dump(data, allow_unicode=True, default_flow_style=False, sort_keys=False),
                encoding="utf-8",
            )
        except ImportError:
            raise ImportError("PyYAML requis : pip install pyyaml")
    else:
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


class ConfigManager:
    def __init__(self, config_path: Optional[Path] = None):
        self.path = config_path or self._find_config()
        self.data = self._load()

    @staticmethod
    def _find_config() -> Optional[Path]:
        import os
        cwd = Path(os.getcwd()).resolve()
        for p in config_search_paths():
            if p.exists():
                print(f"Config: Fichier trouvé → {p.resolve()}  (CWD={cwd})")
                return p
        print(f"Config: ATTENTION aucun fichier config trouvé. CWD={cwd} — valeurs par défaut utilisées.")
        return None

    def _load(self) -> dict:
        data = json.loads(json.dumps(DEFAULTS))
        if self.path and self.path.exists():
            try:
                user = load_file(self.path)
                data = deep_merge(data, user)
                logger.info(
                    f"Config: Chargé {self.path.resolve()} "
                    f"port={data.get('server', {}).get('port', '?')}  "
                    f"CWD={Path.cwd().resolve()}"
                )
            except Exception as e:
                logger.warning(f"Config: Lecture impossible {self.path}: {e} — defaults utilisés")
        else:
            logger.warning(f"Config: Aucun fichier config — défauts port=8000. CWD={Path.cwd().resolve()}")
        return data

    def save(self, path: Optional[Path] = None) -> Path:
        dest = path or self.path or Path("config.json")
        dest.parent.mkdir(parents=True, exist_ok=True)
        save_file(self.data, dest)
        logger.info(f"Config: Sauvegardé → {dest}")
        return dest

    def get(self, *keys, default: Any = None) -> Any:
        node = self.data
        for k in keys:
            if not isinstance(node, dict) or k not in node:
                return default
            node = node[k]
        return node

    # ------------------------------------------------------------------ General
    def get_save_dir(self) -> Path:
        return Path(self.get("general", "save_dir", default="save"))

    def get_db_path(self) -> Path:
        return Path(self.get("general", "db_path", default="bidi.db"))

    def get_log_level(self) -> str:
        return self.get("general", "log_level", default="INFO")

    # ------------------------------------------------------------------ Server
    def get_server_host(self) -> str:
        return self.get("server", "host", default="0.0.0.0")

    def get_server_port(self) -> int:
        return int(self.get("server", "port", default=8000))

    def get_poll_interval(self) -> int:
        return int(self.get("server", "poll_interval", default=5))

    def get_server_url(self) -> str:
        host = self.get_server_host()
        if host in ("0.0.0.0", ""):
            host = "127.0.0.1"
        return f"http://{host}:{self.get_server_port()}"

    def get_cors_enabled(self) -> bool:
        return bool(self.get("server", "cors_enabled", default=False))

    def get_cors_origins(self) -> list:
        return list(self.get("server", "cors_origins", default=[]))

    def get_auth_enabled(self) -> bool:
        return bool(self.get("server", "auth_enabled", default=False))

    def get_auth_user(self) -> str:
        return self.get("server", "auth_user", default="bidi")

    def get_auth_password(self) -> str:
        return self.get("server", "auth_password", default="")

    # ------------------------------------------------------------------ IMAP
    def get_imap_server(self) -> str:
        return self.get("imap", "server", default="")

    def get_imap_port(self) -> int:
        return int(self.get("imap", "port", default=993))

    def get_imap_ssl(self) -> bool:
        return bool(self.get("imap", "use_ssl", default=True))

    def get_imap_user(self) -> str:
        return self.get("imap", "user", default="")

    def get_imap_password(self) -> str:
        return self.get("imap", "password", default="")

    def get_imap_folder(self) -> str:
        return self.get("imap", "folder", default="INBOX")

    def get_imap_max(self) -> int:
        return int(self.get("imap", "max_emails", default=50))

    # ------------------------------------------------------------------ JDownloader
    def get_jd_enabled(self) -> bool:
        return bool(self.get("jdownloader", "enabled", default=True))

    def get_jd_email(self) -> str:
        return self.get("jdownloader", "email", default="")

    def get_jd_password(self) -> str:
        return self.get("jdownloader", "password", default="")

    def get_jd_device(self) -> str:
        return self.get("jdownloader", "device", default="")

    def get_jd_timeout(self) -> int:
        return int(self.get("jdownloader", "timeout", default=600))

    def get_jd_max_parallel(self) -> int:
        return int(self.get("jdownloader", "max_parallel", default=10))

    def get_jd_watch_dir(self) -> str:
        return self.get("jdownloader", "watch_dir", default="")

    def get_jd_use_api(self) -> bool:
        return bool(self.get("jdownloader", "use_api", default=True))

    # ------------------------------------------------------------------ gallery-dl
    def get_gdl_enabled(self) -> bool:
        return bool(self.get("gallery_dl", "enabled", default=True))

    def get_gdl_max_parallel(self) -> int:
        return int(self.get("gallery_dl", "max_parallel", default=1))

    def get_gdl_extra_args(self) -> list:
        return list(self.get("gallery_dl", "extra_args", default=[]))

    def get_gdl_timeout(self) -> int:
        return int(self.get("gallery_dl", "timeout", default=180))

    # ------------------------------------------------------------------ yt-dlp
    def get_ytdlp_enabled(self) -> bool:
        return bool(self.get("yt_dlp", "enabled", default=True))

    def get_ytdlp_max_parallel(self) -> int:
        return int(self.get("yt_dlp", "max_parallel", default=1))

    def get_ytdlp_extra_args(self) -> list:
        return list(self.get("yt_dlp", "extra_args", default=[]))

    def get_ytdlp_timeout(self) -> int:
        """Timeout pour step_send (download réel). NE PAS utiliser pour step_meta."""
        return int(self.get("yt_dlp", "timeout", default=300))

    # ------------------------------------------------------------------ LLM
    def get_llm_enabled(self) -> bool:
        return bool(self.get("llm", "enabled", default=False))

    def get_llm_host(self) -> str:
        return self.get("llm", "host", default="http://localhost:11434")

    def get_llm_model(self) -> str:
        return self.get("llm", "model", default="llama3.2:latest")

    def get_llm_auto_process(self) -> bool:
        return bool(self.get("llm", "auto_process", default=False))

    # ------------------------------------------------------------------ Reddit / Cookies
    def get_reddit_cookies_path(self) -> Optional[str]:
        """
        Retourne le chemin absolu du fichier cookies Reddit (format Netscape).

        Résolution :
          1. Valeur vide / absente → None (pas de cookies)
          2. Chemin absolu → utilisé tel quel
          3. Chemin relatif → résolu par rapport au répertoire du fichier config,
             puis par rapport au CWD si config non trouvée.

        Exemple config.yaml :
          reddit:
            cookies_path: www.reddit.com_cookies.txt   # dans le même dossier que config.yaml
            # ou :
            cookies_path: /home/user/.config/bidi/reddit_cookies.txt
        """
        raw = self.get("reddit", "cookies_path", default="") or ""
        if not raw:
            return None
        p = Path(raw)
        if p.is_absolute():
            resolved = p
        else:
            # Relatif au dossier du fichier config
            base = self.path.parent if self.path else Path.cwd()
            resolved = (base / p).resolve()

        if not resolved.exists():
            logger.warning(f"Config: cookies Reddit introuvable : {resolved}")
            return None

        logger.info(f"Config: cookies Reddit → {resolved}")
        return str(resolved)

    # Alias rétrocompatibilité (ancienne API)
    def get_reddit_cookies(self) -> Optional[str]:
        return self.get_reddit_cookies_path()

    # ------------------------------------------------------------------ Keywords
    def get_keywords(self) -> list:
        return list(self.get("keywords", default=[]))


# Singleton
_instance: Optional[ConfigManager] = None


def get_config(config_path: Optional[Path] = None) -> ConfigManager:
    global _instance
    if _instance is None:
        _instance = ConfigManager(config_path)
    return _instance


def reset_config() -> None:
    global _instance
    _instance = None
