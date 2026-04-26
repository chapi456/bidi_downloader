"""
File: test_config.py
Path: tests/test_config.py

Version: 5.0.0
Date: 2026-04-16

Changelog:
- 5.0.0 (2026-04-16): Création — tests unitaires ConfigManager
"""

import json
import pytest
from pathlib import Path
from config_manager import ConfigManager, get_config, reset_config, _deep_merge


@pytest.fixture(autouse=True)
def clean_singleton():
    reset_config()
    yield
    reset_config()


class TestDefaults:
    def test_should_load_defaults_without_file(self, tmp_path, monkeypatch):
        # On se place dans un dossier vide : aucun config.yaml/json ne sera trouvé
        monkeypatch.chdir(tmp_path)
        reset_config()
        cfg = ConfigManager(config_path=None)
        assert cfg.get_save_dir() == Path("_save")
        assert cfg.get_db_path() == Path("bidi.db")
        assert cfg.get_imap_port() == 993
        assert cfg.get_imap_ssl() is True
        assert cfg.get_server_port() == 5000
        assert cfg.get_jd_max_parallel() == 10
        assert cfg.get_llm_enabled() is False
        assert cfg.get_keywords() == []

    def test_should_build_server_url(self):
        cfg = ConfigManager()
        assert cfg.get_server_url() == "http://127.0.0.1:5000"


class TestLoading:
    def test_should_load_from_file(self, tmp_path):
        f = tmp_path / "config.json"
        f.write_text(json.dumps({
            "general": {"save_dir": "/data/saves", "db_path": "/data/bidi.db"},
            "imap": {"server": "imap.example.com", "user": "user@example.com"},
            "keywords": ["kw1", "kw2"],
        }), encoding="utf-8")
        cfg = ConfigManager(config_path=f)
        assert cfg.get_save_dir() == Path("/data/saves")
        assert cfg.get_imap_server() == "imap.example.com"
        assert cfg.get_keywords() == ["kw1", "kw2"]

    def test_should_merge_with_defaults(self, tmp_path):
        f = tmp_path / "config.json"
        f.write_text(json.dumps({"imap": {"server": "imap.test.com"}}))
        cfg = ConfigManager(config_path=f)
        assert cfg.get_imap_server() == "imap.test.com"
        assert cfg.get_imap_port() == 993  # default conservé

    def test_should_fallback_to_defaults_on_invalid_json(self, tmp_path):
        f = tmp_path / "config.json"
        f.write_text("pas du json { } [[ }", encoding="utf-8")
        cfg = ConfigManager(config_path=f)
        assert cfg.get_server_port() == 5000

    def test_should_override_nested_value(self, tmp_path):
        f = tmp_path / "config.json"
        f.write_text(json.dumps({"server": {"port": 8080}}))
        cfg = ConfigManager(config_path=f)
        assert cfg.get_server_port() == 8080
        assert cfg.get_server_host() == "127.0.0.1"  # default conservé


class TestSave:
    def test_should_save_to_file(self, tmp_path):
        cfg = ConfigManager()
        out = tmp_path / "out.json"
        cfg.save(out)
        assert out.exists()
        data = json.loads(out.read_text())
        assert "general" in data
        assert "imap" in data
        assert "jdownloader" in data

    def test_should_create_parent_dirs_on_save(self, tmp_path):
        cfg = ConfigManager()
        deep = tmp_path / "a" / "b" / "config.json"
        cfg.save(deep)
        assert deep.exists()


class TestSingleton:
    def test_should_return_same_instance(self):
        cfg1 = get_config()
        cfg2 = get_config()
        assert cfg1 is cfg2

    def test_should_create_new_instance_after_reset(self):
        cfg1 = get_config()
        reset_config()
        cfg2 = get_config()
        assert cfg1 is not cfg2


class TestDeepMerge:
    def test_should_override_scalar(self):
        result = _deep_merge({"a": 1, "b": 2}, {"b": 99})
        assert result == {"a": 1, "b": 99}

    def test_should_add_new_key(self):
        result = _deep_merge({"a": 1}, {"b": 2})
        assert result["b"] == 2

    def test_should_merge_nested_dicts(self):
        base = {"sec": {"a": 1, "b": 2}}
        over = {"sec": {"b": 99, "c": 3}}
        result = _deep_merge(base, over)
        assert result["sec"] == {"a": 1, "b": 99, "c": 3}

    def test_should_not_mutate_base(self):
        base = {"sec": {"a": 1}}
        _deep_merge(base, {"sec": {"a": 99}})
        assert base["sec"]["a"] == 1

    def test_should_replace_list_entirely(self):
        # Les listes ne sont pas mergées élément par élément
        result = _deep_merge({"kw": ["a", "b"]}, {"kw": ["c"]})
        assert result["kw"] == ["c"]
