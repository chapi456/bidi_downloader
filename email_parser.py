"""
File: email_parser.py
Path: email_parser.py

Version: 2.0.0
Date: 2026-04-16

Changelog:
- 2.0.0 (2026-04-16): ParseResult dataclass
- 1.2.0 (2026-02-28): Extraction URL robuste
- 1.0.0 (2026-02-17): Version initiale
"""

import re
import html
import logging
from dataclasses import dataclass, field
from typing import Optional, List

logger = logging.getLogger(__name__)

_PLATFORMS = (
    r'reddit\.com|redd\.it'
    r'|twitter\.com|x\.com'
    r'|grok\.com'
    r'|redgifs?\.com'
    r'|(?:[a-z]{2}\.)?pornhub\.com'
)

_URL_RE = re.compile(
    r'https?://(?:www\.)?(?:' + _PLATFORMS + r')[^\s<>]*',
    re.IGNORECASE,
)

_HREF_RE = re.compile(
    r'<a[^>]+href=["\']?(https?://[^"\'>\s]+)["\']?[^>]*>',
    re.IGNORECASE,
)

_MARKDOWN_RE = re.compile(
    r'\[[^\]]*\]\((https?://[^)]+)\)',
)

_TOKEN_RE = re.compile(r'[a-zA-Z0-9_]+')


@dataclass
class ParseResult:
    url: Optional[str] = None
    known_kws: list = field(default_factory=list)
    unknown_kws: list = field(default_factory=list)

    @property
    def has_url(self) -> bool:
        return self.url is not None

    @property
    def all_kws(self) -> list:
        return self.known_kws + self.unknown_kws


def _clean_url(raw: str) -> str:
    url = html.unescape(raw)
    url = re.split(r'["\'<>\s]', url)[0]
    return url.rstrip(".,;)>]")


def _has_url(line: str) -> bool:
    return bool(re.search(r'https?://', line, re.IGNORECASE))


def _extract_url(line: str) -> Optional[str]:
    m = _HREF_RE.search(line)
    if m:
        return _clean_url(m.group(1))
    m = _MARKDOWN_RE.search(line)
    if m:
        return _clean_url(m.group(1))
    m = _URL_RE.search(line)
    if m:
        return _clean_url(m.group(0))
    return None


def _join_continuation_lines(lines: list) -> list:
    result = []
    for line in lines:
        s = line.strip()
        if (result and _has_url(result[-1]) and s
                and not s.startswith(('http', '[', '<'))
                and re.match(r'^[a-zA-Z0-9_/\-]+/?$', s)):
            result[-1] = result[-1].rstrip() + s
        else:
            result.append(line)
    return result


def parse_email_body(body, known_keywords: List[str]) -> ParseResult:
    """Extrait URL + mots-clés depuis le corps d'un email."""
    if not body:
        return ParseResult()

    lines = [
        ln.strip()
        for ln in _join_continuation_lines(body.splitlines())
        if ln.strip()
    ]

    url: Optional[str] = None
    url_idx: int = -1
    for idx, line in enumerate(lines):
        candidate = _extract_url(line)
        if candidate:
            url, url_idx = candidate, idx
            break

    if url is None:
        logger.debug("parse_email_body: aucune URL trouvée")
        return ParseResult()

    raw_kws: list = []
    for nxt in lines[url_idx + 1: url_idx + 4]:
        if _has_url(nxt):
            break
        tokens = [t.lower() for t in nxt.split() if _TOKEN_RE.match(t)]
        if tokens:
            raw_kws = tokens
            break

    seen: set = set()
    deduped: list = []
    for kw in raw_kws:
        if kw not in seen:
            seen.add(kw)
            deduped.append(kw)

    known_lower = {k.lower() for k in known_keywords}
    return ParseResult(
        url=url,
        known_kws=[kw for kw in deduped if kw in known_lower],
        unknown_kws=[kw for kw in deduped if kw not in known_lower],
    )