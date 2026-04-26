"""
File: test_email_parser.py
Path: tests/test_email_parser.py

Version: 1.0.0
Date: 2026-04-16
"""

import pytest
from email_parser import parse_email_body, ParseResult

KNOWN = ["modele", "pose", "concept", "reference", "background"]


class TestBareUrl:
    def test_should_extract_reddit_url(self):
        body = "https://www.reddit.com/r/test/comments/abc123/title/"
        r = parse_email_body(body, KNOWN)
        assert r.url == "https://www.reddit.com/r/test/comments/abc123/title/"

    def test_should_extract_pornhub_url(self):
        body = "https://fr.pornhub.com/view_video.php?viewkey=abc"
        r = parse_email_body(body, KNOWN)
        assert r.url == "https://fr.pornhub.com/view_video.php?viewkey=abc"

    def test_should_extract_x_url(self):
        body = "check this: https://x.com/user/status/123"
        r = parse_email_body(body, KNOWN)
        assert r.url == "https://x.com/user/status/123"

    def test_should_return_no_url_for_unknown_platform(self):
        body = "https://example.com/page"
        r = parse_email_body(body, KNOWN)
        assert not r.has_url


class TestMarkdownUrl:
    def test_should_extract_markdown_link(self):
        body = "[voir ici](https://reddit.com/r/art/comments/xyz/)"
        r = parse_email_body(body, KNOWN)
        assert r.url == "https://reddit.com/r/art/comments/xyz/"


class TestHrefUrl:
    def test_should_extract_href(self):
        body = '<a href="https://www.reddit.com/r/foo/comments/bar/">lien</a>'
        r = parse_email_body(body, KNOWN)
        assert r.url == "https://www.reddit.com/r/foo/comments/bar/"

    def test_should_decode_html_entities(self):
        body = '<a href="https://www.reddit.com/r/foo?a=1&amp;b=2">lien</a>'
        r = parse_email_body(body, KNOWN)
        assert r.url and "a=1&b=2" in r.url


class TestKeywords:
    def test_should_extract_known_keyword(self):
        body = "https://reddit.com/r/art/comments/abc/\nmodele pose"
        r = parse_email_body(body, KNOWN)
        assert "modele" in r.known_kws
        assert "pose" in r.known_kws
        assert r.unknown_kws == []

    def test_should_separate_unknown_keywords(self):
        body = "https://reddit.com/r/art/comments/abc/\nmodele fantasy"
        r = parse_email_body(body, KNOWN)
        assert "modele" in r.known_kws
        assert "fantasy" in r.unknown_kws

    def test_should_deduplicate_keywords(self):
        body = "https://reddit.com/r/abc/\nmodele modele pose"
        r = parse_email_body(body, KNOWN)
        assert r.all_kws.count("modele") == 1

    def test_should_not_pick_keywords_from_second_url_line(self):
        body = "https://reddit.com/r/abc/\nhttps://reddit.com/r/xyz/"
        r = parse_email_body(body, KNOWN)
        assert r.known_kws == []
        assert r.unknown_kws == []


class TestEdgeCases:
    def test_should_return_empty_on_empty_body(self):
        r = parse_email_body("", KNOWN)
        assert not r.has_url
        assert r.all_kws == []

    def test_should_return_empty_on_none_body(self):
        r = parse_email_body(None, KNOWN)
        assert not r.has_url

    def test_should_handle_url_continuation_line(self):
        # Ligne coupée par un client mail
        body = "https://reddit.com/r/art/comments/abc123/\nmy_title_suffix/"
        r = parse_email_body(body, KNOWN)
        assert r.url and "my_title_suffix" in r.url

    def test_should_strip_trailing_punctuation(self):
        body = "https://reddit.com/r/art/comments/abc/."
        r = parse_email_body(body, KNOWN)
        assert r.url and not r.url.endswith(".")

    def test_parse_result_has_url_property(self):
        r = ParseResult(url="https://reddit.com/r/x/")
        assert r.has_url is True

    def test_parse_result_no_url_property(self):
        r = ParseResult()
        assert r.has_url is False
