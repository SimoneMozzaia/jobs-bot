from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser


@dataclass(frozen=True)
class Anchor:
    href: str
    text: str
    title: str | None = None
    aria_label: str | None = None


class _AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._anchors: list[Anchor] = []
        self._in_a = False
        self._cur_href: str | None = None
        self._cur_title: str | None = None
        self._cur_aria: str | None = None
        self._buf: list[str] = []

    @property
    def anchors(self) -> list[Anchor]:
        return self._anchors

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return

        attrs_map = {k.lower(): (v or "") for k, v in attrs}
        href = (attrs_map.get("href") or "").strip()
        if not href:
            return

        self._in_a = True
        self._cur_href = href
        self._cur_title = (attrs_map.get("title") or "").strip() or None
        self._cur_aria = (attrs_map.get("aria-label") or "").strip() or None
        self._buf = []

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or not self._in_a:
            return

        text = " ".join(" ".join(self._buf).split()).strip()
        href = self._cur_href or ""
        self._anchors.append(
            Anchor(
                href=href,
                text=text,
                title=self._cur_title,
                aria_label=self._cur_aria,
            )
        )
        self._in_a = False
        self._cur_href = None
        self._cur_title = None
        self._cur_aria = None
        self._buf = []

    def handle_data(self, data: str) -> None:
        if not self._in_a:
            return
        if data:
            self._buf.append(data)


def extract_anchors(html: str) -> list[Anchor]:
    """Extract anchor tags from HTML (dependency-free)."""
    parser = _AnchorParser()
    parser.feed(html or "")
    return parser.anchors
