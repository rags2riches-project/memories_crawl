"""Original scan URLs preserve all MAIS authorization tokens (issue #42)."""

from urllib.parse import parse_qsl, urlsplit

import pytest

from memories_crawl import (
    download,
    gelderland,
    limburg,
    noordholland,
    overijssel,
    utrechtsarchief,
    zeeland,
)


@pytest.mark.parametrize(
    "mod,base",
    [
        (noordholland, "https://preserve-nha.archieven.nl/mi-0/fonc-nha/178/1/a.jpg"),
        (utrechtsarchief, "https://img.hetutrechtsarchief.nl/mi-39/hua/archiefbank/a.jpg"),
        (zeeland, "https://preserve-zaf.archieven.nl/mi-239/fonc-zaf/398/1/a.jpg"),
        (gelderland, "https://preserve2.archieven.nl/mi-37/fonc-gea/0021/1/1-0001.jp2"),
    ],
)
@pytest.mark.parametrize(
    "query",
    [
        "format=thumb&miadt=37&miahd=123&mivast=37&rdt=20251205&open=46EC9",
        "open=a%2Bb%26c&miahd=123&format=large&miadt=37&rdt=20251205&mivast=37",
        "miadt=37&miahd=123&mivast=37&rdt=20251205&open=46EC9&format=thumb",
        "miadt=37&miahd=123&mivast=37&rdt=20251205&open=46EC9",
        "format=large&open=&format=thumb&extra=1&extra=2",
        "",
    ],
)
def test_original_url_replaces_format_and_preserves_tokens(mod, base, query):
    thumb = base + "?" + query + "#page"
    result = mod._fullsize_url(thumb)
    parts = urlsplit(result)
    assert parts._replace(query="") == urlsplit(thumb)._replace(query="")
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    assert [(k, v) for k, v in pairs if k == "format"] == [("format", "download")]
    assert [(k, v) for k, v in pairs if k != "format"] == [
        (k, v) for k, v in parse_qsl(query, keep_blank_values=True) if k != "format"
    ]
    assert download.mais_original_url(result) == result


def test_overijssel_original_url():
    url = overijssel._image_url(1, 0, 123, "20200425", "ABC")
    assert urlsplit(url).path.endswith("/1/NL-ZlHCO_0136.4_1_0000.jpg")
    assert parse_qsl(urlsplit(url).query) == [
        ("format", "download"),
        ("miadt", "141"),
        ("miahd", "123"),
        ("mivast", "20"),
        ("rdt", "20200425"),
        ("open", "ABC"),
    ]


def test_limburg_original_url():
    url = limburg._image_url(
        "07.D03", {"invnr": 1, "page": 0, "miahd": 123, "rdt": "20200425", "open": "ABC"}
    )
    assert urlsplit(url).path.endswith("/07.D03/1/NL-MtHCL_07.D03_1_0000.jpg")
    assert parse_qsl(urlsplit(url).query) == [
        ("format", "download"),
        ("miadt", "38"),
        ("miahd", "123"),
        ("mivast", "0"),
        ("rdt", "20200425"),
        ("open", "ABC"),
    ]


@pytest.mark.parametrize("failed_first", [False, True])
def test_utrecht_repairs_legacy_completed_register_and_retries_failures(
    tmp_path, monkeypatch, failed_first
):
    from memories_crawl import paths

    paths.set_out_dir(tmp_path)
    monkeypatch.setattr(utrechtsarchief, "KANTOREN", {"Test": "337-1"})
    monkeypatch.setattr(
        utrechtsarchief, "_discover_subsections", lambda code: [{"minr": "A", "text": "1818"}]
    )
    monkeypatch.setattr(
        utrechtsarchief,
        "_harvest_page_tokens",
        lambda code, minr: [
            {
                "invnr": 1,
                "page": 0,
                "inv_text": "1 1818",
                "thumb_url": "https://example.invalid/scan.jpg?format=thumb&open=ABC",
            }
        ],
    )
    paths.cache_file("utrechtsarchief", "done_Test.txt").write_text("1\n")
    dest = paths.archive_dir("utrechtsarchief") / "Test" / "0001" / "0000.jpg"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(b"\x89PNG\r\n\x1a\npreview")
    calls = []

    class Response:
        status_code = 200

        def iter_content(self, size):
            yield b"<svg/>" if failed_first and len(calls) == 1 else b"\xff\xd8\xfforiginal"

    class Session:
        def get(self, url, **kwargs):
            assert "format=download" in url
            calls.append(url)
            return Response()

    monkeypatch.setattr(utrechtsarchief, "_session", Session)
    monkeypatch.setattr(download.time, "sleep", lambda _: None)
    first = utrechtsarchief.main(workers=1)
    marker = paths.cache_file("utrechtsarchief", "done_originals_Test.txt")
    if failed_first:
        assert first.pages.missing == 1
        assert not marker.exists()
        assert utrechtsarchief.main(workers=1).pages.downloaded == 1
    else:
        assert first.pages.downloaded == 1
    assert dest.read_bytes() == b"\xff\xd8\xfforiginal"
    assert marker.read_text() == "1\n"
    before = len(calls)
    assert utrechtsarchief.main(workers=1).pages.skipped == 1
    assert len(calls) == before


# ---------------------------------------------------------------------------
# Limburg pages are saved as .jpg; pre-0.5.2 .png names are migrated (#42).
# ---------------------------------------------------------------------------

JPEG = b"\xff\xd8\xfforiginal"
PNG_PREVIEW = b"\x89PNG\r\n\x1a\npreview"
LIMBURG_ITEM = {
    "invnr": 1,
    "title": "Amby, 1818-1828",
    "name": "Amby",
    "datering": "1818-1828",
    "axis": "Plaats",
    "minr": 7,
    "hasScan": True,
}


class _LimburgResponse:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self.body = body
        self.headers = {}

    def iter_content(self, size):
        yield self.body


class _LimburgSession:
    """Answers every request with the same scripted response, and records it."""

    def __init__(self, status_code=200, body=JPEG):
        self.status_code = status_code
        self.body = body
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append(url)
        return _LimburgResponse(self.status_code, self.body)


def test_limburg_page_filename_is_jpg():
    tok = {"invnr": 12, "page": 3}
    assert limburg._page_filename("07.D08", tok) == "NL-MtHCL_07.D08_12_0003.jpg"


@pytest.fixture
def limburg_run(tmp_path, monkeypatch):
    """Run limburg.main() over one cached register of two pages, no Playwright."""
    from memories_crawl import paths

    paths.set_out_dir(tmp_path)
    monkeypatch.setattr(limburg, "ARCHIVE_CODES", {"07.D03": limburg.ARCHIVE_CODES["07.D03"]})
    monkeypatch.setattr(limburg, "_harvest_inventory", lambda code: [LIMBURG_ITEM])
    monkeypatch.setattr(limburg, "_harvest_all_tokens", lambda code, items: None)
    monkeypatch.setattr(download.time, "sleep", lambda _: None)
    tokens = [
        {"invnr": 1, "page": page, "miahd": 1, "rdt": "20200425", "open": f"T{page}"}
        for page in (1, 2)
    ]
    limburg._save_json(limburg._tokens_cache_path("07.D03", 1), tokens)
    reg = paths.archive_dir("limburg") / "07.D03" / "1"

    def run(session):
        monkeypatch.setattr(limburg, "_session", lambda: session)
        return limburg.main(workers=1)

    yield reg, run
    paths.set_out_dir(None)


def test_limburg_saves_pages_as_validated_jpg(limburg_run):
    reg, run = limburg_run
    session = _LimburgSession()
    assert run(session).pages.downloaded == 2
    assert sorted(p.name for p in reg.iterdir()) == [
        "NL-MtHCL_07.D03_1_0001.jpg",
        "NL-MtHCL_07.D03_1_0002.jpg",
        "metadata.json",
    ]
    assert all("format=download" in url for url in session.calls)
    # A rerun finds both pages and makes no request.
    assert run(session).pages.skipped == 2
    assert len(session.calls) == 2


def test_limburg_renames_jpeg_saved_as_png_without_downloading(limburg_run):
    """0.5.1 saved the original JPEG under .png: rename it, never fetch it again."""
    reg, run = limburg_run
    reg.mkdir(parents=True)
    for page in (1, 2):
        (reg / f"NL-MtHCL_07.D03_1_{page:04d}.png").write_bytes(JPEG + bytes([page]))
    session = _LimburgSession()
    summary = run(session)
    assert session.calls == []
    assert summary.pages.skipped == 2
    assert not list(reg.glob("*.png"))
    for page in (1, 2):
        assert (reg / f"NL-MtHCL_07.D03_1_{page:04d}.jpg").read_bytes() == JPEG + bytes([page])


def test_limburg_replaces_png_preview_then_removes_it(limburg_run):
    reg, run = limburg_run
    reg.mkdir(parents=True)
    for page in (1, 2):
        (reg / f"NL-MtHCL_07.D03_1_{page:04d}.png").write_bytes(PNG_PREVIEW)
    session = _LimburgSession()
    assert run(session).pages.downloaded == 2
    assert len(session.calls) == 2
    assert not list(reg.glob("*.png"))
    assert (reg / "NL-MtHCL_07.D03_1_0001.jpg").read_bytes() == JPEG


@pytest.mark.parametrize(
    "status,body",
    [
        (200, b"<svg>placeholder</svg>"),
        (200, PNG_PREVIEW),
        (200, b"<html>error</html>"),
        (403, b""),
    ],
)
def test_limburg_failed_download_keeps_legacy_preview(limburg_run, status, body):
    reg, run = limburg_run
    reg.mkdir(parents=True)
    legacy = reg / "NL-MtHCL_07.D03_1_0001.png"
    legacy.write_bytes(PNG_PREVIEW)
    summary = run(_LimburgSession(status, body))
    assert summary.pages.downloaded == 0
    assert legacy.read_bytes() == PNG_PREVIEW
    assert not list(reg.glob("*.jpg"))
    assert not list(reg.glob("*.part"))
    # The next run with a working server still repairs it.
    assert run(_LimburgSession()).pages.downloaded == 2
    assert not legacy.exists()


def test_limburg_202_placeholder_is_missing_and_keeps_legacy(tmp_path):
    legacy = tmp_path / "NL-MtHCL_07.D03_1_0001.png"
    legacy.write_bytes(PNG_PREVIEW)
    dest = legacy.with_suffix(".jpg")
    session = _LimburgSession(202, b"<svg/>")
    assert limburg._download_one(session, "https://x.invalid/a", dest) == "missing"
    assert legacy.read_bytes() == PNG_PREVIEW
    assert not dest.exists()


def test_limburg_existing_jpg_drops_stale_legacy_copy(tmp_path):
    dest = tmp_path / "NL-MtHCL_07.D03_1_0001.jpg"
    dest.write_bytes(JPEG)
    legacy = dest.with_suffix(".png")
    legacy.write_bytes(PNG_PREVIEW)
    session = _LimburgSession()
    assert limburg._download_one(session, "https://x.invalid/a", dest) == "exists"
    assert session.calls == []
    assert dest.read_bytes() == JPEG
    assert not legacy.exists()


def test_limburg_calls_fetch_file_through_the_module(tmp_path, monkeypatch):
    """Wrappers (e.g. a downscaling crawl) patch download.fetch_file at runtime."""
    seen = []

    def patched(session, url, dest, **kwargs):
        seen.append((dest, kwargs))
        dest.write_bytes(JPEG)
        return "downloaded"

    monkeypatch.setattr(download, "fetch_file", patched)
    dest = tmp_path / "NL-MtHCL_07.D03_1_0001.jpg"
    legacy = dest.with_suffix(".png")
    legacy.write_bytes(PNG_PREVIEW)
    assert limburg._download_one(_LimburgSession(), "https://x.invalid/a", dest) == "downloaded"
    assert seen == [(dest, {"missing_statuses": (202, 404)})]
    assert not legacy.exists()
