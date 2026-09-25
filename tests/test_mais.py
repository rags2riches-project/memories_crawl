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
