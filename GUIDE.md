# Guide: How this project downloads Memories van Successie

## What are we downloading?

When someone died in the Netherlands between 1806 and 1927, their heirs had to register the estate with the local tax office. These registers — *Memories van Successie* — list the deceased's name, date and place of death, their heirs, and what the estate was worth. They are a unique source for family history.

About 150,000 register volumes survive across ten regional archives. Each volume contains anything from a few dozen to over a thousand handwritten pages. This project downloads all of them — scans and metadata — so researchers can work with them offline.

## Why not just browse the websites?

The scans exist online. But every archive uses a different website, a different viewer, and a different way of organising its data. There is no "download all" button. Some archives let you view one page at a time through a clunky in-browser viewer. Others hide the images behind JavaScript that only runs when you click through their interface. None of them expose a simple list of links you can hand to a download tool.

That is what these scripts do: they automate the clicking, the waiting, the page-stepping, and the URL-collecting — tasks a human could do by hand, but which would take months of repetitive work.

## The two kinds of archive software

Most Dutch archives run one of two commercial platforms to serve their scans online.

**MAIS** (by De Ree) is a tree-based inventory viewer. You expand folders in a hierarchy — archive → section → register → page — and a thumbnail strip loads at the bottom. The full-size images are protected by per-page tokens that expire. The browser gets these tokens from JavaScript code that runs when you click on a thumbnail; you cannot simply copy a URL and come back later. Six of the ten archives use MAIS. Because MAIS is configurable, each archive's tree has a slightly different shape, so each needs its own script — but the underlying trick (launch a headless browser, simulate clicks, harvest the URLs that appear) is the same.

**Memorix** (by Picturae) is a REST API behind a search portal. You send a query (e.g. "give me all registers labelled *memorie van successie*") and get back structured data — metadata, file URLs, people linked to deeds. This is much easier to work with than MAIS because the data is machine-readable from the start. Three of the ten archives (Drenthe, BHIC, Tresoar) expose their collections through Memorix.

The Nationaal Archief uses neither system and has its own custom viewer.

## Why three Memorix pipelines look different

Although Drenthe, BHIC, and Tresoar all run Memorix, they attach scans at different levels of the data model:

- **Drenthe** puts scans on individual *deeds* (one entry in a register). Each dead person gets their own folder.
- **BHIC** puts scans on the *register* (the bound book). All pages of the book download into one folder, and a separate `deeds.json` sidecar lists every entry inside it with names and dates.
- **Tresoar** (Friesland) also puts scans at the deed level, but the deeds are linked to *persons*, so the pipeline creates one folder per person. The images are JPEG 2000 files rather than standard JPEGs.

These differences exist because each archive chose its own digitisation workflow — some scanned whole books, others scanned individual entries, and the metadata linking was done differently each time.

## The Playwright part

For the six MAIS archives, the scripts use a tool called Playwright. It launches a real Chromium browser (the same engine inside Google Chrome) but invisibly, without a window. The script tells this browser: "go to this page, wait for the tree to load, click every expand button, then collect every image URL you see." The gathered URLs — each containing a fresh authentication token — are saved to disk so the slow browser step only runs once. After that, a standard downloader fetches all the images.

## What you end up with

```
scans/
├── friesland/Sneek/1234/Pieter_Janssen_abc123/
│   ├── metadata.json
│   └── 0001.jp2 … 0024.jp2
├── bhic/Den_Bosch/deel_5678/
│   ├── metadata.json
│   ├── deeds.json
│   └── DenBosch_044_0001.jpg …
├── overijssel/Zwolle/9012/
│   ├── metadata.json
│   └── 0000.jpg … 0127.jpg
…
```

Every folder gets a `metadata.json` sidecar with the archive name, inventory number, kantoor (tax district), name of the deceased (where available), and the original web URL. From there you can browse, search, or feed the collection into other tools.

## What this project does not do

It does not transcribe handwriting, index names, or turn the scans into searchable text. It only downloads what the archives already published online — just in a form you can actually work with.

---

The scripts live in `python/`. Each file covers one archive. Run `uv run python main.py all` to download everything, or pick individual archives with e.g. `uv run python main.py bhic`. See `README.md` for the full command list and setup instructions.
