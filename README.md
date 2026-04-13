# Memories Crawl

This project is a 3-step pipeline for collecting downloadable scan URLs for the Dutch archival series called *Memories van Successie*.

The goal is to move from a large public metadata search index to a list of direct file URLs, and then download the scans to disk.

## Why this exists

Open Archieven aggregates archival metadata from several Dutch archives. That metadata includes record identifiers and, in many cases, links to scans.

The problem is that the site does not give you one simple national download. Instead, the data is spread across:

- a search API for finding records
- archive dumps containing richer XML metadata
- direct scan URLs that can be used to download the actual images

This repository breaks that into three small scripts so each step is easy to run and retry.

## The 3 steps

### Step 1: collect record GUIDs from the search API

File: `step1_collect_record_guids_from_search_api.py`

What it does:
- Queries the Open Archieven search API archive by archive
- Filters results for `Memories van Successie`
- Extracts the record identifier for each matching record
- Writes the results to `records.csv`

Why this step matters:
- It builds the master list of records we care about
- This is the starting point for everything else
- Without record IDs, we cannot resolve scan URLs or download anything

Output:
- `records.csv`
- Columns: `archive, record_id, url`

### Step 2: extract scan URLs from OAI-PMH/XML dumps

File: `step2_oai_pmh_dumps.py`

What it does:
- Downloads the per-archive dump files from Open Archieven
- Unpacks the archive files
- Reads the XML metadata inside them
- Finds the scan URLs embedded in the record metadata
- Writes those scan URLs to `scan_urls.csv`

Why this step matters:
- The scan URLs are not in the search results themselves
- The dumps contain richer metadata than the search API
- This is the fastest way to get direct download links for many records without making hundreds of thousands of API calls

Output:
- `scan_urls.csv`
- Columns: `archive, record_id, page_seq, scan_uri`

### Step 3: download the scans

File: `step3_download_steps.py`

What it does:
- Reads `scan_urls.csv`
- Downloads each scan URL
- Saves the files into folders by archive and record ID

Why this step matters:
- This is the actual download phase
- It turns a list of URLs into files on your hard drive
- The folder structure makes the files easier to resume, inspect, and organize

Output:
- A `scans/` directory with this structure:

```text
scans/
  archive_code/
    record_id/
      1.jpg
      2.jpg
      3.jpg
```

## Typical workflow

Run the scripts in order:

1. `python step1_collect_record_guids_from_search_api.py`
2. `python step2_oai_pmh_dumps.py`
3. `python step3_download_steps.py`

## Dependencies

This project uses:
- `requests`

Install and run with `uv`.

## Notes

- The scripts are written to be resilient to small response-format differences.
- The project is designed for large-scale harvesting, so it uses simple retry-friendly file outputs.
- The Open Archieven API may rate-limit requests, so running Step 1 or Step 2 repeatedly in quick succession may require delays.

## Important limitation

This repository does not guarantee that every record has a downloadable scan.
Some records may be indexed in the metadata but not yet available as images.
