# AGENTS.md

## Repository purpose

This repository contains a 3-step Python pipeline for collecting and downloading scans for the Dutch archival series *Memories van Successie* from Open Archieven.

The pipeline is intentionally split into independent scripts:

1. `python/step1_collect_record_guids_from_search_api.py`
   - queries the Open Archieven search API
   - writes a CSV of matching records
2. `python/step2_oai_pmh_dumps.py`
   - reads archive XML dump files
   - extracts scan URLs from `a2a:A2A` records
   - writes a CSV of scan URLs
3. `python/step3_download_steps.py`
   - downloads the scans listed by step 2

## Current project state

- Dependency management is done with `uv`.
- Runtime dependency: `requests`
- Dev dependency: `pytest`
- Tests live in `tests/`.
- Step 1 currently behaves as a small sample run: it writes the first 2 records per archive.
- Step 2 currently behaves as a sample-oriented extractor: it writes about half of the scan URLs per record, with a minimum of 1.
- Step 2 uses streaming gzip XML parsing with a UTF-8-safe sanitizer.

## Key files

- `python/step1_collect_record_guids_from_search_api.py`
- `python/step2_oai_pmh_dumps.py`
- `python/step3_download_steps.py`
- `tests/test_python/step1_collect_record_guids_from_search_api.py`
- `tests/test_python/step2_oai_pmh_dumps.py`
- `tests/test_python/step3_download_steps.py`
- `README.md`
- `pyproject.toml`

## How to run

Use `uv` for all Python execution.

### Run scripts

```bash
uv run python python/step1_collect_record_guids_from_search_api.py
uv run python python/step2_oai_pmh_dumps.py
uv run python python/step3_download_steps.py
```

### Run tests

```bash
uv run pytest -q
```

### Install/sync dependencies

```bash
uv sync
```

## Coding conventions for this repo

- Keep the scripts simple and script-oriented unless there is a strong reason to refactor.
- Prefer small helper functions over large rewrites.
- Preserve CSV formats unless the user explicitly asks to change them.
- Match the current standard-library-heavy style.
- Avoid adding new dependencies unless necessary.
- Use `requests.Session()` for networked steps.
- When changing step 2, be careful with memory usage: the real dumps are very large.
- When changing parsing logic in step 2, preserve the streaming approach if possible.
- When changing test behavior, update `README.md` if the documented behavior changes.

## Step-specific guidance

### Step 1

- Output columns must remain:
  - `archive`
  - `record_id`
  - `url`
- Current tests expect the script to write only the first 2 records per archive.
- Be careful when changing record ID extraction fallback behavior.

### Step 2

- This is the most fragile part of the repository.
- The dumps may contain malformed byte sequences.
- The sanitizer currently works by:
  - incremental UTF-8 decode with replacement
  - removal of XML-disallowed control characters
  - removal of replacement characters introduced by malformed sequences
- The parser uses `xml.etree.ElementTree.XMLPullParser` and yields `A2A` records incrementally.
- Output columns must remain:
  - `archive`
  - `record_id`
  - `page_seq`
  - `scan_uri`
- Existing CLI options:
  - `--output`
  - `--dumps-dir`
  - `--limit-per-archive`
  - `--archives`

### Step 3

- Files are written to `output_dir / archive / record_id / f"{page_seq}{suffix}"`.
- Existing files with non-zero size are skipped.
- The suffix is inferred from the scan URL path, with `.jpg` as fallback.

## Testing expectations

Before finishing code changes, run:

```bash
uv run pytest -q
```

If you change script CLI behavior or output structure, update tests accordingly.

## Local data and generated outputs

Treat these as local/untracked unless explicitly asked otherwise:

- `dumps/`
- `test_results/`

Do not commit archive dumps or downloaded scan output.

## Common safe tasks for agents

- improve parsing robustness in step 2
- add or update pytest coverage
- update README examples and usage docs
- add small CLI options for targeted validation
- fix CSV generation or download path bugs

## Things to be careful about

- Do not assume full-dataset runs are cheap; the archive dumps are large.
- Do not replace the streaming parser in step 2 with full-file parsing unless explicitly requested.
- Do not commit `dumps/` or `test_results/`.
- Do not invent APIs or URLs; rely on the existing Open Archieven endpoints already used in the scripts.
