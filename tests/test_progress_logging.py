"""Tests for progress logging and checkpointing in the Open Archieven pipeline."""
from __future__ import annotations

import subprocess
import sys


def test_step1_checkpoint_functions():
    """Test that step 1 checkpoint load/save functions work."""
    result = subprocess.run(
        [sys.executable, "-c", """
import json
import sys
sys.path.insert(0, '.')
from pathlib import Path

# Test _load_checkpoint returns None when no file
from python.step1_collect_record_guids_from_search_api import _load_checkpoint, _save_checkpoint

result = _load_checkpoint()
assert result is None, f'Expected None, got {result}'

# Test _save_checkpoint creates file
_save_checkpoint({'test': 'data'})
assert Path('step1_checkpoint.json').exists(), 'Checkpoint file should exist'

with open('step1_checkpoint.json') as f:
    data = json.load(f)
assert data == {'test': 'data'}, f'Expected test data, got {data}'

# Cleanup
Path('step1_checkpoint.json').unlink()
print('PASS')
"""],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Script failed: {result.stderr}"
    assert "PASS" in result.stdout


def test_step2_checkpoint_functions():
    """Test that step 2 checkpoint load/save functions work."""
    result = subprocess.run(
        [sys.executable, "-c", """
import json
import sys
sys.path.insert(0, '.')
from pathlib import Path

from python.step2_oai_pmh_dumps import _load_checkpoint, _save_checkpoint

result = _load_checkpoint()
assert result is None, f'Expected None, got {result}'

_save_checkpoint({'test': 'data'})
assert Path('step2_checkpoint.json').exists(), 'Checkpoint file should exist'

with open('step2_checkpoint.json') as f:
    data = json.load(f)
assert data == {'test': 'data'}, f'Expected test data, got {data}'

Path('step2_checkpoint.json').unlink()
print('PASS')
"""],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Script failed: {result.stderr}"
    assert "PASS" in result.stdout


def test_step3_checkpoint_functions():
    """Test that step 3 checkpoint load/save functions work."""
    result = subprocess.run(
        [sys.executable, "-c", """
import json
import sys
sys.path.insert(0, '.')
from pathlib import Path

from python.step3_download_steps import _load_checkpoint, _save_checkpoint

result = _load_checkpoint()
assert result is None, f'Expected None, got {result}'

_save_checkpoint({'last_row': 123})
assert Path('step3_checkpoint.json').exists(), 'Checkpoint file should exist'

with open('step3_checkpoint.json') as f:
    data = json.load(f)
assert data == {'last_row': 123}, f'Expected test data, got {data}'

Path('step3_checkpoint.json').unlink()
print('PASS')
"""],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Script failed: {result.stderr}"
    assert "PASS" in result.stdout


def test_step1_has_progress_logging():
    """Test that step 1 print statements include progress metrics."""
    from pathlib import Path

    source = Path("python/step1_collect_record_guids_from_search_api.py").read_text()
    # Check for progress-related patterns in the source
    assert "offset=" in source, "Source should contain offset logging"
    assert "unique_records=" in source, "Source should contain unique records logging"
    assert "pages/s" in source, "Source should contain rate logging"
    assert "PROGRESS_LOG_INTERVAL" in source, "Source should have progress log interval"


def test_step2_has_progress_logging():
    """Test that step 2 print statements include progress metrics."""
    from pathlib import Path

    source = Path("python/step2_oai_pmh_dumps.py").read_text()
    assert "records=" in source, "Source should contain record count logging"
    assert "rec/s" in source, "Source should contain rate logging"


def test_step3_has_progress_logging():
    """Test that step 3 print statements include progress metrics."""
    from pathlib import Path

    source = Path("python/step3_download_steps.py").read_text()
    assert "progress:" in source, "Source should contain progress logging"
    assert "rows/s" in source, "Source should contain rate logging"


def test_step3_retry_logic():
    """Test that step 3 has retry logic for 5xx errors."""
    from pathlib import Path

    source = Path("python/step3_download_steps.py").read_text()
    assert "502" in source or "503" in source or "504" in source, "Should handle server errors"
    assert "retries" in source.lower(), "Should have retry logic"


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])