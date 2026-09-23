import json
from pathlib import Path
import pytest
from dbt_retry.run_with_retry import classify

FIXTURES_DIR = Path(__file__).parent / "fixtures"

def load_fixture(name: str) -> dict:
    with open(FIXTURES_DIR / name) as f:
        return json.load(f)

def test_classify_connection_error():
    data = load_fixture("connection_error.json")
    assert classify(data) == "retry"

def test_classify_data_failure():
    data = load_fixture("data_failure.json")
    assert classify(data) == "fail"

def test_classify_compilation_error():
    data = load_fixture("compilation_error.json")
    assert classify(data) == "fail"

def test_classify_clean_run():
    data = {
        "results": [
            {"status": "pass", "message": "OK"},
            {"status": "warn", "message": "6 unmapped campaigns"},
        ]
    }
    assert classify(data) == "ok"

def test_classify_mixed_fail_and_connection_error():
    """Criterion 5: One data fail + one connection error MUST yield 'fail'."""
    data = {
        "results": [
            {"status": "fail", "message": "Got 1 result, expected 0"},
            {"status": "error", "message": "RemoteDisconnected: Remote end closed connection"},
        ]
    }
    assert classify(data) == "fail"