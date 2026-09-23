import json
from unittest.mock import MagicMock, patch
import pytest
from dbt_retry.run_with_retry import main

def test_main_missing_env_var_fails(monkeypatch):
    monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)
    assert main(["test"]) == 1

def test_main_target_mismatch_aborts_without_retry(tmp_path, monkeypatch):
    project_dir = tmp_path / "dbt_project"
    project_dir.mkdir()
    monkeypatch.setenv("DBT_PROJECT_DIR", str(project_dir))

    results_file = project_dir / "target" / "run_results.json"
    results_file.parent.mkdir(parents=True)
    results_file.write_text(json.dumps({
        "args": {"target": "prod"},
        "results": [{"status": "error", "message": "RemoteDisconnected"}]
    }))

    mock_run = MagicMock(return_value=MagicMock(returncode=1))
    with patch("subprocess.run", mock_run):
        # Passing --target dev when run_results has target prod
        exit_code = main(["test", "--target", "dev"])

    assert exit_code == 1
    assert mock_run.call_count == 1  # Executed initial command, but aborted before dbt retry