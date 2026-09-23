import argparse
import json
import os
import subprocess
import sys

CONNECTION_SIGNATURES = (
    "RemoteDisconnected",
    "Connection aborted",
    "SSLEOFError",
    "Connection reset",
    "EOF occurred",
)


def classify(run_results: dict) -> str:
    """Pure function to decide what to do about a finished dbt invocation."""
    results = run_results.get("results", [])

    # Rule 1: Any node has status == "fail" -> "fail"
    if any(node.get("status") == "fail" for node in results):
        return "fail"

    # Rule 2: No node has status in ("fail", "error") -> "ok"
    if not any(node.get("status") in ("fail", "error") for node in results):
        return "ok"

    # Rule 3: Every error node's message contains a CONNECTION_SIGNATURES entry -> "retry"
    error_nodes = [node for node in results if node.get("status") == "error"]
    if error_nodes and all(
        any(sig in node.get("message", "") for sig in CONNECTION_SIGNATURES)
        for node in error_nodes
    ):
        return "retry"

    # Rule 4: Anything else -> "fail"
    return "fail"


def parse_target_from_argv(argv: list[str]) -> str | None:
    """Extract --target / -t value from argv without failing on unknown dbt flags."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--target", "-t", type=str, default=None)
    args, _ = parser.parse_known_args(argv)
    return args.target


def validate_target(argv: list[str], run_results: dict) -> bool:
    """Ensure target in argv matches target recorded in run_results.json."""
    argv_target = parse_target_from_argv(argv)
    if argv_target is None:
        return True  # No explicit target passed in argv, target check passes

    results_target = run_results.get("args", {}).get("target")
    return argv_target == results_target


def main(argv: list[str]) -> int:
    # Fail loudly if DBT_PROJECT_DIR is missing
    dbt_project_dir = os.getenv("DBT_PROJECT_DIR")
    if not dbt_project_dir:
        sys.stderr.write("Error: DBT_PROJECT_DIR environment variable is not set.\n")
        return 1

    run_results_path = os.path.join(dbt_project_dir, "target", "run_results.json")
    max_retries = int(os.getenv("MAX_RETRIES", "2"))

    if os.path.exists(run_results_path):
     try:
        os.remove(run_results_path)
     except OSError:
        pass

    # Initial dbt execution
    cmd = ["dbt"] + argv
    proc = subprocess.run(cmd)

    if proc.returncode == 0:
        return 0

    # Non-zero exit code: read results and re-classify
    attempts = 0
    while proc.returncode != 0 and attempts < max_retries:
        if not os.path.exists(run_results_path):
            sys.stderr.write(f"Error: {run_results_path} missing or unreadable.\n")
            return 1

        try:
            with open(run_results_path, "r") as f:
                run_results = json.load(f)
        except (json.JSONDecodeError, OSError):
            sys.stderr.write(f"Error: Could not parse {run_results_path}.\n")
            return 1

        # Target mismatch guardrail check
        if not validate_target(argv, run_results):
            sys.stderr.write(
                "Error: Target in argv does not match target in run_results.json.\n"
            )
            return 1

        decision = classify(run_results)

        # EDIT 2: If nodes passed/warned ("ok") but dbt exited non-zero, fail immediately without retry
        if decision in ("fail", "ok"):
            return 1

        if decision == "retry":
            attempts += 1

            proc = subprocess.run(["dbt", "retry"])
            if proc.returncode == 0:
                return 0

    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))