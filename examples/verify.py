#!/usr/bin/env python
"""
Unified verification script for pretty-cfn examples.
Replaces verify_examples.sh, validate_template.py, and validate_sam.py.
"""

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Optional

# Colors
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RESET = "\033[0m"
BOLD = "\033[1m"

ROOT_DIR = Path(__file__).parent.parent


def log(label: str, status: str, message: str, color: str = RESET):
    print(f"{color}{status}{RESET} {label:<25} {message}")


def run_command(cmd: List[str], cwd: Optional[Path] = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)


def validate_cfn_lint(path: Path) -> bool:
    """Run cfn-lint on a template."""
    cmd = ["cfn-lint", "--format", "json", str(path)]
    proc = run_command(cmd)

    if proc.returncode == 0 and not proc.stdout.strip():
        return True

    try:
        issues = json.loads(proc.stdout)
    except json.JSONDecodeError:
        # Fallback if not JSON
        if proc.returncode != 0:
            print(f"  cfn-lint failed (raw output): {proc.stdout.strip()}")
            return False
        return True

    has_error = False
    for issue in issues:
        level = issue.get("Level", "Unknown")
        message = issue.get("Message", "")
        rule = issue.get("Rule", {}).get("Id", "?")
        loc = issue.get("Location", {}).get("Start", {})
        line = loc.get("LineNumber", "?")

        if level.lower() == "error":
            has_error = True
            print(f"  [{rule}] {level}: {message} (line {line})")
        elif level.lower() == "warning":
            # Just print warnings, don't fail
            # print(f"  [{rule}] {level}: {message} (line {line})")
            pass

    return not has_error


def validate_sam_transform(path: Path) -> str:
    """
    Validate SAM template using cfn-lint.
    Returns status: PASS, FAIL
    """
    if validate_cfn_lint(path):
        return "PASS"
    return "FAIL"


def check_idempotency(input_path: Path, label: str) -> bool:
    """
    Check if formatting is idempotent:
    input -> out1
    out1 -> out2
    assert out1 == out2
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        out1 = tmp / "out1.yaml"
        out2 = tmp / "out2.yaml"

        # Format 1
        cmd1 = ["uv", "run", "pretty-cfn", "format", "--input", str(input_path), "-o", str(out1)]
        proc1 = run_command(cmd1, cwd=ROOT_DIR)
        if proc1.returncode != 0:
            log(label, "FAIL", "Format failed")
            print(proc1.stderr)
            return False

        # Format 2
        cmd2 = ["uv", "run", "pretty-cfn", "format", "--input", str(out1), "-o", str(out2)]
        proc2 = run_command(cmd2, cwd=ROOT_DIR)
        if proc2.returncode != 0:
            log(label, "FAIL", "Second pass format failed")
            return False

        # Compare
        if out1.read_text() != out2.read_text():
            log(label, "FAIL", "Not idempotent")
            return False

        # Lint check on the formatted output
        if not validate_cfn_lint(out1):
            log(label, "FAIL", "cfn-lint errors")
            return False

        log(label, "PASS", "Valid & Idempotent", color=GREEN)
        return True


def verify_cfn_examples():
    print(f"\n{BOLD}> Validating CloudFormation Examples{RESET}")
    failed = 0
    examples_dir = ROOT_DIR / "examples" / "cfn"

    # Handle both json and yaml subdirectories
    subdirs = ["json", "yaml"]

    for subdir_name in subdirs:
        subdir = examples_dir / subdir_name
        if not subdir.exists():
            continue

        for d in subdir.iterdir():
            if not d.is_dir():
                continue

            # Check for input.yaml or input.json
            input_file = d / "input.yaml"
            if not input_file.exists():
                input_file = d / "input.json"

            if not input_file.exists():
                continue

            if not check_idempotency(input_file, f"{subdir_name}/{d.name}"):
                failed += 1

            # Check generated SAM if exists
            sam_template = d / "sam" / "template.yaml"
            if sam_template.exists():
                status = validate_sam_transform(sam_template)
                label = f"{subdir_name}/{d.name}/sam"
                if status == "FAIL":
                    log(label, "FAIL", "SAM Transform failed", color=RED)
                    failed += 1
                elif status == "SKIP":
                    log(label, "SKIP", "Skipped SAM validation", color=CYAN)
                else:
                    log(label, "PASS", "SAM Valid", color=GREEN)

    return failed


def verify_cdk_examples():
    print(f"\n{BOLD}> Validating CDK Examples{RESET}")
    failed = 0
    examples_dir = ROOT_DIR / "examples" / "cdk"
    for d in examples_dir.iterdir():
        if not d.is_dir():
            continue

        # Find template.json
        cdk_out = d / "cdk.out"
        if not cdk_out.exists():
            continue

        templates = list(cdk_out.glob("*.template.json"))
        if not templates:
            continue

        template = templates[0]
        label = d.name

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            clean_out = tmp / "clean.yaml"

            # Refactor clean-cfn
            cmd = [
                "uv",
                "run",
                "pretty-cfn",
                "refactor",
                "--input",
                str(template),
                "--target",
                "clean-cfn",
                "-o",
                str(clean_out),
            ]
            proc = run_command(cmd, cwd=ROOT_DIR)
            if proc.returncode != 0:
                log(label, "FAIL", "Refactor failed", color=RED)
                print(proc.stderr)
                failed += 1
                continue

            # Lint
            if not validate_cfn_lint(clean_out):
                log(label, "FAIL", "cfn-lint errors", color=RED)
                failed += 1
                continue

            log(label, "PASS", "Clean & Valid", color=GREEN)

            # Check SAM output
            sam_template = d / "sam" / "template.yaml"
            if sam_template.exists():
                status = validate_sam_transform(sam_template)
                sam_label = f"{label}/sam"
                if status == "FAIL":
                    log(sam_label, "FAIL", "SAM Transform failed", color=RED)
                    failed += 1
                elif status == "SKIP":
                    log(sam_label, "SKIP", "Skipped SAM validation", color=CYAN)
                else:
                    log(sam_label, "PASS", "SAM Valid", color=GREEN)

    return failed


def main():
    failed_count = 0
    failed_count += verify_cfn_examples()
    failed_count += verify_cdk_examples()

    if failed_count > 0:
        print(f"\n{RED}[FAIL] {failed_count} example(s) failed validation{RESET}")
        sys.exit(1)
    else:
        print(f"\n{GREEN}[PASS] All examples are valid{RESET}")
        sys.exit(0)


if __name__ == "__main__":
    main()
