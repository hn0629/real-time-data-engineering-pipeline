$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot

Push-Location $ProjectRoot

try {
    $TestFileNames = @(
        "test_prepare_clean_publish.py",
        "test_publish_clean_artifact.py",
        "test_register_clean_partition.py",
        "test_publish_and_register.py",
        "test_publisher_analytics_gate.py",
        "test_v2_orchestration_config.py",
        "test_publisher_preparation_inputs.py"
    )

    $TransferredFiles = @{}

    foreach ($TestFileName in $TestFileNames) {
        $TestFilePath = Join-Path ".\scripts\tests" $TestFileName

        if (-not (Test-Path -LiteralPath $TestFilePath -PathType Leaf)) {
            throw "Missing test file: $TestFilePath"
        }

        $ResolvedPath = (Resolve-Path -LiteralPath $TestFilePath).Path

        $TransferredFiles["scripts/tests/$TestFileName"] = (
            [Convert]::ToBase64String(
                [System.IO.File]::ReadAllBytes($ResolvedPath)
            )
        )
    }

    $WrapperPath = (
        Resolve-Path -LiteralPath ".\scripts\prepare_clean_publish.py"
    ).Path

    $TransferredFiles["scripts/prepare_clean_publish.py"] = (
        [Convert]::ToBase64String(
            [System.IO.File]::ReadAllBytes($WrapperPath)
        )
    )

    $PayloadJson = ConvertTo-Json `
        -InputObject $TransferredFiles `
        -Compress

    $PayloadBase64 = [Convert]::ToBase64String(
        [System.Text.Encoding]::UTF8.GetBytes($PayloadJson)
    )

    $PythonRunner = @"
import base64
import json
import socket
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

files = json.loads(
    base64.b64decode("__PAYLOAD_BASE64__").decode("utf-8")
)


def block_connection(*args, **kwargs):
    raise RuntimeError(
        "STOP: An outbound connection was attempted during unit tests."
    )


with tempfile.TemporaryDirectory(
    prefix="clean-publication-suite-"
) as directory:
    root = Path(directory)

    for relative_path, encoded_content in files.items():
        destination = root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(base64.b64decode(encoded_content))

    sys.path.insert(0, "/opt/airflow")
    sys.path.insert(0, str(root / "scripts"))

    with (
        patch.object(socket.socket, "connect", block_connection),
        patch.object(socket.socket, "connect_ex", block_connection),
        patch("socket.create_connection", block_connection),
    ):
        suite = unittest.defaultTestLoader.discover(
            start_dir=str(root / "scripts" / "tests"),
            pattern="test_*.py",
        )

        discovered = suite.countTestCases()
        print(f"Discovered test methods: {discovered}", flush=True)

        if discovered != 62:
            raise SystemExit(
                f"STOP: Expected 62 test methods, found {discovered}. "
                "Inspect discovery before proceeding."
            )

        from prepare_clean_publish import prepare as wrapper_prepare
        from src.clean_publish.prepare_clean_artifact import (
            prepare as shared_prepare,
        )

        if wrapper_prepare is not shared_prepare:
            raise SystemExit(
                "STOP: Preparation wrapper does not expose "
                "the shared implementation."
            )

        print(
            "Preparation wrapper delegates to shared implementation: True",
            flush=True,
        )

        result = unittest.TextTestRunner(verbosity=2).run(suite)

print(
    f"\nCombined suite summary: tests={result.testsRun}, "
    f"failures={len(result.failures)}, "
    f"errors={len(result.errors)}, "
    f"skipped={len(result.skipped)}",
    flush=True,
)

print(
    "No DAG tasks or dbt build were executed; "
    "outbound socket connections were blocked.",
    flush=True,
)

passed = (
    result.wasSuccessful()
    and result.testsRun == 62
    and not result.skipped
)

raise SystemExit(0 if passed else 1)
"@

    $PythonRunner = $PythonRunner.Replace(
        "__PAYLOAD_BASE64__",
        $PayloadBase64
    )

    $PythonRunner |
        docker compose exec -T airflow-scheduler python -

    $TestExitCode = $LASTEXITCODE

    Write-Host "Combined publication tests exit code: $TestExitCode"
}
finally {
    Pop-Location
}

exit $TestExitCode
