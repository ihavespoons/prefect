import asyncio
import subprocess
import sys

import pytest
from rich.console import Console

from prefect.infrastructure.provisioners.container_instance import AzureCLI


def test_run_command_propagates_nonzero_exit(tmp_path):
    """
    AzureCLI.run_command must propagate subprocess.CalledProcessError when the
    underlying CLI fails, per its documented contract, rather than silently
    returning None.
    """
    # Use a deterministic interpreter failure via a real subprocess that exits
    # non-zero. We invoke the interpreter through the CLI's own command
    # execution path, so no scratch script is needed.
    failing = subprocess.list2cmdline(
        [sys.executable, "-c", "import sys; sys.exit(23)"]
    )

    cli = AzureCLI(Console(record=True))

    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        asyncio.run(cli.run_command(failing, return_json=True))

    assert exc_info.value.returncode == 23
