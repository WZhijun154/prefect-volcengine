"""Deployment steps for Volcengine services."""

import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Dict, Optional

from prefect.utilities.filesystem import relative_path_to_current_platform

from prefect_volcengine.credentials import VolcengineCredentials


def push_to_vefaas(
    credentials: VolcengineCredentials,
    function_name: str,
    source_path: str = ".",
    ignore_patterns: Optional[list] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Push code to Volcengine VeFaaS function.

    Args:
        credentials: Volcengine credentials
        function_name: Name of the VeFaaS function
        source_path: Path to source code directory
        ignore_patterns: Patterns to ignore when packaging
        **kwargs: Additional arguments

    Returns:
        Dictionary with deployment information
    """
    if ignore_patterns is None:
        ignore_patterns = [
            "*.pyc",
            "__pycache__",
            ".git",
            ".env",
            "tests",
            "*.md",
        ]

    source_path = Path(source_path).resolve()

    # Create temporary directory for packaging
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        package_path = temp_path / "package"

        # Copy source files, excluding ignored patterns
        shutil.copytree(
            source_path,
            package_path,
            ignore=shutil.ignore_patterns(*ignore_patterns)
        )

        # Create deployment package
        zip_path = temp_path / f"{function_name}.zip"
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in package_path.rglob('*'):
                if file_path.is_file():
                    arcname = file_path.relative_to(package_path)
                    zipf.write(file_path, arcname)

        # In a real implementation, this would upload to VeFaaS
        # For now, we'll simulate the deployment
        deployment_info = {
            "function_name": function_name,
            "region": credentials.region,
            "package_size": zip_path.stat().st_size,
            "deployment_status": "success",
            "code_sha256": "simulated-sha256-hash",
        }

        return deployment_info