"""Deployment steps for Volcengine services."""

import base64
import hashlib
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Dict, Optional

import volcenginesdkcore
import volcenginesdkfaas
from prefect.utilities.filesystem import relative_path_to_current_platform

from prefect_volcengine.credentials import VolcengineCredentials


def push_to_vefaas(
    credentials: VolcengineCredentials,
    function_name: str,
    source_path: str = ".",
    ignore_patterns: Optional[list] = None,
    runtime: str = "python3.9",
    handler: str = "index.handler",
    memory_spec: int = 128,
    timeout: int = 60,
    description: Optional[str] = None,
    environment_variables: Optional[Dict[str, str]] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Push code to Volcengine VeFaaS function.

    Args:
        credentials: Volcengine credentials
        function_name: Name of the VeFaaS function
        source_path: Path to source code directory
        ignore_patterns: Patterns to ignore when packaging
        runtime: Function runtime environment
        handler: Function entry point
        memory_spec: Memory specification in MB
        timeout: Function timeout in seconds
        description: Function description
        environment_variables: Environment variables for the function
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
            ".pytest_cache",
            "*.log",
        ]

    if environment_variables is None:
        environment_variables = {}

    source_path = Path(source_path).resolve()

    # Initialize VeFaaS client
    configuration = credentials.get_volcengine_configuration()
    volcenginesdkcore.Configuration.set_default(configuration)
    faas_client = volcenginesdkfaas.FaasApi()

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

        # Read and encode the zip file
        with open(zip_path, 'rb') as zip_file:
            zip_content = zip_file.read()
            zip_base64 = base64.b64encode(zip_content).decode('utf-8')
            zip_sha256 = hashlib.sha256(zip_content).hexdigest()

        # Check if function exists
        function_exists = False
        try:
            get_request = volcenginesdkfaas.GetFunctionRequest(
                function_name=function_name
            )
            response = faas_client.get_function(get_request)
            function_exists = True
        except Exception:
            function_exists = False

        try:
            if not function_exists:
                # Create new function
                create_request = volcenginesdkfaas.CreateFunctionRequest(
                    function_name=function_name,
                    runtime=runtime,
                    handler=handler,
                    code_zip_file=zip_base64,
                    memory_spec=memory_spec,
                    timeout=timeout,
                    description=description or f"Prefect flow function: {function_name}",
                    envs=environment_variables
                )

                response = faas_client.create_function(create_request)
                deployment_info = {
                    "function_name": function_name,
                    "region": credentials.region,
                    "package_size": len(zip_content),
                    "deployment_status": "created",
                    "code_sha256": zip_sha256,
                    "runtime": runtime,
                    "handler": handler,
                    "memory_spec": memory_spec,
                    "timeout": timeout,
                }
            else:
                # Update existing function configuration
                update_config_request = volcenginesdkfaas.UpdateFunctionConfigurationRequest(
                    function_name=function_name,
                    runtime=runtime,
                    handler=handler,
                    memory_spec=memory_spec,
                    timeout=timeout,
                    description=description or f"Prefect flow function: {function_name}",
                    envs=environment_variables
                )

                faas_client.update_function_configuration(update_config_request)

                # Update function code
                update_code_request = volcenginesdkfaas.UpdateFunctionCodeRequest(
                    function_name=function_name,
                    code_zip_file=zip_base64
                )

                faas_client.update_function_code(update_code_request)

                deployment_info = {
                    "function_name": function_name,
                    "region": credentials.region,
                    "package_size": len(zip_content),
                    "deployment_status": "updated",
                    "code_sha256": zip_sha256,
                    "runtime": runtime,
                    "handler": handler,
                    "memory_spec": memory_spec,
                    "timeout": timeout,
                }

            return deployment_info

        except Exception as e:
            return {
                "function_name": function_name,
                "region": credentials.region,
                "package_size": len(zip_content),
                "deployment_status": "failed",
                "error": str(e),
                "code_sha256": zip_sha256,
            }