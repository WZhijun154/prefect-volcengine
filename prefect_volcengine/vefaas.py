"""VeFaaS (Volcengine Function as a Service) infrastructure blocks."""

import asyncio
import base64
import json
import time
import zipfile
from io import BytesIO
from typing import Any, Dict, List, Optional, Union

import volcenginesdkcore
import volcenginesdkfaas
from prefect.blocks.core import Block
from prefect.infrastructure.base import Infrastructure, InfrastructureResult
from prefect.utilities.asyncutils import run_sync_in_worker_thread
from pydantic import Field, validator

from prefect_volcengine.credentials import VolcengineCredentials


class VeFaaSJob(Infrastructure):
    """Infrastructure block for running flows on Volcengine VeFaaS.

    This block allows you to run Prefect flows as serverless functions
    on Volcengine's VeFaaS platform.
    """

    _block_type_name = "VeFaaS Job"
    _logo_url = "https://www.volcengine.com/favicon.ico"
    _documentation_url = "https://github.com/volcengine/volcengine-python-sdk"

    type: str = Field(
        default="vefaas-job",
        description="The type of infrastructure."
    )

    credentials: VolcengineCredentials = Field(
        description="Volcengine credentials for authentication"
    )

    function_name: str = Field(
        description="Name of the VeFaaS function"
    )

    runtime: str = Field(
        default="python3.9",
        description="Runtime environment for the function",
    )

    memory_spec: int = Field(
        default=128,
        description="Memory specification in MB"
    )

    timeout: int = Field(
        default=60,
        description="Function timeout in seconds"
    )

    environment_variables: Optional[Dict[str, str]] = Field(
        default_factory=dict,
        description="Environment variables to set in the function"
    )

    vpc_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="VPC configuration for the function"
    )

    code_zip_file: Optional[str] = Field(
        default=None,
        description="Base64 encoded zip file containing function code"
    )

    handler: str = Field(
        default="index.handler",
        description="Function entry point"
    )

    description: Optional[str] = Field(
        default="Prefect flow execution function",
        description="Function description"
    )

    @validator("memory_spec")
    def validate_memory_spec(cls, v):
        if not 64 <= v <= 3008:
            raise ValueError("Memory spec must be between 64 and 3008 MB")
        return v

    @validator("timeout")
    def validate_timeout(cls, v):
        if not 1 <= v <= 900:
            raise ValueError("Timeout must be between 1 and 900 seconds")
        return v

    def _get_faas_client(self):
        """Get VeFaaS API client."""
        configuration = self.credentials.get_volcengine_configuration()
        volcenginesdkcore.Configuration.set_default(configuration)
        return volcenginesdkfaas.FaasApi()

    def _create_default_code_package(self) -> str:
        """Create a default code package with Prefect flow runner."""
        code_content = '''
import json
import os
from prefect import flow, get_run_logger

def handler(event, context):
    """Default VeFaaS handler for Prefect flows."""
    logger = get_run_logger()

    # Extract flow information from event
    flow_run_id = event.get('flow_run_id')
    flow_name = event.get('flow_name', 'unnamed_flow')

    logger.info(f"Executing flow: {flow_name}, run_id: {flow_run_id}")

    # Here you would implement the actual flow execution logic
    # This is a placeholder implementation

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Flow {flow_name} executed successfully',
            'flow_run_id': flow_run_id
        })
    }
'''

        # Create zip file in memory
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr('index.py', code_content.strip())

        # Return base64 encoded zip
        zip_buffer.seek(0)
        return base64.b64encode(zip_buffer.getvalue()).decode('utf-8')

    async def run(
        self,
        task_status: Optional[Any] = None,
    ) -> InfrastructureResult:
        """Run a flow on VeFaaS."""

        def _run_sync():
            try:
                # Initialize VeFaaS client
                faas_client = self._get_faas_client()

                # Prepare function configuration
                if not self.code_zip_file:
                    code_zip = self._create_default_code_package()
                else:
                    code_zip = self.code_zip_file

                # Check if function exists, create if not
                try:
                    # Try to get existing function
                    get_request = volcenginesdkfaas.GetFunctionRequest(
                        function_name=self.function_name
                    )
                    response = faas_client.get_function(get_request)
                    function_exists = True
                except Exception:
                    function_exists = False

                if not function_exists:
                    # Create new function
                    create_request = volcenginesdkfaas.CreateFunctionRequest(
                        function_name=self.function_name,
                        runtime=self.runtime,
                        handler=self.handler,
                        code_zip_file=code_zip,
                        memory_spec=self.memory_spec,
                        timeout=self.timeout,
                        description=self.description,
                        envs=self.environment_variables or {}
                    )

                    if self.vpc_config:
                        create_request.vpc_config = self.vpc_config

                    create_response = faas_client.create_function(create_request)
                    self._logger.info(f"Created VeFaaS function: {self.function_name}")
                else:
                    # Update existing function if needed
                    update_request = volcenginesdkfaas.UpdateFunctionConfigurationRequest(
                        function_name=self.function_name,
                        runtime=self.runtime,
                        handler=self.handler,
                        memory_spec=self.memory_spec,
                        timeout=self.timeout,
                        description=self.description,
                        envs=self.environment_variables or {}
                    )

                    if self.vpc_config:
                        update_request.vpc_config = self.vpc_config

                    faas_client.update_function_configuration(update_request)
                    self._logger.info(f"Updated VeFaaS function: {self.function_name}")

                    # Update function code
                    update_code_request = volcenginesdkfaas.UpdateFunctionCodeRequest(
                        function_name=self.function_name,
                        code_zip_file=code_zip
                    )
                    faas_client.update_function_code(update_code_request)

                # Invoke the function
                invoke_payload = {
                    "flow_run_id": f"prefect-{int(time.time())}",
                    "flow_name": "prefect-flow",
                    "environment_variables": self.environment_variables or {}
                }

                invoke_request = volcenginesdkfaas.InvokeFunctionRequest(
                    function_name=self.function_name,
                    payload=json.dumps(invoke_payload).encode('utf-8'),
                    invocation_type="RequestResponse"  # Synchronous
                )

                invoke_response = faas_client.invoke_function(invoke_request)

                execution_id = f"vefaas-{self.function_name}-{int(time.time())}"

                # Check response status
                if hasattr(invoke_response, 'status_code') and invoke_response.status_code == 200:
                    return InfrastructureResult(
                        status_code=0,
                        identifier=execution_id
                    )
                else:
                    return InfrastructureResult(
                        status_code=1,
                        identifier=execution_id
                    )

            except Exception as e:
                self._logger.error(f"VeFaaS execution failed: {e}")
                return InfrastructureResult(
                    status_code=1,
                    identifier=f"vefaas-{self.function_name}-failed-{int(time.time())}"
                )

        if task_status:
            task_status.started("VeFaaS function starting...")

        # Run the synchronous VeFaaS operations in a thread
        result = await run_sync_in_worker_thread(_run_sync)

        return result

    def preview(self) -> str:
        """Preview the VeFaaS job configuration."""
        return f"""
VeFaaS Job Configuration:
- Function Name: {self.function_name}
- Runtime: {self.runtime}
- Memory: {self.memory_spec}MB
- Timeout: {self.timeout}s
- Handler: {self.handler}
- Region: {self.credentials.region}
        """.strip()


class VeFaaSCredentials(Block):
    """Deprecated: Use VolcengineCredentials instead."""

    _block_type_name = "VeFaaS Credentials"

    def __init__(self, **data):
        import warnings
        warnings.warn(
            "VeFaaSCredentials is deprecated. Use VolcengineCredentials instead.",
            DeprecationWarning,
            stacklevel=2
        )
        super().__init__(**data)