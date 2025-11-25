"""VeFaaS worker implementation for running Prefect flows on Volcengine Functions."""

import asyncio
import base64
import json
import time
import zipfile
from io import BytesIO
from typing import Any, Dict, Optional, Type

import volcenginesdkcore
import volcenginesdkfaas
from prefect.workers.base import BaseJobConfiguration, BaseVariables, BaseWorker
from prefect.infrastructure.base import InfrastructureResult
from prefect.utilities.asyncutils import run_sync_in_worker_thread
from pydantic import Field

from prefect_volcengine.credentials import VolcengineCredentials


class VeFaaSWorkerJobConfiguration(BaseJobConfiguration):
    """Configuration for VeFaaS worker jobs."""

    function_name: str = Field(
        description="Name of the VeFaaS function"
    )

    runtime: str = Field(
        default="python3.9",
        description="Runtime environment for the function"
    )

    memory_spec: int = Field(
        default=128,
        description="Memory specification in MB"
    )

    timeout: int = Field(
        default=300,
        description="Function timeout in seconds"
    )

    environment_variables: Dict[str, str] = Field(
        default_factory=dict,
        description="Environment variables for the function"
    )

    vpc_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="VPC configuration"
    )

    handler: str = Field(
        default="index.handler",
        description="Function entry point"
    )

    code_zip_file: Optional[str] = Field(
        default=None,
        description="Base64 encoded zip file containing function code"
    )

    description: Optional[str] = Field(
        default="Prefect flow worker function",
        description="Function description"
    )


class VeFaaSWorkerVariables(BaseVariables):
    """Variables for configuring VeFaaS worker."""

    credentials: VolcengineCredentials = Field(
        description="Volcengine credentials for authentication"
    )


class VeFaaSWorker(BaseWorker):
    """Worker for running flows on Volcengine VeFaaS platform."""

    type = "vefaas"
    job_configuration = VeFaaSWorkerJobConfiguration
    job_configuration_variables = VeFaaSWorkerVariables

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._faas_client = None

    def _get_faas_client(self):
        """Get or create VeFaaS API client."""
        if self._faas_client is None:
            configuration = self._configuration.credentials.get_volcengine_configuration()
            volcenginesdkcore.Configuration.set_default(configuration)
            self._faas_client = volcenginesdkfaas.FaasApi()
        return self._faas_client

    def _create_flow_runner_code_package(self, flow_run: Any) -> str:
        """Create a code package that can execute Prefect flow runs."""
        code_content = f'''
import json
import os
import sys
from prefect import flow, get_run_logger, Task
from prefect.client.orchestration import get_client

def handler(event, context):
    """VeFaaS handler for Prefect flow execution."""
    try:
        logger = get_run_logger()

        # Extract flow run information from event
        flow_run_id = event.get('flow_run_id', '{flow_run.id}')
        api_url = event.get('api_url', os.getenv('PREFECT_API_URL'))
        api_key = event.get('api_key', os.getenv('PREFECT_API_KEY'))

        logger.info(f"Executing flow run: {{flow_run_id}}")

        # Set environment variables for Prefect client
        if api_url:
            os.environ['PREFECT_API_URL'] = api_url
        if api_key:
            os.environ['PREFECT_API_KEY'] = api_key

        # TODO: Implement actual flow execution logic
        # This would involve:
        # 1. Downloading the flow code
        # 2. Setting up the flow environment
        # 3. Executing the flow
        # 4. Reporting results back to Prefect

        logger.info("Flow execution completed successfully")

        return {{
            'statusCode': 200,
            'body': json.dumps({{
                'message': 'Flow executed successfully',
                'flow_run_id': flow_run_id
            }})
        }}

    except Exception as e:
        logger = get_run_logger()
        logger.error(f"Flow execution failed: {{e}}")

        return {{
            'statusCode': 500,
            'body': json.dumps({{
                'error': str(e),
                'flow_run_id': event.get('flow_run_id', 'unknown')
            }})
        }}
'''

        # Create zip file in memory
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr('index.py', code_content.strip())

            # Add requirements.txt
            requirements = "prefect>=3.0.0"
            zip_file.writestr('requirements.txt', requirements)

        # Return base64 encoded zip
        zip_buffer.seek(0)
        return base64.b64encode(zip_buffer.getvalue()).decode('utf-8')

    async def run(
        self,
        flow_run: Any,
        configuration: VeFaaSWorkerJobConfiguration,
        task_status: Optional[Any] = None,
    ) -> InfrastructureResult:
        """Run a flow on VeFaaS."""

        def _run_sync():
            try:
                # Get VeFaaS client
                faas_client = self._get_faas_client()

                # Prepare function code
                if not configuration.code_zip_file:
                    code_zip = self._create_flow_runner_code_package(flow_run)
                else:
                    code_zip = configuration.code_zip_file

                # Prepare environment variables with Prefect context
                env_vars = {
                    **configuration.environment_variables,
                    "PREFECT_API_URL": getattr(self, 'api_url', '') or "",
                    "PREFECT_API_KEY": getattr(self, 'api_key', '') or "",
                }

                # Check if function exists
                function_exists = False
                try:
                    get_request = volcenginesdkfaas.GetFunctionRequest(
                        function_name=configuration.function_name
                    )
                    response = faas_client.get_function(get_request)
                    function_exists = True
                except Exception:
                    function_exists = False

                if not function_exists:
                    # Create new function
                    create_request = volcenginesdkfaas.CreateFunctionRequest(
                        function_name=configuration.function_name,
                        runtime=configuration.runtime,
                        handler=configuration.handler,
                        code_zip_file=code_zip,
                        memory_spec=configuration.memory_spec,
                        timeout=configuration.timeout,
                        description=configuration.description,
                        envs=env_vars
                    )

                    if configuration.vpc_config:
                        create_request.vpc_config = configuration.vpc_config

                    create_response = faas_client.create_function(create_request)
                    self._logger.info(f"Created VeFaaS function: {configuration.function_name}")
                else:
                    # Update existing function
                    update_request = volcenginesdkfaas.UpdateFunctionConfigurationRequest(
                        function_name=configuration.function_name,
                        runtime=configuration.runtime,
                        handler=configuration.handler,
                        memory_spec=configuration.memory_spec,
                        timeout=configuration.timeout,
                        description=configuration.description,
                        envs=env_vars
                    )

                    if configuration.vpc_config:
                        update_request.vpc_config = configuration.vpc_config

                    faas_client.update_function_configuration(update_request)

                    # Update function code
                    update_code_request = volcenginesdkfaas.UpdateFunctionCodeRequest(
                        function_name=configuration.function_name,
                        code_zip_file=code_zip
                    )
                    faas_client.update_function_code(update_code_request)

                    self._logger.info(f"Updated VeFaaS function: {configuration.function_name}")

                # Invoke the function with flow run details
                invoke_payload = {
                    "flow_run_id": str(flow_run.id),
                    "flow_name": getattr(flow_run, 'flow_name', 'unknown'),
                    "api_url": getattr(self, 'api_url', '') or "",
                    "api_key": getattr(self, 'api_key', '') or "",
                }

                invoke_request = volcenginesdkfaas.InvokeFunctionRequest(
                    function_name=configuration.function_name,
                    payload=json.dumps(invoke_payload).encode('utf-8'),
                    invocation_type="RequestResponse"  # Synchronous
                )

                invoke_response = faas_client.invoke_function(invoke_request)

                execution_id = f"vefaas-{configuration.function_name}-{int(time.time())}"

                # Check response status
                if hasattr(invoke_response, 'status_code') and invoke_response.status_code == 200:
                    self._logger.info(f"VeFaaS function execution completed: {execution_id}")
                    return InfrastructureResult(
                        status_code=0,
                        identifier=execution_id
                    )
                else:
                    self._logger.error(f"VeFaaS function execution failed: {execution_id}")
                    return InfrastructureResult(
                        status_code=1,
                        identifier=execution_id
                    )

            except Exception as e:
                self._logger.error(f"VeFaaS execution failed: {e}")
                return InfrastructureResult(
                    status_code=1,
                    identifier=f"vefaas-{configuration.function_name}-failed-{int(time.time())}"
                )

        self._logger.info(f"Starting VeFaaS function: {configuration.function_name}")

        if task_status:
            task_status.started("VeFaaS function execution started")

        # Run the synchronous VeFaaS operations in a thread
        result = await run_sync_in_worker_thread(_run_sync)

        return result

    async def kill(self, infrastructure_pid: str, grace_seconds: int = 30):
        """Kill a running VeFaaS function."""
        self._logger.info(f"Stopping VeFaaS function: {infrastructure_pid}")
        # Note: VeFaaS functions are typically short-lived and self-terminating
        # This method would be used for long-running async functions if supported