"""VeFaaS worker implementation for running Prefect flows on Volcengine Functions."""

import asyncio
import json
import time
from typing import Any, Dict, Optional, Type

from prefect.workers.base import BaseJobConfiguration, BaseVariables, BaseWorker
from prefect.infrastructure.base import InfrastructureResult
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

    memory_size: int = Field(
        default=128,
        description="Memory size in MB (128-3008)"
    )

    timeout: int = Field(
        default=300,
        description="Function timeout in seconds (1-10800)"
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
        default="main.handler",
        description="Function entry point"
    )

    code_location: Optional[str] = Field(
        default=None,
        description="Code location (S3 bucket or archive)"
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
        self._client = None

    async def run(
        self,
        flow_run: Any,
        configuration: VeFaaSWorkerJobConfiguration,
        task_status: Optional[Any] = None,
    ) -> InfrastructureResult:
        """Run a flow on VeFaaS."""

        # Get credentials from variables
        credentials = self._configuration.credentials

        # Prepare function configuration
        function_config = {
            "FunctionName": configuration.function_name,
            "Runtime": configuration.runtime,
            "MemorySize": configuration.memory_size,
            "Timeout": configuration.timeout,
            "Handler": configuration.handler,
            "Environment": {
                "Variables": {
                    **configuration.environment_variables,
                    "PREFECT_API_URL": self._configuration.api_url or "",
                    "PREFECT_API_KEY": self._configuration.api_key or "",
                }
            }
        }

        if configuration.vpc_config:
            function_config["VpcConfig"] = configuration.vpc_config

        self._logger.info(f"Starting VeFaaS function: {configuration.function_name}")

        if task_status:
            task_status.started("VeFaaS function execution started")

        # In a real implementation, this would:
        # 1. Deploy/update the function code to VeFaaS
        # 2. Invoke the function with flow run parameters
        # 3. Monitor the execution
        # 4. Return results

        # For now, simulate execution
        await asyncio.sleep(2)

        execution_id = f"vefaas-{configuration.function_name}-{int(time.time())}"

        self._logger.info(f"VeFaaS function completed: {execution_id}")

        return InfrastructureResult(
            status_code=0,
            identifier=execution_id
        )

    async def kill(self, infrastructure_pid: str, grace_seconds: int = 30):
        """Kill a running VeFaaS function."""
        self._logger.info(f"Stopping VeFaaS function: {infrastructure_pid}")
        # In real implementation, this would cancel/stop the function execution