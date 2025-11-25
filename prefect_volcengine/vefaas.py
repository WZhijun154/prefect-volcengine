"""VeFaaS (Volcengine Function as a Service) infrastructure blocks."""

import asyncio
import json
import time
from typing import Any, Dict, List, Optional, Union

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

    memory_size: int = Field(
        default=128,
        description="Memory size in MB (128-3008)"
    )

    timeout: int = Field(
        default=60,
        description="Function timeout in seconds (1-900)"
    )

    environment_variables: Optional[Dict[str, str]] = Field(
        default_factory=dict,
        description="Environment variables to set in the function"
    )

    vpc_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="VPC configuration for the function"
    )

    code_location: Optional[str] = Field(
        default=None,
        description="Location of the function code (S3 bucket or local path)"
    )

    handler: str = Field(
        default="main.handler",
        description="Function entry point"
    )

    @validator("memory_size")
    def validate_memory_size(cls, v):
        if not 128 <= v <= 3008:
            raise ValueError("Memory size must be between 128 and 3008 MB")
        return v

    @validator("timeout")
    def validate_timeout(cls, v):
        if not 1 <= v <= 900:
            raise ValueError("Timeout must be between 1 and 900 seconds")
        return v

    async def run(
        self,
        task_status: Optional[Any] = None,
    ) -> InfrastructureResult:
        """Run a flow on VeFaaS."""

        # Initialize VeFaaS client (placeholder for actual SDK)
        client_kwargs = self.credentials.get_volcengine_client_kwargs()

        # Simulate function deployment and execution
        function_config = {
            "FunctionName": self.function_name,
            "Runtime": self.runtime,
            "MemorySize": self.memory_size,
            "Timeout": self.timeout,
            "Handler": self.handler,
            "Environment": {
                "Variables": self.environment_variables or {}
            }
        }

        if self.vpc_config:
            function_config["VpcConfig"] = self.vpc_config

        # In a real implementation, this would use the Volcengine SDK
        # to create/update and invoke the function

        if task_status:
            task_status.started("VeFaaS function starting...")

        # Simulate function execution
        await asyncio.sleep(1)

        # Return success result
        return InfrastructureResult(
            status_code=0,
            identifier=f"vefaas-{self.function_name}-{int(time.time())}"
        )

    def preview(self) -> str:
        """Preview the VeFaaS job configuration."""
        return f"""
VeFaaS Job Configuration:
- Function Name: {self.function_name}
- Runtime: {self.runtime}
- Memory: {self.memory_size}MB
- Timeout: {self.timeout}s
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