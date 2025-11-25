"""Tests for VeFaaS worker."""

import pytest
from prefect_volcengine.workers.vefaas import (
    VeFaaSWorker,
    VeFaaSWorkerJobConfiguration,
    VeFaaSWorkerVariables
)
from prefect_volcengine.credentials import VolcengineCredentials


def test_vefaas_worker_configuration():
    """Test VeFaaS worker job configuration."""
    config = VeFaaSWorkerJobConfiguration(
        function_name="test-function",
        runtime="python3.9",
        memory_size=256,
        timeout=300,
        environment_variables={"TEST": "value"}
    )

    assert config.function_name == "test-function"
    assert config.runtime == "python3.9"
    assert config.memory_size == 256
    assert config.timeout == 300
    assert config.environment_variables == {"TEST": "value"}


def test_vefaas_worker_variables(volcengine_credentials):
    """Test VeFaaS worker variables."""
    variables = VeFaaSWorkerVariables(
        credentials=volcengine_credentials
    )

    assert variables.credentials == volcengine_credentials


def test_vefaas_worker_creation():
    """Test creating VeFaaS worker."""
    worker = VeFaaSWorker()

    assert worker.type == "vefaas"
    assert worker.job_configuration == VeFaaSWorkerJobConfiguration
    assert worker.job_configuration_variables == VeFaaSWorkerVariables


@pytest.mark.asyncio
async def test_vefaas_worker_run(volcengine_credentials):
    """Test VeFaaS worker run method."""
    # Create worker with mock configuration
    worker = VeFaaSWorker()
    worker._configuration = VeFaaSWorkerVariables(
        credentials=volcengine_credentials
    )

    # Create job configuration
    config = VeFaaSWorkerJobConfiguration(
        function_name="test-function"
    )

    # Mock flow run
    class MockFlowRun:
        id = "test-flow-run"

    flow_run = MockFlowRun()

    # Test run
    result = await worker.run(flow_run, config)

    assert result.status_code == 0
    assert "vefaas-test-function" in result.identifier