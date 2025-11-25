"""Tests for VeFaaS infrastructure and blocks."""

import pytest
from prefect_volcengine.vefaas import VeFaaSJob
from prefect_volcengine.credentials import VolcengineCredentials


def test_vefaas_job_creation(volcengine_credentials):
    """Test creating a VeFaaS job."""
    job = VeFaaSJob(
        credentials=volcengine_credentials,
        function_name="test-function",
        runtime="python3.9",
        memory_size=256,
        timeout=300
    )

    assert job.function_name == "test-function"
    assert job.runtime == "python3.9"
    assert job.memory_size == 256
    assert job.timeout == 300


def test_vefaas_job_memory_validation():
    """Test VeFaaS job memory size validation."""
    credentials = VolcengineCredentials(
        access_key_id="test",
        secret_access_key="test",
        region="us-east-1"
    )

    # Test invalid memory size
    with pytest.raises(ValueError, match="Memory size must be between 128 and 3008 MB"):
        VeFaaSJob(
            credentials=credentials,
            function_name="test",
            memory_size=100  # Too low
        )

    with pytest.raises(ValueError, match="Memory size must be between 128 and 3008 MB"):
        VeFaaSJob(
            credentials=credentials,
            function_name="test",
            memory_size=4000  # Too high
        )


def test_vefaas_job_timeout_validation():
    """Test VeFaaS job timeout validation."""
    credentials = VolcengineCredentials(
        access_key_id="test",
        secret_access_key="test",
        region="us-east-1"
    )

    # Test invalid timeout
    with pytest.raises(ValueError, match="Timeout must be between 1 and 900 seconds"):
        VeFaaSJob(
            credentials=credentials,
            function_name="test",
            timeout=0  # Too low
        )

    with pytest.raises(ValueError, match="Timeout must be between 1 and 900 seconds"):
        VeFaaSJob(
            credentials=credentials,
            function_name="test",
            timeout=1000  # Too high
        )


def test_vefaas_job_preview(volcengine_credentials):
    """Test VeFaaS job preview functionality."""
    job = VeFaaSJob(
        credentials=volcengine_credentials,
        function_name="test-function",
        runtime="python3.9",
        memory_size=256,
        timeout=300
    )

    preview = job.preview()
    assert "test-function" in preview
    assert "python3.9" in preview
    assert "256MB" in preview
    assert "300s" in preview
    assert volcengine_credentials.region in preview


@pytest.mark.asyncio
async def test_vefaas_job_run(volcengine_credentials):
    """Test VeFaaS job run method."""
    job = VeFaaSJob(
        credentials=volcengine_credentials,
        function_name="test-function",
        runtime="python3.9",
        memory_size=256,
        timeout=300
    )

    result = await job.run()

    assert result.status_code == 0
    assert "vefaas-test-function" in result.identifier