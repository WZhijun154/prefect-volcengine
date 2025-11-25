"""Pytest configuration for prefect-volcengine tests."""

import pytest
from prefect_volcengine.credentials import VolcengineCredentials


@pytest.fixture
def volcengine_credentials():
    """Mock Volcengine credentials for testing."""
    return VolcengineCredentials(
        access_key_id="test_access_key",
        secret_access_key="test_secret_key",
        region="cn-beijing"
    )