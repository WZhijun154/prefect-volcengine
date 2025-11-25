"""Tests for Volcengine credentials."""

import pytest
from prefect_volcengine.credentials import VolcengineCredentials


def test_volcengine_credentials_creation():
    """Test creating Volcengine credentials."""
    credentials = VolcengineCredentials(
        access_key_id="test_key",
        secret_access_key="test_secret",
        region="us-east-1"
    )

    assert credentials.access_key_id == "test_key"
    assert credentials.secret_access_key.get_secret_value() == "test_secret"
    assert credentials.region == "us-east-1"


def test_volcengine_credentials_with_endpoint():
    """Test creating credentials with custom endpoint."""
    credentials = VolcengineCredentials(
        access_key_id="test_key",
        secret_access_key="test_secret",
        region="us-east-1",
        endpoint_url="https://custom.endpoint.com"
    )

    assert credentials.endpoint_url == "https://custom.endpoint.com"


def test_get_volcengine_client_kwargs():
    """Test getting client kwargs from credentials."""
    credentials = VolcengineCredentials(
        access_key_id="test_key",
        secret_access_key="test_secret",
        region="us-east-1",
        endpoint_url="https://custom.endpoint.com"
    )

    kwargs = credentials.get_volcengine_client_kwargs()

    assert kwargs["access_key_id"] == "test_key"
    assert kwargs["secret_access_key"] == "test_secret"
    assert kwargs["region"] == "us-east-1"
    assert kwargs["endpoint_url"] == "https://custom.endpoint.com"


def test_get_volcengine_client_kwargs_no_endpoint():
    """Test getting client kwargs without custom endpoint."""
    credentials = VolcengineCredentials(
        access_key_id="test_key",
        secret_access_key="test_secret",
        region="us-east-1"
    )

    kwargs = credentials.get_volcengine_client_kwargs()

    assert kwargs["access_key_id"] == "test_key"
    assert kwargs["secret_access_key"] == "test_secret"
    assert kwargs["region"] == "us-east-1"
    assert "endpoint_url" not in kwargs