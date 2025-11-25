"""Credentials for authenticating with Volcengine."""

from typing import Optional

from prefect.blocks.core import Block
from pydantic import Field, SecretStr


class VolcengineCredentials(Block):
    """Block for storing Volcengine credentials.

    Attributes:
        access_key_id: Volcengine access key ID
        secret_access_key: Volcengine secret access key
        region: Volcengine region
        endpoint_url: Optional custom endpoint URL
    """

    _block_type_name = "Volcengine Credentials"
    _logo_url = "https://www.volcengine.com/favicon.ico"
    _documentation_url = "https://github.com/volcengine/volcengine-python-sdk"

    access_key_id: str = Field(
        description="Volcengine access key ID"
    )
    secret_access_key: SecretStr = Field(
        description="Volcengine secret access key"
    )
    region: str = Field(
        default="us-east-1",
        description="Volcengine region"
    )
    endpoint_url: Optional[str] = Field(
        default=None,
        description="Custom endpoint URL for Volcengine services"
    )

    def get_volcengine_client_kwargs(self) -> dict:
        """Get configuration for Volcengine client initialization."""
        client_kwargs = {
            "access_key_id": self.access_key_id,
            "secret_access_key": self.secret_access_key.get_secret_value(),
            "region": self.region,
        }

        if self.endpoint_url:
            client_kwargs["endpoint_url"] = self.endpoint_url

        return client_kwargs