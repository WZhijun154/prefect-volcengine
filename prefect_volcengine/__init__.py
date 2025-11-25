"""Prefect integrations for Volcengine services."""

from prefect_volcengine._version import __version__
from prefect_volcengine.credentials import VolcengineCredentials
from prefect_volcengine.vefaas import VeFaaSJob
from prefect_volcengine.workers import VeFaaSWorker

__all__ = [
    "__version__",
    "VolcengineCredentials",
    "VeFaaSJob",
    "VeFaaSWorker",
]