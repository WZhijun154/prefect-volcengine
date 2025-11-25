# prefect-volcengine

<p align="center">
    <img src="https://www.volcengine.com/favicon.ico" alt="Volcengine logo" width="50" height="50">
    <br>
    <a href="https://pypi.python.org/pypi/prefect-volcengine/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-volcengine?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/PrefectHQ/prefect-volcengine/" alt="Stars">
        <img src="https://img.shields.io/github/stars/PrefectHQ/prefect-volcengine?color=0052FF&labelColor=090422" /></a>
    <a href="https://pepy.tech/badge/prefect-volcengine/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-volcengine?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/PrefectHQ/prefect-volcengine/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/PrefectHQ/prefect-volcengine?color=0052FF&labelColor=090422" /></a>
    <br>
    <a href="https://prefect-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect.io/" alt="Discourse">
        <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?color=0052FF&labelColor=090422&logo=discourse" /></a>
</p>

## Welcome!

Prefect integrations for working with Volcengine services.

## Getting Started

### Python setup

Requires an installation of Python 3.8+.

We recommend using a Python virtual environment manager such as pipenv, conda, or virtualenv.

### Installation

Install `prefect-volcengine` with `uv`:

```bash
uv add prefect-volcengine
```

Requires an installation of Python 3.8+.

### Quick Start

#### Setting up credentials

To use `prefect-volcengine`, you'll need:

1. A Volcengine account
2. Access Key ID and Secret Access Key
3. Appropriate permissions for VeFaaS

Create a credentials block:

```python
from prefect import flow
from prefect_volcengine import VolcengineCredentials

@flow
def setup_credentials():
    credentials = VolcengineCredentials(
        access_key_id="your-access-key-id",
        secret_access_key="your-secret-access-key",
        region="us-east-1"
    )
    credentials.save("volcengine-credentials")

if __name__ == "__main__":
    setup_credentials()
```

#### Running flows on VeFaaS

Use the VeFaaSJob infrastructure block to run flows on Volcengine's serverless platform:

```python
from prefect import flow
from prefect_volcengine import VolcengineCredentials, VeFaaSJob

# Define your flow
@flow(log_prints=True)
def my_flow(name: str = "world"):
    print(f"Hello {name} from VeFaaS!")

# Configure VeFaaS deployment
if __name__ == "__main__":
    credentials = VolcengineCredentials.load("volcengine-credentials")

    vefaas_job = VeFaaSJob(
        credentials=credentials,
        function_name="my-prefect-flow",
        runtime="python3.9",
        memory_size=256,
        timeout=300,
    )

    my_flow.with_options(infrastructure=vefaas_job).deploy(
        name="my-vefaas-deployment"
    )
```

#### Using the VeFaaS Worker

For a more scalable approach, use the VeFaaS worker:

```python
from prefect_volcengine import VeFaaSWorker
from prefect.workers import serve

# Start the worker
if __name__ == "__main__":
    serve(
        VeFaaSWorker.from_type(),
        limit=10,
        with_healthcheck=True,
    )
```

## Resources

For more tips on how to use tasks and flows in a Collection, check out [Using Collections](https://docs.prefect.io/latest/collections/usage/)!

### Installation

- [PyPI](https://pypi.org/project/prefect-volcengine/): `uv add prefect-volcengine`

### Feedback

If you encounter any bugs while using `prefect-volcengine`, feel free to open an issue in the [prefect-volcengine](https://github.com/PrefectHQ/prefect-volcengine) repository.

If you have any questions or issues while using `prefect-volcengine`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

Feel free to star or watch [`prefect-volcengine`](https://github.com/PrefectHQ/prefect-volcengine) for updates too!

### Development

If you'd like to install a version of `prefect-volcengine` for development, clone the repository and perform an editable install with `uv`:

```bash
git clone https://github.com/PrefectHQ/prefect-volcengine.git

cd prefect-volcengine/

uv sync --dev
```

## Supported Volcengine Services

This collection currently supports:

- **VeFaaS (Volcengine Function as a Service)**: Run Prefect flows as serverless functions
- **Credentials Management**: Secure credential storage and management

### VeFaaS Features

- **Infrastructure Block**: `VeFaaSJob` for individual flow deployments
- **Worker**: `VeFaaSWorker` for scalable work pool execution
- **Deployment Steps**: `push_to_vefaas` for automated code deployment
- **Configurable Resources**: Memory, timeout, runtime environment settings
- **VPC Support**: Deploy functions within VPC networks
- **Environment Variables**: Secure environment variable injection

## Contributing

Thanks for thinking about contributing to this collection!

We'd love to have you contribute! Please make sure to install `prefect-volcengine` locally using the development installation guide above.