"""Elaunira R2Index Airflow provider."""

from importlib.metadata import version

__version__ = version("elaunira-airflow-provider-r2index")


def get_provider_info():
    """Return provider metadata for Airflow."""
    return {
        "package-name": "elaunira-airflow-provider-r2index",
        "name": "Elaunira R2Index",
        "description": "Airflow provider for Elaunira R2Index connections",
        "connection-types": [
            {
                "connection-type": "elaunira_r2index",
                "hook-class-name": "elaunira.airflow.provider.r2index.hooks.r2index.R2IndexHook",
            }
        ],
        "versions": [__version__],
    }
