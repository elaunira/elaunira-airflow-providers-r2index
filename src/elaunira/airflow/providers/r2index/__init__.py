"""Elaunira R2Index Airflow provider."""

from importlib.metadata import version

__version__ = version("elaunira-airflow-providers-r2index")


def get_provider_info():
    """Return provider metadata for Airflow."""
    return {
        "package-name": "elaunira-airflow-providers-r2index",
        "name": "R2Index",
        "description": "Airflow provider for R2Index connections",
        "connection-types": [
            {
                "connection-type": "r2index",
                "hook-class-name": "elaunira.airflow.providers.r2index.hooks.r2index.R2IndexHook",
            }
        ],
        "task-decorators": [
            {
                "name": "r2index_upload",
                "class-name": "elaunira.airflow.providers.r2index.decorators.r2index.r2index_upload",
            },
            {
                "name": "r2index_download",
                "class-name": "elaunira.airflow.providers.r2index.decorators.r2index.r2index_download",
            },
        ],
        "versions": [__version__],
    }
