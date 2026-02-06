"""R2Index TaskFlow decorators."""

from elaunira.airflow.providers.r2index.decorators.r2index import (
    r2index_download,
    r2index_upload,
)

__all__ = ["r2index_download", "r2index_upload"]
