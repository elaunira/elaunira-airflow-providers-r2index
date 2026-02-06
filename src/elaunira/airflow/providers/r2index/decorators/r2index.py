"""R2Index TaskFlow decorators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, TypeVar

from airflow.sdk.bases.decorator import task_decorator_factory

from elaunira.airflow.providers.r2index.operators.r2index import (
    DownloadItem,
    R2IndexDownloadOperator,
    R2IndexUploadOperator,
    UploadItem,
)

if TYPE_CHECKING:
    from airflow.sdk.bases.decorator import TaskDecorator

F = TypeVar("F", bound=Callable[..., Any])


def r2index_upload(
    bucket: str,
    r2index_conn_id: str = "r2index_default",
    **kwargs: Any,
) -> TaskDecorator:
    """
    Decorator to upload files to R2Index.

    The decorated function should return an UploadItem or list of UploadItems.

    Example:
        @task.r2index_upload(bucket="my-bucket")
        def prepare_upload() -> UploadItem:
            return UploadItem(
                source="/tmp/data.csv",
                category="acme",
                entity="acme-data",
                extension="csv",
                media_type="text/csv",
                destination_path="acme/data",
                destination_filename="data.csv",
                destination_version="{{ ds }}",
            )

    :param bucket: R2 bucket name.
    :param r2index_conn_id: Airflow connection ID for R2Index.
    """
    return task_decorator_factory(
        decorated_operator_class=_R2IndexUploadDecoratedOperator,
        bucket=bucket,
        r2index_conn_id=r2index_conn_id,
        **kwargs,
    )


def r2index_download(
    bucket: str,
    r2index_conn_id: str = "r2index_default",
    **kwargs: Any,
) -> TaskDecorator:
    """
    Decorator to download files from R2Index.

    The decorated function should return a DownloadItem or list of DownloadItems.

    Example:
        @task.r2index_download(bucket="my-bucket")
        def prepare_download() -> DownloadItem:
            return DownloadItem(
                source_path="acme/data",
                source_filename="data.csv",
                source_version="{{ ds }}",
                destination="/tmp/downloaded.csv",
            )

    :param bucket: R2 bucket name.
    :param r2index_conn_id: Airflow connection ID for R2Index.
    """
    return task_decorator_factory(
        decorated_operator_class=_R2IndexDownloadDecoratedOperator,
        bucket=bucket,
        r2index_conn_id=r2index_conn_id,
        **kwargs,
    )


class _R2IndexUploadDecoratedOperator(R2IndexUploadOperator):
    """Decorated operator for R2Index uploads."""

    custom_operator_name = "@task.r2index_upload"

    def __init__(
        self,
        *,
        python_callable: Callable[..., UploadItem | list[UploadItem]],
        op_args: list[Any] | None = None,
        op_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        # items will be set in execute()
        kwargs["items"] = []
        super().__init__(**kwargs)

    def execute(self, context: Any) -> list[dict[str, Any]]:
        """Execute the decorated function and upload the result."""
        items = self.python_callable(*self.op_args, **self.op_kwargs)
        self.items = [items] if isinstance(items, UploadItem) else items
        return super().execute(context)


class _R2IndexDownloadDecoratedOperator(R2IndexDownloadOperator):
    """Decorated operator for R2Index downloads."""

    custom_operator_name = "@task.r2index_download"

    def __init__(
        self,
        *,
        python_callable: Callable[..., DownloadItem | list[DownloadItem]],
        op_args: list[Any] | None = None,
        op_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        # items will be set in execute()
        kwargs["items"] = []
        super().__init__(**kwargs)

    def execute(self, context: Any) -> list[dict[str, Any]]:
        """Execute the decorated function and download the result."""
        items = self.python_callable(*self.op_args, **self.op_kwargs)
        self.items = [items] if isinstance(items, DownloadItem) else items
        return super().execute(context)
