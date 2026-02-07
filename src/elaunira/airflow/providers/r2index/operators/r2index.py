"""R2Index operators for Airflow."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from elaunira.airflow.providers.r2index.hooks import R2IndexHook
from elaunira.airflow.providers.r2index.links.r2index import R2IndexFileLink
from elaunira.r2index import AsyncR2IndexClient
from elaunira.r2index.storage import R2TransferConfig

if TYPE_CHECKING:
    from airflow.utils.context import Context



@dataclass
class UploadItem:
    """Defines a single file to upload."""

    source: str
    category: str
    entity: str
    extension: str
    media_type: str
    destination_path: str
    destination_filename: str
    destination_version: str
    bucket: str | None = None
    name: str | None = None
    tags: list[str] | None = None
    extra: dict[str, Any] | None = None
    create_checksum_files: bool = False
    r2index_conn_id: str | None = None


@dataclass
class DownloadItem:
    """Defines a single file to download."""

    source_path: str
    source_filename: str
    source_version: str
    destination: str
    bucket: str | None = None
    verify_checksum: bool = True
    overwrite: bool = True
    r2index_conn_id: str | None = None


class R2IndexUploadOperator(BaseOperator):
    """
    Upload one or more files to R2 in parallel.

    When multiple items are provided, all uploads run concurrently using
    asyncio within the worker process.

    :param bucket: Default R2 bucket name (can be overridden per item).
    :param items: List of UploadItem or single UploadItem defining files to upload.
    :param r2index_conn_id: Default Airflow connection ID (can be overridden per item).
    """

    template_fields: Sequence[str] = ("items",)
    template_ext: Sequence[str] = ()
    ui_color = "#e4f0e8"
    operator_extra_links = (R2IndexFileLink(),)

    def __init__(
        self,
        *,
        bucket: str,
        items: list[UploadItem] | UploadItem,
        r2index_conn_id: str = "r2index_default",
        transfer_config: R2TransferConfig | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket = bucket
        self.items = [items] if isinstance(items, UploadItem) else items
        self.r2index_conn_id = r2index_conn_id
        self.transfer_config = transfer_config

    def _get_client_config(self, conn_id: str) -> dict[str, Any]:
        """Get client configuration using the hook's priority chain."""
        hook = R2IndexHook(r2index_conn_id=conn_id)
        config = hook._get_config_from_connection()
        if config is None or not config.get("index_api_url"):
            config = hook._get_config_from_env()
        return config or {}

    def execute(self, context: Context) -> list[dict[str, Any]]:
        """Execute the uploads in parallel."""
        # Group items by connection ID for efficient client reuse
        conn_configs: dict[str, dict[str, Any]] = {}

        async def get_or_create_config(conn_id: str) -> dict[str, Any]:
            if conn_id not in conn_configs:
                conn_configs[conn_id] = self._get_client_config(conn_id)
            return conn_configs[conn_id]

        async def upload_one(item: UploadItem) -> dict[str, Any]:
            conn_id = item.r2index_conn_id or self.r2index_conn_id
            bucket = item.bucket or self.bucket
            config = await get_or_create_config(conn_id)

            try:
                async with AsyncR2IndexClient(**config) as client:
                    file_record = await client.upload(
                        bucket=bucket,
                        source=item.source,
                        category=item.category,
                        entity=item.entity,
                        extension=item.extension,
                        media_type=item.media_type,
                        destination_path=item.destination_path,
                        destination_filename=item.destination_filename,
                        destination_version=item.destination_version,
                        name=item.name,
                        tags=item.tags,
                        extra=item.extra,
                        create_checksum_files=item.create_checksum_files,
                        transfer_config=self.transfer_config,
                    )
                    return {"status": "success", "file_record": file_record.model_dump()}
            except Exception as e:
                return {"status": "error", "message": str(e), "source": item.source}

        async def upload_all() -> list[dict[str, Any]]:
            tasks = [upload_one(item) for item in self.items]
            return await asyncio.gather(*tasks)

        self.log.info("::group::Upload progress")
        results = asyncio.run(upload_all())
        self.log.info("::endgroup::")

        # Check for errors
        errors = [r for r in results if r.get("status") == "error"]
        if errors:
            self.log.error("Failed uploads: %s", errors)
            raise AirflowException(f"{len(errors)}/{len(results)} uploads failed")

        self.log.info("Uploaded %d files in parallel", len(results))
        return [r["file_record"] for r in results]


class R2IndexDownloadOperator(BaseOperator):
    """
    Download one or more files from R2 in parallel.

    When multiple items are provided, all downloads run concurrently using
    asyncio within the worker process.

    :param bucket: Default R2 bucket name (can be overridden per item).
    :param items: List of DownloadItem or single DownloadItem defining files to download.
    :param r2index_conn_id: Default Airflow connection ID (can be overridden per item).
    """

    template_fields: Sequence[str] = ("items",)
    template_ext: Sequence[str] = ()
    ui_color = "#f0e4e8"
    operator_extra_links = (R2IndexFileLink(),)

    def __init__(
        self,
        *,
        bucket: str,
        items: list[DownloadItem] | DownloadItem,
        r2index_conn_id: str = "r2index_default",
        transfer_config: R2TransferConfig | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket = bucket
        self.items = [items] if isinstance(items, DownloadItem) else items
        self.r2index_conn_id = r2index_conn_id
        self.transfer_config = transfer_config

    def _get_client_config(self, conn_id: str) -> dict[str, Any]:
        """Get client configuration using the hook's priority chain."""
        hook = R2IndexHook(r2index_conn_id=conn_id)
        config = hook._get_config_from_connection()
        if config is None or not config.get("index_api_url"):
            config = hook._get_config_from_env()
        return config or {}

    def execute(self, context: Context) -> list[dict[str, Any]]:
        """Execute the downloads in parallel."""
        # Group items by connection ID for efficient client reuse
        conn_configs: dict[str, dict[str, Any]] = {}

        async def get_or_create_config(conn_id: str) -> dict[str, Any]:
            if conn_id not in conn_configs:
                conn_configs[conn_id] = self._get_client_config(conn_id)
            return conn_configs[conn_id]

        async def download_one(item: DownloadItem) -> dict[str, Any]:
            conn_id = item.r2index_conn_id or self.r2index_conn_id
            bucket = item.bucket or self.bucket
            config = await get_or_create_config(conn_id)

            try:
                async with AsyncR2IndexClient(**config) as client:
                    downloaded_path, file_record = await client.download(
                        bucket=bucket,
                        source_path=item.source_path,
                        source_filename=item.source_filename,
                        source_version=item.source_version,
                        destination=item.destination,
                        verify_checksum=item.verify_checksum,
                        overwrite=item.overwrite,
                        transfer_config=self.transfer_config,
                    )
                    return {
                        "status": "success",
                        "path": str(downloaded_path),
                        "file_record": file_record.model_dump() if file_record else None,
                    }
            except Exception as e:
                return {
                    "status": "error",
                    "message": str(e),
                    "source_path": item.source_path,
                }

        async def download_all() -> list[dict[str, Any]]:
            tasks = [download_one(item) for item in self.items]
            return await asyncio.gather(*tasks)

        self.log.info("::group::Download progress")
        results = asyncio.run(download_all())
        self.log.info("::endgroup::")

        # Check for errors
        errors = [r for r in results if r.get("status") == "error"]
        if errors:
            self.log.error("Failed downloads: %s", errors)
            raise AirflowException(f"{len(errors)}/{len(results)} downloads failed")

        self.log.info("Downloaded %d files in parallel", len(results))
        return [{"path": r["path"], "file_record": r["file_record"]} for r in results]
