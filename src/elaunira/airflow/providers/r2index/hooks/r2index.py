"""R2Index hook for Airflow."""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any

from airflow.sdk.bases.hook import BaseHook

if TYPE_CHECKING:
    from elaunira.r2index import R2IndexClient


class R2IndexHook(BaseHook):
    """
    Hook for interacting with R2Index API and R2 storage.

    Configuration priority:
        1. Airflow connection with Vault/OpenBao reference (fetches secrets dynamically)
        2. Airflow connection with direct credentials
        3. Environment variables (fallback)

    Airflow connection with Vault/OpenBao reference (extra JSON):
        {
            "vault_conn_id": "openbao_default",
            "vault_namespace": "elaunira/production",
            "vault_secrets_mapping": {
                "r2index_api_url": "cloudflare/r2index#api-url",
                "r2index_api_token": "cloudflare/r2index#api-token",
                "r2_access_key_id": "cloudflare/r2/airflow#access-key-id",
                "r2_secret_access_key": "cloudflare/r2/airflow#secret-access-key",
                "r2_endpoint_url": "cloudflare/r2/airflow#endpoint-url"
            }
        }

        The vault_conn_id references an Airflow HashiCorp Vault connection
        configured with AppRole or other auth method.

        vault_secrets_mapping format: "path#key" or "path" (uses config key as secret key)
        Required keys:
        - r2index_api_url
        - r2index_api_token
        - r2_access_key_id
        - r2_secret_access_key
        - r2_endpoint_url

    Airflow connection with direct credentials:
        - host: R2Index API URL
        - password: R2Index API token
        - extra.r2_access_key_id: R2 access key ID
        - extra.r2_secret_access_key: R2 secret access key
        - extra.r2_endpoint_url: R2 endpoint URL

    Environment variables (fallback):
        - R2INDEX_API_URL
        - R2INDEX_API_TOKEN
        - R2_ACCESS_KEY_ID
        - R2_SECRET_ACCESS_KEY
        - R2_ENDPOINT_URL
    """

    conn_name_attr = "r2index_conn_id"
    default_conn_name = "r2index_default"
    conn_type = "r2index"
    hook_name = "R2Index"

    CONFIG_KEYS = [
        "r2index_api_url",
        "r2index_api_token",
        "r2_access_key_id",
        "r2_secret_access_key",
        "r2_endpoint_url",
    ]

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Customize connection form UI."""
        return {
            "hidden_fields": ["port", "schema", "login", "extra"],
            "relabeling": {
                "host": "R2Index API URL (direct mode only)",
                "password": "R2Index API Token (direct mode only)",
            },
            "placeholders": {
                "host": "https://r2index.example.com",
                "password": "API token for direct connection",
                "vault_conn_id": "openbao-myservice",
                "vault_namespace": "myservice/production",
                "vault_secrets_mapping": '{"r2index_api_url": "cloudflare/r2index#api-url", ...}',
                "r2_access_key_id": "Direct mode: R2 access key ID",
                "r2_secret_access_key": "Direct mode: R2 secret access key",
                "r2_endpoint_url": "https://account.r2.cloudflarestorage.com",
            },
        }

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Define custom connection form widgets."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "vault_conn_id": StringField(
                lazy_gettext("Vault Connection ID"),
                widget=BS3TextFieldWidget(),
                description="Airflow Vault connection ID (e.g., openbao-elaunira)",
            ),
            "vault_namespace": StringField(
                lazy_gettext("Vault Namespace"),
                widget=BS3TextFieldWidget(),
                description="OpenBao namespace (e.g., elaunira/production)",
            ),
            "vault_secrets_mapping": StringField(
                lazy_gettext("Vault Secrets (JSON)"),
                widget=BS3TextFieldWidget(),
                description="JSON mapping of config keys to secret paths",
            ),
            "r2_access_key_id": StringField(
                lazy_gettext("R2 Access Key ID"),
                widget=BS3TextFieldWidget(),
                description="Direct mode: Cloudflare R2 access key ID",
            ),
            "r2_secret_access_key": PasswordField(
                lazy_gettext("R2 Secret Access Key"),
                widget=BS3PasswordFieldWidget(),
                description="Direct mode: Cloudflare R2 secret access key",
            ),
            "r2_endpoint_url": StringField(
                lazy_gettext("R2 Endpoint URL"),
                widget=BS3TextFieldWidget(),
                description="Direct mode: Cloudflare R2 endpoint URL",
            ),
        }

    def __init__(self, r2index_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.r2index_conn_id = r2index_conn_id
        self._client: R2IndexClient | None = None

    def _parse_secret_ref(self, secret_ref: str, default_key: str) -> tuple[str, str]:
        """Parse a secret reference into (path, key).

        Format: "path#key" or just "path" (uses default_key).
        """
        if "#" in secret_ref:
            path, key = secret_ref.rsplit("#", 1)
            return path, key
        return secret_ref, default_key

    def _get_config_from_env(self) -> dict[str, str | None]:
        """Get configuration from environment variables."""
        return {
            "index_api_url": os.environ.get("R2INDEX_API_URL"),
            "index_api_token": os.environ.get("R2INDEX_API_TOKEN"),
            "r2_access_key_id": os.environ.get("R2_ACCESS_KEY_ID"),
            "r2_secret_access_key": os.environ.get("R2_SECRET_ACCESS_KEY"),
            "r2_endpoint_url": os.environ.get("R2_ENDPOINT_URL"),
        }

    def _get_config_from_vault(
        self,
        vault_conn_id: str,
        secrets: dict[str, str],
        namespace: str | None = None,
    ) -> dict[str, str | None] | None:
        """Get configuration from Vault/OpenBao using Airflow's VaultHook.

        Args:
            vault_conn_id: Airflow connection ID for Vault/OpenBao
            secrets: Mapping of config key to secret reference (path#key format)
            namespace: OpenBao namespace to use
        """
        from airflow.providers.hashicorp.hooks.vault import VaultHook

        try:
            # Note: namespace should be configured in the Vault connection's extra field
            vault_hook = VaultHook(vault_conn_id=vault_conn_id)
            secret_cache: dict[str, dict[str, Any]] = {}

            def get_secret_value(config_key: str) -> str | None:
                secret_ref = secrets.get(config_key)
                if not secret_ref:
                    return None

                path, key = self._parse_secret_ref(secret_ref, config_key)

                if path not in secret_cache:
                    data = vault_hook.get_secret(secret_path=path, secret_version=None)
                    secret_cache[path] = data or {}

                return secret_cache[path].get(key)

            config = {
                "index_api_url": get_secret_value("r2index_api_url"),
                "index_api_token": get_secret_value("r2index_api_token"),
                "r2_access_key_id": get_secret_value("r2_access_key_id"),
                "r2_secret_access_key": get_secret_value("r2_secret_access_key"),
                "r2_endpoint_url": get_secret_value("r2_endpoint_url"),
            }
            # Log which values are missing
            missing = [k for k, v in config.items() if v is None]
            if missing:
                self.log.warning("Missing Vault secrets: %s", missing)
            return config
        except Exception as e:
            self.log.error("Failed to get config from Vault: %s", e)
            return None

    def _get_config_from_connection(self) -> dict[str, str | None] | None:
        """Get configuration from Airflow connection.

        If connection has vault_conn_id, fetches from Vault/OpenBao.
        Otherwise uses direct credentials from connection fields.
        """
        try:
            conn = self.get_connection(self.r2index_conn_id)
            extra = conn.extra_dejson

            vault_conn_id = extra.get("vault_conn_id")
            if vault_conn_id:
                secrets_raw = extra.get("vault_secrets_mapping")
                if not secrets_raw:
                    return None
                if isinstance(secrets_raw, str):
                    secrets = json.loads(secrets_raw)
                else:
                    secrets = secrets_raw
                return self._get_config_from_vault(
                    vault_conn_id=vault_conn_id,
                    secrets=secrets,
                    namespace=extra.get("vault_namespace"),
                )

            return {
                "index_api_url": conn.host,
                "index_api_token": conn.password,
                "r2_access_key_id": extra.get("r2_access_key_id"),
                "r2_secret_access_key": extra.get("r2_secret_access_key"),
                "r2_endpoint_url": extra.get("r2_endpoint_url"),
            }
        except Exception as e:
            self.log.error("Failed to get config from connection: %s", e)
            return None

    def get_conn(self) -> R2IndexClient:
        """Get the R2IndexClient."""
        if self._client is not None:
            return self._client

        from elaunira.r2index import R2IndexClient

        config = self._get_config_from_connection()
        if config is None or not config.get("index_api_url"):
            config = self._get_config_from_env()

        self._client = R2IndexClient(
            index_api_url=config["index_api_url"],
            index_api_token=config["index_api_token"],
            r2_access_key_id=config["r2_access_key_id"],
            r2_secret_access_key=config["r2_secret_access_key"],
            r2_endpoint_url=config["r2_endpoint_url"],
        )
        return self._client

    def upload(
        self,
        bucket: str,
        source: str,
        category: str,
        entity: str,
        extension: str,
        media_type: str,
        destination_path: str,
        destination_filename: str,
        destination_version: str,
        name: str | None = None,
        tags: list[str] | None = None,
        extra: dict[str, Any] | None = None,
        create_checksum_files: bool = False,
    ) -> dict[str, Any]:
        """Upload a file to R2 and register it with R2Index."""
        client = self.get_conn()
        file_record = client.upload(
            bucket=bucket,
            source=source,
            category=category,
            entity=entity,
            extension=extension,
            media_type=media_type,
            destination_path=destination_path,
            destination_filename=destination_filename,
            destination_version=destination_version,
            name=name,
            tags=tags,
            extra=extra,
            create_checksum_files=create_checksum_files,
        )
        return file_record.model_dump()

    def download(
        self,
        bucket: str,
        source_path: str,
        source_filename: str,
        source_version: str,
        destination: str,
        verify_checksum: bool = True,
    ) -> dict[str, Any]:
        """Download a file from R2."""
        client = self.get_conn()
        downloaded_path, file_record = client.download(
            bucket=bucket,
            source_path=source_path,
            source_filename=source_filename,
            source_version=source_version,
            destination=destination,
            verify_checksum=verify_checksum,
        )
        return {
            "path": str(downloaded_path),
            "file_record": file_record.model_dump(),
        }

    def get_file(self, file_id: str) -> dict[str, Any]:
        """Get a file record by ID."""
        client = self.get_conn()
        return client.get(file_id).model_dump()

    def list_files(
        self,
        bucket: str | None = None,
        category: str | None = None,
        entity: str | None = None,
        extension: str | None = None,
        tags: list[str] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """List files with optional filters."""
        client = self.get_conn()
        response = client.list_files(
            bucket=bucket,
            category=category,
            entity=entity,
            extension=extension,
            tags=tags,
            limit=limit,
        )
        return response.model_dump()
