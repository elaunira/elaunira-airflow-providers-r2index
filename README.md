# Elaunira Airflow Provider for R2Index

Airflow provider package for R2Index, providing connection type, operators, and TaskFlow decorators.

## Installation

```bash
pip install elaunira-airflow-providers-r2index
```

## Features

- **R2Index** connection type in Airflow UI
- `R2IndexUploadOperator` and `R2IndexDownloadOperator` for file transfers
- `@task.r2index_upload` and `@task.r2index_download` TaskFlow decorators

## Connection Configuration

After installation, the **R2Index** connection type will be available in Airflow's connection UI.

### Vault/OpenBao Mode (Recommended)

Fetches credentials dynamically from HashiCorp Vault or OpenBao:

| Field | Description |
|-------|-------------|
| Vault Connection ID | Airflow Vault connection ID (e.g., `openbao-elaunira`) |
| Vault Namespace | OpenBao namespace (e.g., `elaunira/production`) |
| Vault Secrets Mapping | JSON mapping of config keys to secret paths |

Example Vault Secrets Mapping:
```json
{
    "r2index_api_url": "cloudflare/r2index#api-url",
    "r2index_api_token": "cloudflare/r2index#api-token",
    "r2_access_key_id": "cloudflare/r2/e2e-tests#access-key-id",
    "r2_secret_access_key": "cloudflare/r2/e2e-tests#secret-access-key",
    "r2_endpoint_url": "cloudflare/r2/e2e-tests#endpoint-url"
}
```

Secret path format: `path#key` (e.g., `cloudflare/r2index#api-url`)

### Direct Mode

Store credentials directly in the connection:

| Field | Description |
|-------|-------------|
| R2Index API URL | API endpoint URL |
| R2Index API Token | API authentication token |
| R2 Access Key ID | Cloudflare R2 access key |
| R2 Secret Access Key | Cloudflare R2 secret key |
| R2 Endpoint URL | Cloudflare R2 endpoint |

### Environment Variables Fallback

If no connection is configured, the hook falls back to environment variables:

- `R2INDEX_API_URL`
- `R2INDEX_API_TOKEN`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_ENDPOINT_URL`

## Operators

### R2IndexUploadOperator

Upload files to R2Index:

```python
from elaunira.airflow.providers.r2index.operators import R2IndexUploadOperator, UploadItem

upload = R2IndexUploadOperator(
    task_id="upload_file",
    bucket="my-bucket",
    r2index_conn_id="my_r2index_connection",
    items=UploadItem(
        source="/tmp/data.csv",
        category="example",
        entity="sample-data",
        extension="csv",
        media_type="text/csv",
        destination_path="example/data",
        destination_filename="data.csv",
        destination_version="{{ ds }}",
    ),
)
```

### R2IndexDownloadOperator

Download files from R2Index:

```python
from elaunira.airflow.providers.r2index.operators import R2IndexDownloadOperator, DownloadItem

download = R2IndexDownloadOperator(
    task_id="download_file",
    bucket="my-bucket",
    r2index_conn_id="my_r2index_connection",
    items=DownloadItem(
        source_path="example/data",
        source_filename="data.csv",
        source_version="{{ ds }}",
        destination="/tmp/downloaded.csv",
    ),
)
```

## TaskFlow Decorators

### @task.r2index_upload

```python
from airflow.sdk import dag, task
from elaunira.airflow.providers.r2index.operators import UploadItem

@dag(schedule=None)
def my_dag():
    @task.r2index_upload(bucket="my-bucket", r2index_conn_id="my_connection")
    def prepare_upload() -> UploadItem:
        return UploadItem(
            source="/tmp/data.csv",
            category="example",
            entity="sample-data",
            extension="csv",
            media_type="text/csv",
            destination_path="example/data",
            destination_filename="data.csv",
            destination_version="2024-01-01",
        )

    prepare_upload()
```

### @task.r2index_download

```python
from airflow.sdk import dag, task
from elaunira.airflow.providers.r2index.operators import DownloadItem

@dag(schedule=None)
def my_dag():
    @task.r2index_download(bucket="my-bucket", r2index_conn_id="my_connection")
    def prepare_download() -> DownloadItem:
        return DownloadItem(
            source_path="example/data",
            source_filename="data.csv",
            source_version="2024-01-01",
            destination="/tmp/downloaded.csv",
        )

    prepare_download()
```

## Hook Usage

```python
from elaunira.airflow.providers.r2index.hooks import R2IndexHook

hook = R2IndexHook(r2index_conn_id="my_r2index_connection")
client = hook.get_conn()
```

## License

MIT
