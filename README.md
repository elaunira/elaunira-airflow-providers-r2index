# Elaunira Airflow Provider for R2Index

Airflow provider package that adds the **Elaunira R2Index** connection type for managing R2Index credentials.

## Installation

```bash
pip install elaunira-airflow-provider-r2index
```

## Usage

After installation, a new connection type **Elaunira R2Index** will be available in Airflow's connection UI.

### Connection Configuration

The connection supports two modes:

#### 1. Vault/OpenBao Mode (Recommended)

Fetches credentials dynamically from HashiCorp Vault or OpenBao:

| Field | Description |
|-------|-------------|
| Vault Connection ID | Airflow Vault connection ID (e.g., `openbao-ipregistry`) |
| Vault Namespace | OpenBao namespace (e.g., `ipregistry/production`) |
| Vault Secrets (JSON) | JSON mapping of config keys to secret paths |

Example Vault Secrets JSON:
```json
{
    "r2index_api_url": "cloudflare/r2index#api-url",
    "r2index_api_token": "cloudflare/r2index#api-token",
    "r2_access_key_id": "cloudflare/r2/airflow#access-key-id",
    "r2_secret_access_key": "cloudflare/r2/airflow#secret-access-key",
    "r2_endpoint_url": "cloudflare/r2/airflow#endpoint-url"
}
```

Secret path format: `path#key` (e.g., `cloudflare/r2index#api-url`)

#### 2. Direct Mode

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

## Hook Usage

```python
from elaunira.airflow.provider.r2index.hooks import R2IndexHook

hook = R2IndexHook(r2index_conn_id="my_r2index_connection")
client = hook.get_conn()
```

## License

MIT
