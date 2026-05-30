# subtitle: app/Repositories/StorageClient.py
"""
HTTP client for the storage service.
Calls /files/register. Raises on failure so the Kafka message redelivers.
"""
import httpx

from app.Config.Config import config


class StorageRegisterFailed(Exception):
    pass


class StorageClient:
    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    async def register_file(
        self,
        *,
        job_id: str,
        user_id: int,
        category: str,
        file_type: str,
        path: str,
        mime_type: str,
        size_bytes: int = 0,
    ) -> None:
        """
        Register a file with storage. Raises on non-2xx so the worker
        does NOT commit the Kafka offset and the message redelivers.
        """
        try:
            resp = await self.client.post(
                f"{config.STORAGE_URL}/files/register",
                json={
                    "job_id": job_id,
                    "user_id": user_id,
                    "category": category,
                    "type": file_type,
                    "path": path,
                    "mime_type": mime_type,
                    "size_bytes": size_bytes,
                },
                timeout=10.0,
            )
        except httpx.HTTPError as e:
            raise StorageRegisterFailed(f"HTTP error: {e}") from e

        if resp.status_code >= 300:
            raise StorageRegisterFailed(
                f"Storage returned {resp.status_code}: {resp.text}"
            )