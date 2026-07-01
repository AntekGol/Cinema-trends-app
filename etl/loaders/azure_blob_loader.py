"""
Azure Blob Storage loader for the data lake.
Handles upload/download with Hive-style partitioning (bronze/silver/gold layers).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

try:
    from azure.storage.blob import BlobServiceClient, ContainerClient
except ImportError:
    logger.warning("azure-storage-blob not installed, AzureBlobLoader won't work")
    BlobServiceClient = None
    ContainerClient = None


class AzureBlobLoader:
    """
    Upload/download data to Azure Blob Storage.
    Organizes files in Hive partitions like: bronze/trending_movies/date=2026-06-29/data.json
    """

    def __init__(self, connection_string: str | None = None, container_name: str | None = None) -> None:
        if BlobServiceClient is None:
            raise ImportError("Need azure-storage-blob: pip install azure-storage-blob")

        self.connection_string = connection_string or os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
        self.container_name = container_name or os.environ.get("AZURE_CONTAINER_NAME", "cinetrends-datalake")

        if not self.connection_string:
            raise ValueError("Azure connection string required. Set AZURE_STORAGE_CONNECTION_STRING.")

        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self._ensure_container_exists()
        logger.info(f"AzureBlobLoader ready (container={self.container_name})")

    def _ensure_container_exists(self) -> None:
        try:
            client = self.blob_service_client.get_container_client(self.container_name)
            client.get_container_properties()
        except Exception:
            logger.info(f"Creating container '{self.container_name}'")
            self.blob_service_client.create_container(self.container_name)

    def _get_container_client(self) -> ContainerClient:
        return self.blob_service_client.get_container_client(self.container_name)

    def upload_json(self, data: list[dict] | dict, blob_path: str, overwrite: bool = True) -> str:
        """Upload JSON data to a blob path."""
        container = self._get_container_client()
        content = json.dumps(data, ensure_ascii=False, indent=2, default=str)
        container.get_blob_client(blob_path).upload_blob(content, overwrite=overwrite, encoding="utf-8")
        logger.info(f"Uploaded {len(content)} bytes to {blob_path}")
        return blob_path

    def upload_daily_extract(self, data: list[dict], layer: str, source_name: str, date: datetime | None = None) -> str:
        """Upload with Hive partitioning: {layer}/{source}/date=YYYY-MM-DD/data.json"""
        if date is None:
            date = datetime.utcnow()
        blob_path = f"{layer}/{source_name}/date={date.strftime('%Y-%m-%d')}/data.json"
        return self.upload_json(data, blob_path)

    def upload_file(self, local_path: str, blob_path: str, overwrite: bool = True) -> str:
        """Upload a local file to blob storage."""
        container = self._get_container_client()
        with open(local_path, "rb") as f:
            container.get_blob_client(blob_path).upload_blob(f, overwrite=overwrite)
        logger.info(f"Uploaded file to {blob_path}")
        return blob_path

    def download_json(self, blob_path: str) -> list[dict] | dict:
        """Download and parse a JSON blob."""
        container = self._get_container_client()
        content = container.get_blob_client(blob_path).download_blob().readall().decode("utf-8")
        return json.loads(content)

    def download_file(self, blob_path: str, local_path: str) -> str:
        """Download a blob to local file."""
        container = self._get_container_client()
        with open(local_path, "wb") as f:
            f.write(container.get_blob_client(blob_path).download_blob().readall())
        return local_path

    def list_blobs(self, prefix: str = "") -> list[str]:
        """List blob names matching a prefix."""
        container = self._get_container_client()
        return [blob.name for blob in container.list_blobs(name_starts_with=prefix)]

    def blob_exists(self, blob_path: str) -> bool:
        container = self._get_container_client()
        try:
            container.get_blob_client(blob_path).get_blob_properties()
            return True
        except Exception:
            return False

    def delete_blob(self, blob_path: str) -> None:
        container = self._get_container_client()
        container.get_blob_client(blob_path).delete_blob()
        logger.info(f"Deleted {blob_path}")
