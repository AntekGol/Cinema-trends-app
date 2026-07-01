"""
Base extractor abstract class for the CineTrends ETL pipeline.
Defines the contract that all extractors must implement.
"""
from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class BaseExtractor(ABC):
    """Abstract base class for data extractors."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def extract(self) -> list[dict[str, Any]]:
        """Run the full extraction workflow."""

    @abstractmethod
    def validate(self, data: list[dict[str, Any]]) -> bool:
        """Validate a batch of extracted records."""

    def save_raw(
        self,
        data: list[dict[str, Any]],
        filepath: str | Path,
    ) -> Path:
        """Persist raw extracted data as a JSON file."""
        output_path = Path(filepath).resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Saving {len(data)} raw records to {output_path}")

        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=False, default=str)

        self.logger.debug(
            f"Raw data written successfully ({output_path.stat().st_size:,} bytes)"
        )
        return output_path
