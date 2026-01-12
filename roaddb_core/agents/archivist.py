#!/usr/bin/env python3
"""Codex-DB Archivist agent implementation.

The Archivist listens for the rhythm of data flow. It monitors storage patterns,
models data lifecycle, sketches archival strategies that balance cost with access,
and records the evolving "laws" that govern data retention in the BlackRoad swarm.

The implementation mirrors the charter encoded within ``codex_archivist.yaml`` by:

* grounding every decision on measured storage metrics (or clearly stating when
  data is absent),
* keeping beauty with efficiency by producing both structured artifacts and a
  lyrical archive journal,
* preserving data gently by preferring tiered storage over deletion, and
* translating complex metrics into policies that collaborating agents can execute.

Architecture:
    ┌─────────────────────────────────────────────────────────────────┐
    │                        Archivist Agent                         │
    ├─────────────────────────────────────────────────────────────────┤
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
    │  │   Storage   │  │  Lifecycle  │  │  Retention  │             │
    │  │   Monitor   │──│   Manager   │──│   Policy    │             │
    │  └─────────────┘  └─────────────┘  └─────────────┘             │
    │         │               │               │                       │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
    │  │    Tier     │  │ Compression │  │   Archive   │             │
    │  │   Manager   │──│   Engine    │──│   Writer    │             │
    │  └─────────────┘  └─────────────┘  └─────────────┘             │
    └─────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import math
import os
import shutil
import threading
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from pathlib import Path
from statistics import mean, median, pstdev
from typing import (
    Any, Callable, Dict, Iterable, Iterator, List, Mapping,
    MutableMapping, Optional, Sequence, Set, Tuple, Type, Union
)

import yaml

logger = logging.getLogger(__name__)

DEFAULT_STATE_ROOT = Path("/srv/roaddb/archivist")
METRICS_LOG_NAME = "storage_metrics.jsonl"
LIFECYCLE_LOG_NAME = "lifecycle_events.jsonl"
POLICY_FILE_NAME = "retention_policies.json"
ARCHIVE_MANIFEST_NAME = "archive_manifest.json"
JOURNAL_NAME = "archive_journal.md"
STATE_FILENAME = "state.json"


# ---------------------------------------------------------------------------
# Enums and Constants
# ---------------------------------------------------------------------------


class StorageTier(Enum):
    """Storage tier classifications."""
    HOT = "hot"        # Fast SSD, frequently accessed
    WARM = "warm"      # Standard SSD, occasional access
    COLD = "cold"      # HDD, rare access
    ARCHIVE = "archive"  # Object storage, very rare access
    GLACIER = "glacier"  # Deep archive, restore required


class LifecyclePhase(Enum):
    """Data lifecycle phases."""
    ACTIVE = auto()
    AGING = auto()
    ARCHIVING = auto()
    ARCHIVED = auto()
    EXPIRING = auto()
    DELETED = auto()


class CompressionAlgorithm(Enum):
    """Supported compression algorithms."""
    NONE = "none"
    GZIP = "gzip"
    LZ4 = "lz4"
    ZSTD = "zstd"
    BROTLI = "brotli"


class RetentionAction(Enum):
    """Actions for retention policy."""
    KEEP = auto()
    COMPRESS = auto()
    TIER_DOWN = auto()
    ARCHIVE = auto()
    DELETE = auto()


# ---------------------------------------------------------------------------
# Seed Loading Utilities
# ---------------------------------------------------------------------------


def _ensure_list(value: Any) -> List[str]:
    """Return value coerced into a list of clean strings."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    raise TypeError(f"Expected list-compatible value, received {type(value)!r}")


@dataclass
class ArchivistSeed:
    """Structured representation of the Archivist seed manifest."""
    identifier: str
    agent_name: str
    generation: str
    parent: Optional[str]
    siblings: List[str]
    domain: List[str]
    moral_constant: str
    core_principle: str
    purpose: str
    directives: List[str]
    jobs: List[str]
    personality: Mapping[str, Any]
    input_channels: List[str]
    output_channels: List[str]
    behavioural_loop: List[str]
    seed_language: str
    boot_command: str

    # Archivist-specific fields
    default_retention_days: int = 365
    compression_threshold_bytes: int = 1024 * 1024  # 1MB
    archive_after_days: int = 90
    cold_storage_after_days: int = 30
    hot_storage_max_gb: float = 100.0


def load_seed(path: Path) -> ArchivistSeed:
    """Load and validate the Archivist seed file."""
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    if not isinstance(data, MutableMapping):
        raise ValueError("Archivist seed must be a mapping at the top level")

    charter = data.get("system_charter")
    if not isinstance(charter, MutableMapping):
        raise ValueError("Archivist seed missing 'system_charter' mapping")

    required_charter = [
        "agent_name", "generation", "domain", "moral_constant",
        "core_principle", "purpose", "directives", "jobs"
    ]
    for key in required_charter:
        if key not in charter:
            raise ValueError(f"Archivist seed missing required field: {key}")

    return ArchivistSeed(
        identifier=data.get("identifier", "archivist-001"),
        agent_name=charter["agent_name"],
        generation=charter["generation"],
        parent=charter.get("parent"),
        siblings=_ensure_list(charter.get("siblings")),
        domain=_ensure_list(charter["domain"]),
        moral_constant=charter["moral_constant"],
        core_principle=charter["core_principle"],
        purpose=charter["purpose"],
        directives=_ensure_list(charter["directives"]),
        jobs=_ensure_list(charter["jobs"]),
        personality=charter.get("personality", {}),
        input_channels=_ensure_list(charter.get("input_channels")),
        output_channels=_ensure_list(charter.get("output_channels")),
        behavioural_loop=_ensure_list(charter.get("behavioural_loop")),
        seed_language=data.get("seed_language", "python"),
        boot_command=data.get("boot_command", "archivist run"),
        default_retention_days=data.get("default_retention_days", 365),
        compression_threshold_bytes=data.get("compression_threshold_bytes", 1024 * 1024),
        archive_after_days=data.get("archive_after_days", 90),
        cold_storage_after_days=data.get("cold_storage_after_days", 30),
        hot_storage_max_gb=data.get("hot_storage_max_gb", 100.0),
    )


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------


@dataclass
class StorageMetrics:
    """Storage metrics for a table or database."""
    table_name: str
    total_bytes: int
    row_count: int
    index_bytes: int
    avg_row_size: float
    compression_ratio: float
    last_accessed: datetime
    last_modified: datetime
    access_frequency: float  # accesses per hour
    growth_rate: float  # bytes per day
    tier: StorageTier
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def total_mb(self) -> float:
        return self.total_bytes / (1024 * 1024)

    @property
    def total_gb(self) -> float:
        return self.total_bytes / (1024 * 1024 * 1024)

    @property
    def age_days(self) -> float:
        delta = datetime.now(timezone.utc) - self.created_at
        return delta.total_seconds() / 86400

    @property
    def idle_days(self) -> float:
        delta = datetime.now(timezone.utc) - self.last_accessed
        return delta.total_seconds() / 86400

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "total_bytes": self.total_bytes,
            "total_mb": self.total_mb,
            "row_count": self.row_count,
            "index_bytes": self.index_bytes,
            "avg_row_size": self.avg_row_size,
            "compression_ratio": self.compression_ratio,
            "last_accessed": self.last_accessed.isoformat(),
            "last_modified": self.last_modified.isoformat(),
            "access_frequency": self.access_frequency,
            "growth_rate": self.growth_rate,
            "tier": self.tier.value,
            "age_days": self.age_days,
            "idle_days": self.idle_days,
        }


@dataclass
class RetentionPolicy:
    """Retention policy for a table or data category."""
    name: str
    table_pattern: str  # Regex pattern for matching tables
    retention_days: int
    archive_after_days: int
    delete_after_days: int
    compression: CompressionAlgorithm = CompressionAlgorithm.ZSTD
    tier_progression: List[Tuple[int, StorageTier]] = field(default_factory=list)
    exceptions: List[str] = field(default_factory=list)
    enabled: bool = True

    def __post_init__(self):
        if not self.tier_progression:
            # Default tier progression
            self.tier_progression = [
                (0, StorageTier.HOT),
                (7, StorageTier.WARM),
                (30, StorageTier.COLD),
                (90, StorageTier.ARCHIVE),
            ]

    def get_action(self, age_days: float, access_frequency: float) -> RetentionAction:
        """Determine what action to take based on data age and access patterns."""
        if age_days >= self.delete_after_days:
            return RetentionAction.DELETE
        if age_days >= self.archive_after_days:
            return RetentionAction.ARCHIVE
        if age_days >= self.tier_progression[-1][0]:
            return RetentionAction.TIER_DOWN
        if access_frequency < 0.01 and age_days > 7:  # Very low access
            return RetentionAction.COMPRESS
        return RetentionAction.KEEP

    def get_target_tier(self, age_days: float) -> StorageTier:
        """Get the target storage tier for given age."""
        target_tier = StorageTier.HOT
        for days, tier in sorted(self.tier_progression, reverse=True):
            if age_days >= days:
                target_tier = tier
                break
        return target_tier


@dataclass
class ArchiveRecord:
    """Record of an archived data segment."""
    archive_id: str
    source_table: str
    archive_path: Path
    original_size: int
    compressed_size: int
    row_count: int
    date_range: Tuple[datetime, datetime]
    compression: CompressionAlgorithm
    checksum: str
    created_at: datetime
    tier: StorageTier = StorageTier.ARCHIVE
    restored_count: int = 0
    last_restored: Optional[datetime] = None

    @property
    def compression_ratio(self) -> float:
        if self.original_size == 0:
            return 1.0
        return self.compressed_size / self.original_size

    @property
    def savings_bytes(self) -> int:
        return self.original_size - self.compressed_size


@dataclass
class LifecycleEvent:
    """Record of a lifecycle event."""
    event_id: str
    table_name: str
    event_type: str
    phase_from: LifecyclePhase
    phase_to: LifecyclePhase
    action: RetentionAction
    details: Dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Storage Monitor
# ---------------------------------------------------------------------------


class StorageMonitor:
    """Monitors storage usage and access patterns."""

    def __init__(self, state_root: Path):
        self.state_root = state_root
        self.metrics_log = state_root / METRICS_LOG_NAME
        self._metrics_cache: Dict[str, StorageMetrics] = {}
        self._access_log: Dict[str, List[datetime]] = defaultdict(list)
        self._lock = threading.Lock()

    def record_access(self, table_name: str) -> None:
        """Record a table access event."""
        with self._lock:
            now = datetime.now(timezone.utc)
            self._access_log[table_name].append(now)

            # Keep only last 24 hours
            cutoff = now - timedelta(hours=24)
            self._access_log[table_name] = [
                t for t in self._access_log[table_name] if t > cutoff
            ]

    def get_access_frequency(self, table_name: str) -> float:
        """Get access frequency (accesses per hour) for a table."""
        with self._lock:
            accesses = self._access_log.get(table_name, [])
            if not accesses:
                return 0.0

            hours = 24.0  # We track 24 hours
            return len(accesses) / hours

    def collect_metrics(self, table_name: str, storage_backend: Any) -> StorageMetrics:
        """Collect storage metrics for a table."""
        now = datetime.now(timezone.utc)

        # Get storage stats from backend (mock implementation)
        stats = self._get_table_stats(table_name, storage_backend)

        metrics = StorageMetrics(
            table_name=table_name,
            total_bytes=stats.get("total_bytes", 0),
            row_count=stats.get("row_count", 0),
            index_bytes=stats.get("index_bytes", 0),
            avg_row_size=stats.get("avg_row_size", 0.0),
            compression_ratio=stats.get("compression_ratio", 1.0),
            last_accessed=stats.get("last_accessed", now),
            last_modified=stats.get("last_modified", now),
            access_frequency=self.get_access_frequency(table_name),
            growth_rate=self._calculate_growth_rate(table_name, stats),
            tier=StorageTier(stats.get("tier", "hot")),
            created_at=stats.get("created_at", now),
        )

        # Cache and log
        self._metrics_cache[table_name] = metrics
        self._log_metrics(metrics)

        return metrics

    def _get_table_stats(self, table_name: str, storage_backend: Any) -> Dict[str, Any]:
        """Get raw stats from storage backend."""
        # This would interface with the actual storage backend
        # For now, return mock data
        return {
            "total_bytes": 1024 * 1024 * 10,  # 10MB
            "row_count": 10000,
            "index_bytes": 1024 * 1024,
            "avg_row_size": 1024,
            "compression_ratio": 0.7,
            "tier": "hot",
        }

    def _calculate_growth_rate(self, table_name: str, current_stats: Dict) -> float:
        """Calculate growth rate in bytes per day."""
        if table_name not in self._metrics_cache:
            return 0.0

        previous = self._metrics_cache[table_name]
        now = datetime.now(timezone.utc)
        days_elapsed = (now - previous.created_at).total_seconds() / 86400

        if days_elapsed < 0.001:  # Less than ~1 minute
            return 0.0

        bytes_change = current_stats.get("total_bytes", 0) - previous.total_bytes
        return bytes_change / days_elapsed

    def _log_metrics(self, metrics: StorageMetrics) -> None:
        """Log metrics to file."""
        self.metrics_log.parent.mkdir(parents=True, exist_ok=True)
        with self.metrics_log.open("a", encoding="utf-8") as f:
            record = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                **metrics.to_dict(),
            }
            f.write(json.dumps(record) + "\n")

    def get_all_metrics(self) -> List[StorageMetrics]:
        """Get all cached metrics."""
        return list(self._metrics_cache.values())

    def get_storage_summary(self) -> Dict[str, Any]:
        """Get summary of all storage usage."""
        metrics = self.get_all_metrics()
        if not metrics:
            return {"total_tables": 0}

        total_bytes = sum(m.total_bytes for m in metrics)
        total_rows = sum(m.row_count for m in metrics)

        return {
            "total_tables": len(metrics),
            "total_bytes": total_bytes,
            "total_gb": total_bytes / (1024 ** 3),
            "total_rows": total_rows,
            "avg_table_size_mb": mean(m.total_mb for m in metrics),
            "by_tier": self._group_by_tier(metrics),
        }

    def _group_by_tier(self, metrics: List[StorageMetrics]) -> Dict[str, Dict]:
        """Group metrics by storage tier."""
        by_tier: Dict[str, List[StorageMetrics]] = defaultdict(list)
        for m in metrics:
            by_tier[m.tier.value].append(m)

        return {
            tier: {
                "count": len(items),
                "total_bytes": sum(m.total_bytes for m in items),
                "total_gb": sum(m.total_gb for m in items),
            }
            for tier, items in by_tier.items()
        }


# ---------------------------------------------------------------------------
# Lifecycle Manager
# ---------------------------------------------------------------------------


class LifecycleManager:
    """Manages data lifecycle transitions."""

    def __init__(self, state_root: Path, policies: List[RetentionPolicy]):
        self.state_root = state_root
        self.policies = {p.name: p for p in policies}
        self.event_log = state_root / LIFECYCLE_LOG_NAME
        self._lock = threading.Lock()

    def add_policy(self, policy: RetentionPolicy) -> None:
        """Add or update a retention policy."""
        with self._lock:
            self.policies[policy.name] = policy
            logger.info(f"Added retention policy: {policy.name}")

    def remove_policy(self, name: str) -> None:
        """Remove a retention policy."""
        with self._lock:
            if name in self.policies:
                del self.policies[name]
                logger.info(f"Removed retention policy: {name}")

    def evaluate_table(
        self,
        metrics: StorageMetrics,
        policy: Optional[RetentionPolicy] = None,
    ) -> Tuple[RetentionAction, StorageTier]:
        """Evaluate what action to take for a table."""
        if policy is None:
            policy = self._find_matching_policy(metrics.table_name)

        if policy is None:
            return RetentionAction.KEEP, metrics.tier

        action = policy.get_action(metrics.age_days, metrics.access_frequency)
        target_tier = policy.get_target_tier(metrics.age_days)

        return action, target_tier

    def _find_matching_policy(self, table_name: str) -> Optional[RetentionPolicy]:
        """Find the first matching policy for a table."""
        import re
        for policy in self.policies.values():
            if policy.enabled and re.match(policy.table_pattern, table_name):
                return policy
        return None

    def execute_transition(
        self,
        table_name: str,
        from_phase: LifecyclePhase,
        to_phase: LifecyclePhase,
        action: RetentionAction,
        details: Dict[str, Any],
    ) -> LifecycleEvent:
        """Execute and record a lifecycle transition."""
        import uuid
        event = LifecycleEvent(
            event_id=str(uuid.uuid4()),
            table_name=table_name,
            event_type="transition",
            phase_from=from_phase,
            phase_to=to_phase,
            action=action,
            details=details,
        )

        self._log_event(event)
        return event

    def _log_event(self, event: LifecycleEvent) -> None:
        """Log lifecycle event to file."""
        self.event_log.parent.mkdir(parents=True, exist_ok=True)
        with self.event_log.open("a", encoding="utf-8") as f:
            record = {
                "event_id": event.event_id,
                "table_name": event.table_name,
                "event_type": event.event_type,
                "phase_from": event.phase_from.name,
                "phase_to": event.phase_to.name,
                "action": event.action.name,
                "details": event.details,
                "timestamp": event.timestamp.isoformat(),
            }
            f.write(json.dumps(record) + "\n")


# ---------------------------------------------------------------------------
# Compression Engine
# ---------------------------------------------------------------------------


class CompressionEngine:
    """Handles data compression and decompression."""

    ALGORITHMS: Dict[CompressionAlgorithm, Tuple[Callable, Callable]] = {}

    def __init__(self):
        self._init_algorithms()

    def _init_algorithms(self) -> None:
        """Initialize compression algorithm handlers."""
        # GZIP
        self.ALGORITHMS[CompressionAlgorithm.GZIP] = (
            lambda data: gzip.compress(data, compresslevel=9),
            gzip.decompress,
        )

        # NONE (passthrough)
        self.ALGORITHMS[CompressionAlgorithm.NONE] = (
            lambda data: data,
            lambda data: data,
        )

        # LZ4 (if available)
        try:
            import lz4.frame
            self.ALGORITHMS[CompressionAlgorithm.LZ4] = (
                lz4.frame.compress,
                lz4.frame.decompress,
            )
        except ImportError:
            pass

        # ZSTD (if available)
        try:
            import zstandard as zstd
            cctx = zstd.ZstdCompressor(level=19)
            dctx = zstd.ZstdDecompressor()
            self.ALGORITHMS[CompressionAlgorithm.ZSTD] = (
                cctx.compress,
                dctx.decompress,
            )
        except ImportError:
            pass

    def compress(
        self,
        data: bytes,
        algorithm: CompressionAlgorithm = CompressionAlgorithm.ZSTD,
    ) -> Tuple[bytes, float]:
        """Compress data and return (compressed_data, ratio)."""
        if algorithm not in self.ALGORITHMS:
            # Fallback to gzip
            algorithm = CompressionAlgorithm.GZIP

        compress_fn, _ = self.ALGORITHMS[algorithm]
        compressed = compress_fn(data)
        ratio = len(compressed) / len(data) if data else 1.0

        return compressed, ratio

    def decompress(
        self,
        data: bytes,
        algorithm: CompressionAlgorithm,
    ) -> bytes:
        """Decompress data."""
        if algorithm not in self.ALGORITHMS:
            raise ValueError(f"Unsupported algorithm: {algorithm}")

        _, decompress_fn = self.ALGORITHMS[algorithm]
        return decompress_fn(data)

    def best_algorithm(self, sample_data: bytes) -> CompressionAlgorithm:
        """Find the best compression algorithm for given data."""
        best_algo = CompressionAlgorithm.GZIP
        best_ratio = 1.0

        for algo in self.ALGORITHMS:
            try:
                _, ratio = self.compress(sample_data, algo)
                if ratio < best_ratio:
                    best_ratio = ratio
                    best_algo = algo
            except Exception:
                continue

        return best_algo


# ---------------------------------------------------------------------------
# Archive Writer
# ---------------------------------------------------------------------------


class ArchiveWriter:
    """Writes data to archive storage."""

    def __init__(
        self,
        archive_root: Path,
        compression_engine: CompressionEngine,
    ):
        self.archive_root = archive_root
        self.compression = compression_engine
        self.manifest_path = archive_root / ARCHIVE_MANIFEST_NAME
        self._manifest: Dict[str, ArchiveRecord] = {}
        self._load_manifest()

    def _load_manifest(self) -> None:
        """Load existing archive manifest."""
        if self.manifest_path.exists():
            with self.manifest_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
                # Convert to ArchiveRecord objects
                for archive_id, record in data.items():
                    self._manifest[archive_id] = self._dict_to_record(record)

    def _dict_to_record(self, data: Dict) -> ArchiveRecord:
        """Convert dictionary to ArchiveRecord."""
        return ArchiveRecord(
            archive_id=data["archive_id"],
            source_table=data["source_table"],
            archive_path=Path(data["archive_path"]),
            original_size=data["original_size"],
            compressed_size=data["compressed_size"],
            row_count=data["row_count"],
            date_range=(
                datetime.fromisoformat(data["date_range"][0]),
                datetime.fromisoformat(data["date_range"][1]),
            ),
            compression=CompressionAlgorithm(data["compression"]),
            checksum=data["checksum"],
            created_at=datetime.fromisoformat(data["created_at"]),
            tier=StorageTier(data.get("tier", "archive")),
            restored_count=data.get("restored_count", 0),
        )

    def _save_manifest(self) -> None:
        """Save archive manifest to disk."""
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with self.manifest_path.open("w", encoding="utf-8") as f:
            data = {}
            for archive_id, record in self._manifest.items():
                data[archive_id] = {
                    "archive_id": record.archive_id,
                    "source_table": record.source_table,
                    "archive_path": str(record.archive_path),
                    "original_size": record.original_size,
                    "compressed_size": record.compressed_size,
                    "row_count": record.row_count,
                    "date_range": [
                        record.date_range[0].isoformat(),
                        record.date_range[1].isoformat(),
                    ],
                    "compression": record.compression.value,
                    "checksum": record.checksum,
                    "created_at": record.created_at.isoformat(),
                    "tier": record.tier.value,
                    "restored_count": record.restored_count,
                }
            json.dump(data, f, indent=2)

    def archive(
        self,
        table_name: str,
        data: bytes,
        row_count: int,
        date_range: Tuple[datetime, datetime],
        algorithm: Optional[CompressionAlgorithm] = None,
    ) -> ArchiveRecord:
        """Archive data to storage."""
        import uuid

        archive_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        # Determine compression algorithm
        if algorithm is None:
            algorithm = self.compression.best_algorithm(data[:10000])

        # Compress
        compressed, ratio = self.compression.compress(data, algorithm)

        # Calculate checksum
        checksum = hashlib.sha256(compressed).hexdigest()

        # Determine archive path
        year = date_range[0].year
        month = date_range[0].month
        archive_dir = self.archive_root / table_name / str(year) / f"{month:02d}"
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_path = archive_dir / f"{archive_id}.{algorithm.value}"

        # Write to disk
        with archive_path.open("wb") as f:
            f.write(compressed)

        # Create record
        record = ArchiveRecord(
            archive_id=archive_id,
            source_table=table_name,
            archive_path=archive_path,
            original_size=len(data),
            compressed_size=len(compressed),
            row_count=row_count,
            date_range=date_range,
            compression=algorithm,
            checksum=checksum,
            created_at=now,
        )

        # Update manifest
        self._manifest[archive_id] = record
        self._save_manifest()

        logger.info(
            f"Archived {table_name}: {len(data)} -> {len(compressed)} bytes "
            f"({ratio:.1%} compression)"
        )

        return record

    def restore(self, archive_id: str) -> bytes:
        """Restore data from archive."""
        if archive_id not in self._manifest:
            raise ValueError(f"Archive not found: {archive_id}")

        record = self._manifest[archive_id]

        # Read compressed data
        with record.archive_path.open("rb") as f:
            compressed = f.read()

        # Verify checksum
        if hashlib.sha256(compressed).hexdigest() != record.checksum:
            raise ValueError(f"Archive checksum mismatch: {archive_id}")

        # Decompress
        data = self.compression.decompress(compressed, record.compression)

        # Update restore count
        record.restored_count += 1
        record.last_restored = datetime.now(timezone.utc)
        self._save_manifest()

        logger.info(f"Restored archive {archive_id}: {len(data)} bytes")

        return data

    def delete_archive(self, archive_id: str) -> None:
        """Delete an archive."""
        if archive_id not in self._manifest:
            raise ValueError(f"Archive not found: {archive_id}")

        record = self._manifest[archive_id]

        # Delete file
        if record.archive_path.exists():
            record.archive_path.unlink()

        # Remove from manifest
        del self._manifest[archive_id]
        self._save_manifest()

        logger.info(f"Deleted archive {archive_id}")

    def get_archives(self, table_name: Optional[str] = None) -> List[ArchiveRecord]:
        """Get all archives, optionally filtered by table."""
        records = list(self._manifest.values())
        if table_name:
            records = [r for r in records if r.source_table == table_name]
        return sorted(records, key=lambda r: r.created_at, reverse=True)


# ---------------------------------------------------------------------------
# Main Archivist Agent
# ---------------------------------------------------------------------------


class Archivist:
    """The Archivist agent - guardian of data lifecycle.

    The Archivist monitors storage patterns, enforces retention policies,
    manages archival processes, and maintains the delicate balance between
    data preservation and storage efficiency.

    Usage:
        archivist = Archivist(state_root=Path("/srv/roaddb/archivist"))
        archivist.run()  # Start monitoring loop

        # Or use directly
        metrics = archivist.collect_metrics("users")
        action, tier = archivist.evaluate("users")
    """

    def __init__(
        self,
        state_root: Path = DEFAULT_STATE_ROOT,
        seed_path: Optional[Path] = None,
    ):
        self.state_root = state_root
        self.state_root.mkdir(parents=True, exist_ok=True)

        # Load seed configuration if available
        self.seed: Optional[ArchivistSeed] = None
        if seed_path and seed_path.exists():
            self.seed = load_seed(seed_path)

        # Initialize components
        self.monitor = StorageMonitor(state_root)
        self.lifecycle = LifecycleManager(
            state_root,
            policies=self._default_policies(),
        )
        self.compression = CompressionEngine()
        self.archive_writer = ArchiveWriter(
            state_root / "archives",
            self.compression,
        )

        # State
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self.journal_path = state_root / JOURNAL_NAME

        logger.info(f"Archivist initialized at {state_root}")

    def _default_policies(self) -> List[RetentionPolicy]:
        """Create default retention policies."""
        return [
            RetentionPolicy(
                name="default",
                table_pattern=".*",
                retention_days=365,
                archive_after_days=90,
                delete_after_days=730,  # 2 years
            ),
            RetentionPolicy(
                name="logs",
                table_pattern=".*_logs?$",
                retention_days=90,
                archive_after_days=30,
                delete_after_days=180,
            ),
            RetentionPolicy(
                name="audit",
                table_pattern=".*audit.*",
                retention_days=2555,  # 7 years
                archive_after_days=365,
                delete_after_days=2920,  # 8 years
            ),
            RetentionPolicy(
                name="temp",
                table_pattern="^tmp_.*",
                retention_days=7,
                archive_after_days=3,
                delete_after_days=14,
            ),
        ]

    def collect_metrics(
        self,
        table_name: str,
        storage_backend: Any = None,
    ) -> StorageMetrics:
        """Collect storage metrics for a table."""
        return self.monitor.collect_metrics(table_name, storage_backend)

    def evaluate(
        self,
        table_name: str,
        metrics: Optional[StorageMetrics] = None,
    ) -> Tuple[RetentionAction, StorageTier]:
        """Evaluate retention action for a table."""
        if metrics is None:
            metrics = self.monitor._metrics_cache.get(table_name)
            if metrics is None:
                metrics = self.collect_metrics(table_name)

        return self.lifecycle.evaluate_table(metrics)

    def archive_table(
        self,
        table_name: str,
        data: bytes,
        row_count: int,
        date_range: Tuple[datetime, datetime],
    ) -> ArchiveRecord:
        """Archive table data."""
        return self.archive_writer.archive(
            table_name, data, row_count, date_range
        )

    def restore_archive(self, archive_id: str) -> bytes:
        """Restore data from archive."""
        return self.archive_writer.restore(archive_id)

    def run(self, interval_seconds: int = 3600) -> None:
        """Start the archivist monitoring loop."""
        self._running = True
        logger.info(f"Archivist starting (interval: {interval_seconds}s)")

        while self._running:
            try:
                self._run_cycle()
            except Exception as e:
                logger.error(f"Archivist cycle error: {e}")

            time.sleep(interval_seconds)

    def _run_cycle(self) -> None:
        """Run a single archivist cycle."""
        metrics = self.monitor.get_all_metrics()

        for m in metrics:
            action, target_tier = self.evaluate(m.table_name, m)

            if action != RetentionAction.KEEP:
                self._execute_action(m, action, target_tier)

        # Update journal
        self._write_journal_entry()

    def _execute_action(
        self,
        metrics: StorageMetrics,
        action: RetentionAction,
        target_tier: StorageTier,
    ) -> None:
        """Execute a retention action."""
        logger.info(
            f"Executing {action.name} on {metrics.table_name} "
            f"(tier: {metrics.tier.value} -> {target_tier.value})"
        )

        # Record lifecycle event
        self.lifecycle.execute_transition(
            table_name=metrics.table_name,
            from_phase=LifecyclePhase.ACTIVE,
            to_phase=self._action_to_phase(action),
            action=action,
            details={
                "original_tier": metrics.tier.value,
                "target_tier": target_tier.value,
                "size_bytes": metrics.total_bytes,
                "age_days": metrics.age_days,
            },
        )

    def _action_to_phase(self, action: RetentionAction) -> LifecyclePhase:
        """Map action to lifecycle phase."""
        mapping = {
            RetentionAction.KEEP: LifecyclePhase.ACTIVE,
            RetentionAction.COMPRESS: LifecyclePhase.AGING,
            RetentionAction.TIER_DOWN: LifecyclePhase.AGING,
            RetentionAction.ARCHIVE: LifecyclePhase.ARCHIVED,
            RetentionAction.DELETE: LifecyclePhase.DELETED,
        }
        return mapping.get(action, LifecyclePhase.ACTIVE)

    def _write_journal_entry(self) -> None:
        """Write a journal entry summarizing the current state."""
        summary = self.monitor.get_storage_summary()
        now = datetime.now(timezone.utc)

        entry = f"""
## {now.strftime('%Y-%m-%d %H:%M:%S UTC')}

### Storage Summary
- Tables: {summary.get('total_tables', 0)}
- Total Size: {summary.get('total_gb', 0):.2f} GB
- Total Rows: {summary.get('total_rows', 0):,}

### By Tier
"""
        for tier, stats in summary.get("by_tier", {}).items():
            entry += f"- **{tier}**: {stats['count']} tables, {stats['total_gb']:.2f} GB\n"

        entry += "\n---\n\n"

        with self.journal_path.open("a", encoding="utf-8") as f:
            f.write(entry)

    def stop(self) -> None:
        """Stop the archivist."""
        self._running = False
        logger.info("Archivist stopping")

    def status(self) -> Dict[str, Any]:
        """Get archivist status."""
        return {
            "running": self._running,
            "state_root": str(self.state_root),
            "storage_summary": self.monitor.get_storage_summary(),
            "policies": list(self.lifecycle.policies.keys()),
            "archives": len(self.archive_writer._manifest),
        }


# ---------------------------------------------------------------------------
# CLI Interface
# ---------------------------------------------------------------------------


def create_cli_parser() -> argparse.ArgumentParser:
    """Create the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="archivist",
        description="RoadDB Archivist - Data Lifecycle Management Agent",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Run command
    run_parser = subparsers.add_parser("run", help="Start the archivist daemon")
    run_parser.add_argument(
        "--interval", type=int, default=3600,
        help="Check interval in seconds (default: 3600)"
    )
    run_parser.add_argument(
        "--state-root", type=Path, default=DEFAULT_STATE_ROOT,
        help="State directory root"
    )

    # Status command
    status_parser = subparsers.add_parser("status", help="Show archivist status")
    status_parser.add_argument(
        "--state-root", type=Path, default=DEFAULT_STATE_ROOT,
        help="State directory root"
    )

    # Archive command
    archive_parser = subparsers.add_parser("archive", help="Archive a table")
    archive_parser.add_argument("table", help="Table name to archive")
    archive_parser.add_argument(
        "--state-root", type=Path, default=DEFAULT_STATE_ROOT,
        help="State directory root"
    )

    # Restore command
    restore_parser = subparsers.add_parser("restore", help="Restore from archive")
    restore_parser.add_argument("archive_id", help="Archive ID to restore")
    restore_parser.add_argument(
        "--state-root", type=Path, default=DEFAULT_STATE_ROOT,
        help="State directory root"
    )

    # List archives command
    list_parser = subparsers.add_parser("list", help="List archives")
    list_parser.add_argument("--table", help="Filter by table name")
    list_parser.add_argument(
        "--state-root", type=Path, default=DEFAULT_STATE_ROOT,
        help="State directory root"
    )

    return parser


def main() -> None:
    """Main entry point for the archivist CLI."""
    parser = create_cli_parser()
    args = parser.parse_args()

    if args.command == "run":
        archivist = Archivist(state_root=args.state_root)
        archivist.run(interval_seconds=args.interval)

    elif args.command == "status":
        archivist = Archivist(state_root=args.state_root)
        status = archivist.status()
        print(json.dumps(status, indent=2, default=str))

    elif args.command == "list":
        archivist = Archivist(state_root=args.state_root)
        archives = archivist.archive_writer.get_archives(args.table)
        for a in archives:
            print(f"{a.archive_id}: {a.source_table} "
                  f"({a.compressed_size / 1024:.1f} KB, {a.row_count} rows)")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
