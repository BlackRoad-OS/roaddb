"""RoadDB - Distributed Database Engine for BlackRoad OS.

RoadDB is a sophisticated distributed database system that provides:
- Multiple storage backends (SQLite, PostgreSQL, Memory, Distributed)
- Advanced query parsing and optimization
- Automatic sharding and replication
- Agent-based data management (Archivist, Indexer, Guardian, Analyst)
- Real-time streaming and change data capture
- Full CLI and API interfaces

Architecture:
    ┌─────────────────────────────────────────────────────────────────┐
    │                         RoadDB Engine                          │
    ├─────────────────────────────────────────────────────────────────┤
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
    │  │   Query     │  │  Storage    │  │ Replication │             │
    │  │   Engine    │──│  Engines    │──│   Manager   │             │
    │  └─────────────┘  └─────────────┘  └─────────────┘             │
    │         │               │               │                       │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
    │  │  Sharding   │  │   Agents    │  │  Streaming  │             │
    │  │ Coordinator │──│  (AI/ML)    │──│    CDC      │             │
    │  └─────────────┘  └─────────────┘  └─────────────┘             │
    └─────────────────────────────────────────────────────────────────┘

Usage:
    from roaddb_core import RoadDB, Query, Schema

    # Initialize database
    db = RoadDB(backend="distributed", replicas=3)

    # Create schema
    db.create_table("users", Schema(
        id=Column(Integer, primary_key=True),
        name=Column(String(100)),
        email=Column(String(255), unique=True)
    ))

    # Query data
    results = db.query("SELECT * FROM users WHERE active = true")

    # Stream changes
    async for change in db.stream("users"):
        print(f"Change detected: {change}")

CLI:
    $ roaddb init --backend sqlite --path ./data
    $ roaddb query "SELECT * FROM users"
    $ roaddb replicate --source primary --target replica-1
    $ roaddb shard --table users --key user_id --partitions 16

API:
    POST /api/v1/query
    POST /api/v1/execute
    GET  /api/v1/tables
    GET  /api/v1/health

Copyright (c) 2024-2026 BlackRoad OS, Inc. All rights reserved.
"""

from __future__ import annotations

__version__ = "2.0.0"
__author__ = "BlackRoad OS, Inc."
__email__ = "engineering@blackroad.io"

# Core exports
from roaddb_core.engine import RoadDB, Connection, Transaction
from roaddb_core.query.parser import Query, QueryParser
from roaddb_core.query.executor import QueryExecutor, QueryPlan
from roaddb_core.storage.base import StorageEngine, StorageBackend
from roaddb_core.schema import Schema, Column, Table, Index
from roaddb_core.types import (
    Integer, String, Float, Boolean, DateTime, JSON, Binary,
    Array, UUID, Decimal, Text, Timestamp
)

# Agent exports
from roaddb_core.agents.archivist import Archivist
from roaddb_core.agents.indexer import Indexer
from roaddb_core.agents.guardian import Guardian
from roaddb_core.agents.analyst import Analyst
from roaddb_core.agents.replicator import Replicator

# Replication exports
from roaddb_core.replication.manager import ReplicationManager
from roaddb_core.replication.raft import RaftConsensus
from roaddb_core.replication.wal import WriteAheadLog

# Sharding exports
from roaddb_core.sharding.coordinator import ShardCoordinator
from roaddb_core.sharding.router import ShardRouter
from roaddb_core.sharding.strategy import HashSharding, RangeSharding, ConsistentHashing

# Streaming exports
from roaddb_core.streaming.cdc import ChangeDataCapture
from roaddb_core.streaming.publisher import StreamPublisher

__all__ = [
    # Version
    "__version__",

    # Core
    "RoadDB",
    "Connection",
    "Transaction",

    # Query
    "Query",
    "QueryParser",
    "QueryExecutor",
    "QueryPlan",

    # Storage
    "StorageEngine",
    "StorageBackend",

    # Schema
    "Schema",
    "Column",
    "Table",
    "Index",

    # Types
    "Integer",
    "String",
    "Float",
    "Boolean",
    "DateTime",
    "JSON",
    "Binary",
    "Array",
    "UUID",
    "Decimal",
    "Text",
    "Timestamp",

    # Agents
    "Archivist",
    "Indexer",
    "Guardian",
    "Analyst",
    "Replicator",

    # Replication
    "ReplicationManager",
    "RaftConsensus",
    "WriteAheadLog",

    # Sharding
    "ShardCoordinator",
    "ShardRouter",
    "HashSharding",
    "RangeSharding",
    "ConsistentHashing",

    # Streaming
    "ChangeDataCapture",
    "StreamPublisher",
]
