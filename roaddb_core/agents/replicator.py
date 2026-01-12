"""RoadDB Replicator Agent - Data replication and consensus management.

Like Lucidia Core's chemist agent that manages molecular transformations,
the Replicator agent manages data transformations across distributed nodes,
ensuring consistency through consensus protocols and automatic failover.

Architecture:
    ┌─────────────────────────────────────────────────────────────────────┐
    │                        Replicator Agent                            │
    ├─────────────────────────────────────────────────────────────────────┤
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
    │  │   Raft      │  │   Write     │  │   Failover  │                 │
    │  │   Consensus │──│   Ahead Log │──│   Manager   │                 │
    │  └─────────────┘  └─────────────┘  └─────────────┘                 │
    │         │               │               │                           │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
    │  │   Node      │  │   Sync      │  │   Health    │                 │
    │  │   Manager   │──│   Engine    │──│   Monitor   │                 │
    │  └─────────────┘  └─────────────┘  └─────────────┘                 │
    └─────────────────────────────────────────────────────────────────────┘

Core Principle: "Consistency is the foundation of trust in distributed systems."
Moral Constant: Never lose data, always maintain integrity.

Copyright (c) 2024-2026 BlackRoad OS, Inc. All rights reserved.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import struct
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Dict, Generic, Iterator, List, Optional, Set, Tuple, TypeVar, Union

import yaml

# Configure logging
logger = logging.getLogger(__name__)

# Default paths
DEFAULT_STATE_ROOT = Path.home() / ".roaddb" / "replicator"


class NodeState(Enum):
    """Raft node states."""

    FOLLOWER = auto()
    CANDIDATE = auto()
    LEADER = auto()
    OFFLINE = auto()


class ReplicationMode(Enum):
    """Replication modes."""

    SYNC = auto()  # Synchronous - wait for all replicas
    ASYNC = auto()  # Asynchronous - don't wait
    SEMI_SYNC = auto()  # Wait for majority


class ConsistencyLevel(Enum):
    """Read/write consistency levels."""

    ONE = auto()  # Single node acknowledgment
    QUORUM = auto()  # Majority acknowledgment
    ALL = auto()  # All nodes acknowledgment
    LOCAL_ONE = auto()  # Single local datacenter node
    LOCAL_QUORUM = auto()  # Majority in local datacenter


@dataclass
class LogEntry:
    """Entry in the write-ahead log."""

    term: int
    index: int
    command: str
    data: bytes
    timestamp: datetime = field(default_factory=datetime.now)
    checksum: Optional[str] = None

    def __post_init__(self):
        if self.checksum is None:
            self.checksum = self._compute_checksum()

    def _compute_checksum(self) -> str:
        """Compute checksum for integrity verification."""
        content = f"{self.term}:{self.index}:{self.command}".encode() + self.data
        return hashlib.sha256(content).hexdigest()[:16]

    def verify(self) -> bool:
        """Verify entry integrity."""
        return self.checksum == self._compute_checksum()


@dataclass
class NodeInfo:
    """Information about a cluster node."""

    node_id: str
    address: str
    port: int
    state: NodeState = NodeState.OFFLINE
    last_heartbeat: Optional[datetime] = None
    last_log_index: int = 0
    last_log_term: int = 0
    vote_granted: bool = False
    datacenter: Optional[str] = None
    rack: Optional[str] = None
    is_learner: bool = False


@dataclass
class ClusterConfig:
    """Cluster configuration."""

    cluster_id: str
    nodes: List[NodeInfo] = field(default_factory=list)
    replication_factor: int = 3
    mode: ReplicationMode = ReplicationMode.SEMI_SYNC
    read_consistency: ConsistencyLevel = ConsistencyLevel.QUORUM
    write_consistency: ConsistencyLevel = ConsistencyLevel.QUORUM
    heartbeat_interval_ms: int = 150
    election_timeout_ms: int = 300


@dataclass
class ReplicationStats:
    """Replication statistics."""

    total_writes: int = 0
    replicated_writes: int = 0
    failed_writes: int = 0
    replication_lag_ms: float = 0.0
    bytes_replicated: int = 0
    last_sync_time: Optional[datetime] = None


# =============================================================================
# Write-Ahead Log
# =============================================================================


class WriteAheadLog:
    """Write-ahead log for durability and replication.

    Implements a persistent, append-only log with:
    - Sequential writes for performance
    - Checksum verification for integrity
    - Segment rotation for management
    - Compaction for space efficiency
    """

    def __init__(self, log_dir: Path, segment_size: int = 64 * 1024 * 1024):
        """Initialize WAL.

        Args:
            log_dir: Directory for log segments
            segment_size: Maximum size per segment (64MB default)
        """
        self.log_dir = log_dir
        self.segment_size = segment_size
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self._entries: List[LogEntry] = []
        self._current_segment: Optional[Path] = None
        self._segment_file: Optional[Any] = None
        self._lock = threading.RLock()

        self._load_segments()

    def append(self, term: int, command: str, data: bytes) -> LogEntry:
        """Append entry to log.

        Args:
            term: Current term
            command: Command type
            data: Command data

        Returns:
            Created log entry
        """
        with self._lock:
            index = len(self._entries) + 1

            entry = LogEntry(term=term, index=index, command=command, data=data)

            self._entries.append(entry)
            self._write_entry(entry)

            return entry

    def get(self, index: int) -> Optional[LogEntry]:
        """Get entry by index."""
        with self._lock:
            if 1 <= index <= len(self._entries):
                return self._entries[index - 1]
            return None

    def get_range(self, start_index: int, end_index: Optional[int] = None) -> List[LogEntry]:
        """Get entries in range."""
        with self._lock:
            end = end_index or len(self._entries)
            start = max(1, start_index) - 1
            end = min(end, len(self._entries))
            return self._entries[start:end]

    def truncate(self, from_index: int) -> int:
        """Truncate log from index.

        Args:
            from_index: First index to remove

        Returns:
            Number of entries removed
        """
        with self._lock:
            if from_index > len(self._entries):
                return 0

            removed = len(self._entries) - from_index + 1
            self._entries = self._entries[: from_index - 1]
            self._rewrite_segments()

            return removed

    def last_index(self) -> int:
        """Get last log index."""
        with self._lock:
            return len(self._entries)

    def last_term(self) -> int:
        """Get term of last entry."""
        with self._lock:
            if self._entries:
                return self._entries[-1].term
            return 0

    def _load_segments(self) -> None:
        """Load existing log segments."""
        segments = sorted(self.log_dir.glob("segment_*.wal"))

        for segment in segments:
            with open(segment, "rb") as f:
                while True:
                    header = f.read(20)  # term(8) + index(8) + cmd_len(4)
                    if len(header) < 20:
                        break

                    term, index, cmd_len = struct.unpack(">QQI", header)
                    command = f.read(cmd_len).decode()
                    data_len = struct.unpack(">I", f.read(4))[0]
                    data = f.read(data_len)
                    checksum = f.read(16).decode()

                    entry = LogEntry(
                        term=term,
                        index=index,
                        command=command,
                        data=data,
                        checksum=checksum,
                    )

                    if entry.verify():
                        self._entries.append(entry)
                    else:
                        logger.warning(f"Corrupt entry at index {index}")
                        break

        if segments:
            self._current_segment = segments[-1]

    def _write_entry(self, entry: LogEntry) -> None:
        """Write entry to current segment."""
        if self._segment_file is None or (
            self._current_segment and self._current_segment.stat().st_size > self.segment_size
        ):
            self._rotate_segment()

        if self._segment_file:
            # Write format: term(8) + index(8) + cmd_len(4) + cmd + data_len(4) + data + checksum(16)
            cmd_bytes = entry.command.encode()
            header = struct.pack(">QQI", entry.term, entry.index, len(cmd_bytes))
            data_header = struct.pack(">I", len(entry.data))

            self._segment_file.write(header)
            self._segment_file.write(cmd_bytes)
            self._segment_file.write(data_header)
            self._segment_file.write(entry.data)
            self._segment_file.write(entry.checksum.encode())
            self._segment_file.flush()
            os.fsync(self._segment_file.fileno())

    def _rotate_segment(self) -> None:
        """Rotate to a new segment file."""
        if self._segment_file:
            self._segment_file.close()

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        self._current_segment = self.log_dir / f"segment_{timestamp}.wal"
        self._segment_file = open(self._current_segment, "ab")

    def _rewrite_segments(self) -> None:
        """Rewrite all segments after truncation."""
        # Close current file
        if self._segment_file:
            self._segment_file.close()
            self._segment_file = None

        # Remove all segments
        for segment in self.log_dir.glob("segment_*.wal"):
            segment.unlink()

        # Rewrite entries
        self._current_segment = None
        for entry in self._entries:
            self._write_entry(entry)

    def compact(self, up_to_index: int) -> int:
        """Compact log up to index (remove old entries)."""
        with self._lock:
            if up_to_index <= 0 or up_to_index > len(self._entries):
                return 0

            removed = up_to_index
            self._entries = self._entries[up_to_index:]

            # Renumber entries
            for i, entry in enumerate(self._entries):
                entry.index = i + 1

            self._rewrite_segments()
            return removed

    def close(self) -> None:
        """Close the WAL."""
        with self._lock:
            if self._segment_file:
                self._segment_file.close()
                self._segment_file = None


# =============================================================================
# Raft Consensus
# =============================================================================


class RaftConsensus:
    """Raft consensus implementation.

    Implements the Raft consensus algorithm for distributed coordination:
    - Leader election
    - Log replication
    - Safety guarantees
    """

    def __init__(self, node_id: str, config: ClusterConfig, wal: WriteAheadLog):
        """Initialize Raft consensus.

        Args:
            node_id: This node's ID
            config: Cluster configuration
            wal: Write-ahead log
        """
        self.node_id = node_id
        self.config = config
        self.wal = wal

        # Raft state
        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None

        # Volatile state
        self.commit_index = 0
        self.last_applied = 0

        # Leader state
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # Timing
        self.last_heartbeat = datetime.now()
        self.election_timeout = self._random_election_timeout()

        # Callbacks
        self.on_state_change: Optional[Callable[[NodeState], None]] = None
        self.on_commit: Optional[Callable[[LogEntry], None]] = None

        self._lock = threading.RLock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def _random_election_timeout(self) -> timedelta:
        """Generate random election timeout."""
        base = self.config.election_timeout_ms
        return timedelta(milliseconds=random.randint(base, base * 2))

    def start(self) -> None:
        """Start the consensus algorithm."""
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logger.info(f"Raft consensus started for node {self.node_id}")

    def stop(self) -> None:
        """Stop the consensus algorithm."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _run_loop(self) -> None:
        """Main consensus loop."""
        while self._running:
            with self._lock:
                if self.state == NodeState.LEADER:
                    self._leader_duties()
                elif self.state == NodeState.FOLLOWER:
                    self._follower_duties()
                elif self.state == NodeState.CANDIDATE:
                    self._candidate_duties()

            time.sleep(self.config.heartbeat_interval_ms / 1000)

    def _leader_duties(self) -> None:
        """Perform leader duties."""
        # Send heartbeats
        for node in self.config.nodes:
            if node.node_id != self.node_id:
                self._send_append_entries(node)

    def _follower_duties(self) -> None:
        """Perform follower duties."""
        # Check for election timeout
        if datetime.now() - self.last_heartbeat > self.election_timeout:
            logger.info(f"Election timeout, becoming candidate")
            self._become_candidate()

    def _candidate_duties(self) -> None:
        """Perform candidate duties."""
        # Request votes
        votes_received = 1  # Vote for self

        for node in self.config.nodes:
            if node.node_id != self.node_id:
                if self._request_vote(node):
                    votes_received += 1

        # Check for majority
        if votes_received > len(self.config.nodes) // 2:
            self._become_leader()
        elif datetime.now() - self.last_heartbeat > self.election_timeout:
            # Start new election
            self._become_candidate()

    def _become_candidate(self) -> None:
        """Transition to candidate state."""
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat = datetime.now()
        self.election_timeout = self._random_election_timeout()

        if self.on_state_change:
            self.on_state_change(self.state)

        logger.info(f"Node {self.node_id} became candidate for term {self.current_term}")

    def _become_leader(self) -> None:
        """Transition to leader state."""
        self.state = NodeState.LEADER
        self.leader_id = self.node_id

        # Initialize leader state
        last_index = self.wal.last_index()
        for node in self.config.nodes:
            self.next_index[node.node_id] = last_index + 1
            self.match_index[node.node_id] = 0

        if self.on_state_change:
            self.on_state_change(self.state)

        logger.info(f"Node {self.node_id} became leader for term {self.current_term}")

    def _become_follower(self, term: int, leader_id: Optional[str] = None) -> None:
        """Transition to follower state."""
        self.state = NodeState.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.leader_id = leader_id
        self.last_heartbeat = datetime.now()

        if self.on_state_change:
            self.on_state_change(self.state)

        logger.info(f"Node {self.node_id} became follower for term {term}")

    def _request_vote(self, node: NodeInfo) -> bool:
        """Request vote from a node."""
        # In real implementation, this would be an RPC
        # Simulated for demonstration
        return random.random() > 0.5

    def _send_append_entries(self, node: NodeInfo) -> bool:
        """Send append entries to a node."""
        # In real implementation, this would be an RPC
        # Simulated for demonstration
        return True

    def append(self, command: str, data: bytes) -> Tuple[int, int]:
        """Append a new entry (leader only).

        Args:
            command: Command type
            data: Command data

        Returns:
            (term, index) of the new entry
        """
        with self._lock:
            if self.state != NodeState.LEADER:
                raise RuntimeError("Not leader")

            entry = self.wal.append(self.current_term, command, data)
            return entry.term, entry.index

    def receive_append_entries(
        self,
        term: int,
        leader_id: str,
        prev_log_index: int,
        prev_log_term: int,
        entries: List[LogEntry],
        leader_commit: int,
    ) -> Tuple[int, bool]:
        """Handle append entries RPC.

        Returns:
            (current_term, success)
        """
        with self._lock:
            # Reply false if term < currentTerm
            if term < self.current_term:
                return self.current_term, False

            # Update state
            if term > self.current_term:
                self._become_follower(term, leader_id)
            elif self.state != NodeState.FOLLOWER:
                self._become_follower(term, leader_id)

            self.last_heartbeat = datetime.now()
            self.leader_id = leader_id

            # Reply false if log doesn't contain entry at prevLogIndex with prevLogTerm
            if prev_log_index > 0:
                prev_entry = self.wal.get(prev_log_index)
                if prev_entry is None or prev_entry.term != prev_log_term:
                    return self.current_term, False

            # Append new entries
            for entry in entries:
                existing = self.wal.get(entry.index)
                if existing and existing.term != entry.term:
                    # Conflict - truncate
                    self.wal.truncate(entry.index)

                if not existing:
                    self.wal.append(entry.term, entry.command, entry.data)

            # Update commit index
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self.wal.last_index())
                self._apply_committed()

            return self.current_term, True

    def receive_vote_request(
        self,
        term: int,
        candidate_id: str,
        last_log_index: int,
        last_log_term: int,
    ) -> Tuple[int, bool]:
        """Handle vote request RPC.

        Returns:
            (current_term, vote_granted)
        """
        with self._lock:
            # Reply false if term < currentTerm
            if term < self.current_term:
                return self.current_term, False

            # Update term if necessary
            if term > self.current_term:
                self._become_follower(term)

            # Check if already voted
            if self.voted_for is not None and self.voted_for != candidate_id:
                return self.current_term, False

            # Check log is at least as up-to-date
            my_last_term = self.wal.last_term()
            my_last_index = self.wal.last_index()

            if last_log_term < my_last_term:
                return self.current_term, False
            if last_log_term == my_last_term and last_log_index < my_last_index:
                return self.current_term, False

            # Grant vote
            self.voted_for = candidate_id
            return self.current_term, True

    def _apply_committed(self) -> None:
        """Apply committed entries."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.wal.get(self.last_applied)
            if entry and self.on_commit:
                self.on_commit(entry)


# =============================================================================
# Sync Engine
# =============================================================================


class SyncEngine:
    """Handles data synchronization between nodes.

    Supports:
    - Full sync (snapshot + replay)
    - Incremental sync (WAL shipping)
    - Conflict resolution
    """

    def __init__(self, wal: WriteAheadLog, node_id: str):
        """Initialize sync engine.

        Args:
            wal: Write-ahead log
            node_id: This node's ID
        """
        self.wal = wal
        self.node_id = node_id
        self.sync_state: Dict[str, int] = {}  # node_id -> last_synced_index
        self._lock = threading.RLock()

    def get_entries_for_sync(self, target_node: str) -> List[LogEntry]:
        """Get entries to sync to a target node.

        Args:
            target_node: Target node ID

        Returns:
            List of entries to send
        """
        with self._lock:
            last_synced = self.sync_state.get(target_node, 0)
            current_index = self.wal.last_index()

            if last_synced >= current_index:
                return []

            return self.wal.get_range(last_synced + 1, current_index)

    def record_sync(self, target_node: str, up_to_index: int) -> None:
        """Record successful sync to a node.

        Args:
            target_node: Target node ID
            up_to_index: Last synced index
        """
        with self._lock:
            self.sync_state[target_node] = up_to_index

    def needs_full_sync(self, target_node: str) -> bool:
        """Check if node needs full sync.

        Args:
            target_node: Target node ID

        Returns:
            True if full sync needed
        """
        with self._lock:
            last_synced = self.sync_state.get(target_node, 0)
            oldest_available = 1  # After compaction, this would be higher

            return last_synced < oldest_available


# =============================================================================
# Health Monitor
# =============================================================================


class HealthMonitor:
    """Monitors cluster health and triggers failover."""

    def __init__(self, config: ClusterConfig):
        """Initialize health monitor.

        Args:
            config: Cluster configuration
        """
        self.config = config
        self.node_status: Dict[str, NodeState] = {}
        self.last_check: Dict[str, datetime] = {}
        self.failure_count: Dict[str, int] = {}
        self._lock = threading.RLock()

    def record_heartbeat(self, node_id: str) -> None:
        """Record heartbeat from node."""
        with self._lock:
            self.node_status[node_id] = NodeState.FOLLOWER
            self.last_check[node_id] = datetime.now()
            self.failure_count[node_id] = 0

    def record_failure(self, node_id: str) -> None:
        """Record failure from node."""
        with self._lock:
            self.failure_count[node_id] = self.failure_count.get(node_id, 0) + 1

            if self.failure_count[node_id] >= 3:
                self.node_status[node_id] = NodeState.OFFLINE

    def get_healthy_nodes(self) -> List[str]:
        """Get list of healthy node IDs."""
        with self._lock:
            timeout = timedelta(milliseconds=self.config.election_timeout_ms * 3)
            now = datetime.now()

            healthy = []
            for node_id, last_time in self.last_check.items():
                if now - last_time < timeout and self.node_status.get(node_id) != NodeState.OFFLINE:
                    healthy.append(node_id)

            return healthy

    def is_quorum_available(self) -> bool:
        """Check if quorum is available."""
        healthy_count = len(self.get_healthy_nodes())
        total_count = len(self.config.nodes)
        return healthy_count > total_count // 2


# =============================================================================
# Replication Manager
# =============================================================================


class ReplicationManager:
    """Coordinates replication activities."""

    def __init__(self, config: ClusterConfig, wal: WriteAheadLog):
        """Initialize replication manager.

        Args:
            config: Cluster configuration
            wal: Write-ahead log
        """
        self.config = config
        self.wal = wal
        self.sync_engine = SyncEngine(wal, config.cluster_id)
        self.health_monitor = HealthMonitor(config)
        self.stats = ReplicationStats()
        self._lock = threading.RLock()

    def replicate(self, command: str, data: bytes, consistency: ConsistencyLevel) -> Tuple[bool, int]:
        """Replicate a write operation.

        Args:
            command: Command type
            data: Command data
            consistency: Required consistency level

        Returns:
            (success, number of acks)
        """
        with self._lock:
            # Write to local WAL first
            entry = self.wal.append(0, command, data)  # Term managed by consensus
            self.stats.total_writes += 1
            self.stats.bytes_replicated += len(data)

            # Determine required acks
            total_nodes = len(self.config.nodes)
            if consistency == ConsistencyLevel.ONE:
                required_acks = 1
            elif consistency == ConsistencyLevel.QUORUM:
                required_acks = total_nodes // 2 + 1
            elif consistency == ConsistencyLevel.ALL:
                required_acks = total_nodes
            else:
                required_acks = 1

            # Simulate replication (in real implementation, this would be async)
            acks = 1  # Local ack
            healthy = self.health_monitor.get_healthy_nodes()

            for node_id in healthy:
                if node_id != self.config.cluster_id:
                    # Simulate network call
                    if random.random() > 0.1:  # 90% success rate
                        acks += 1
                        self.sync_engine.record_sync(node_id, entry.index)

            success = acks >= required_acks
            if success:
                self.stats.replicated_writes += 1
            else:
                self.stats.failed_writes += 1

            self.stats.last_sync_time = datetime.now()
            return success, acks

    def get_lag(self, node_id: str) -> int:
        """Get replication lag for a node.

        Args:
            node_id: Node ID

        Returns:
            Number of entries behind
        """
        with self._lock:
            current_index = self.wal.last_index()
            synced_index = self.sync_engine.sync_state.get(node_id, 0)
            return current_index - synced_index


# =============================================================================
# Main Replicator Agent
# =============================================================================


class Replicator:
    """Replicator Agent - Data replication and consensus.

    Core Responsibilities:
    1. Raft consensus for leader election
    2. Write-ahead log management
    3. Data synchronization
    4. Failover coordination

    Seed configuration is loaded from a YAML codex file.
    """

    # Core principle
    CORE_PRINCIPLE = "Consistency is the foundation of trust in distributed systems."
    MORAL_CONSTANT = "Never lose data, always maintain integrity."

    def __init__(self, state_root: Path = DEFAULT_STATE_ROOT, seed_path: Optional[Path] = None):
        """Initialize Replicator agent.

        Args:
            state_root: Root directory for replicator state
            seed_path: Path to seed configuration YAML
        """
        self.state_root = state_root
        self.state_root.mkdir(parents=True, exist_ok=True)

        # Load seed configuration
        self.seed = self._load_seed(seed_path)

        # Initialize WAL
        self.wal = WriteAheadLog(state_root / "wal", segment_size=self.seed.get("wal_segment_size", 64 * 1024 * 1024))

        # Initialize cluster config
        node_id = self.seed.get("node_id", "node-1")
        self.config = ClusterConfig(
            cluster_id=self.seed.get("cluster_id", "roaddb-cluster"),
            replication_factor=self.seed.get("replication_factor", 3),
            mode=ReplicationMode[self.seed.get("replication_mode", "SEMI_SYNC")],
            heartbeat_interval_ms=self.seed.get("heartbeat_interval_ms", 150),
            election_timeout_ms=self.seed.get("election_timeout_ms", 300),
        )

        # Initialize components
        self.consensus = RaftConsensus(node_id, self.config, self.wal)
        self.replication_manager = ReplicationManager(self.config, self.wal)

        # Set up callbacks
        self.consensus.on_state_change = self._on_state_change
        self.consensus.on_commit = self._on_commit

        logger.info(f"Replicator agent initialized at {state_root}")
        logger.info(f"Core principle: {self.CORE_PRINCIPLE}")

    def _load_seed(self, seed_path: Optional[Path]) -> Dict[str, Any]:
        """Load seed configuration from YAML."""
        default_seed = {
            "node_id": "node-1",
            "cluster_id": "roaddb-cluster",
            "replication_factor": 3,
            "replication_mode": "SEMI_SYNC",
            "heartbeat_interval_ms": 150,
            "election_timeout_ms": 300,
            "wal_segment_size": 64 * 1024 * 1024,
            "snapshot_interval": 10000,
        }

        if seed_path and seed_path.exists():
            with open(seed_path) as f:
                loaded = yaml.safe_load(f)
                default_seed.update(loaded)

        return default_seed

    def _on_state_change(self, state: NodeState) -> None:
        """Handle state change."""
        logger.info(f"Node state changed to {state.name}")

    def _on_commit(self, entry: LogEntry) -> None:
        """Handle committed entry."""
        logger.debug(f"Entry committed: {entry.index}")

    def start(self) -> None:
        """Start the replicator."""
        self.consensus.start()
        logger.info("Replicator started")

    def stop(self) -> None:
        """Stop the replicator."""
        self.consensus.stop()
        self.wal.close()
        logger.info("Replicator stopped")

    def write(self, command: str, data: bytes, consistency: Optional[ConsistencyLevel] = None) -> bool:
        """Write data with replication.

        Args:
            command: Command type
            data: Command data
            consistency: Consistency level (uses default if None)

        Returns:
            True if write succeeded
        """
        level = consistency or self.config.write_consistency
        success, acks = self.replication_manager.replicate(command, data, level)
        return success

    def get_stats(self) -> Dict[str, Any]:
        """Get replicator statistics."""
        return {
            "state": self.consensus.state.name,
            "term": self.consensus.current_term,
            "leader": self.consensus.leader_id,
            "commit_index": self.consensus.commit_index,
            "last_applied": self.consensus.last_applied,
            "wal_size": self.wal.last_index(),
            "replication_stats": {
                "total_writes": self.replication_manager.stats.total_writes,
                "replicated_writes": self.replication_manager.stats.replicated_writes,
                "failed_writes": self.replication_manager.stats.failed_writes,
                "bytes_replicated": self.replication_manager.stats.bytes_replicated,
            },
        }


# =============================================================================
# CLI Interface
# =============================================================================


def create_cli():
    """Create CLI for Replicator agent."""
    import argparse

    parser = argparse.ArgumentParser(description="RoadDB Replicator Agent - Consensus and replication")

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Status
    subparsers.add_parser("status", help="Show replication status")

    # WAL
    wal_parser = subparsers.add_parser("wal", help="WAL operations")
    wal_parser.add_argument("--last", type=int, help="Show last N entries")
    wal_parser.add_argument("--compact", type=int, help="Compact up to index")

    # Cluster
    subparsers.add_parser("cluster", help="Show cluster info")

    # Stats
    subparsers.add_parser("stats", help="Show replication statistics")

    return parser


def main():
    """Main entry point."""
    parser = create_cli()
    args = parser.parse_args()

    replicator = Replicator()

    if args.command == "status":
        stats = replicator.get_stats()
        print(f"State: {stats['state']}")
        print(f"Term: {stats['term']}")
        print(f"Leader: {stats['leader']}")
        print(f"Commit Index: {stats['commit_index']}")
        print(f"WAL Size: {stats['wal_size']} entries")

    elif args.command == "wal":
        if args.last:
            entries = replicator.wal.get_range(replicator.wal.last_index() - args.last + 1)
            for entry in entries:
                print(f"[{entry.index}] T{entry.term}: {entry.command}")
        elif args.compact:
            removed = replicator.wal.compact(args.compact)
            print(f"Compacted {removed} entries")

    elif args.command == "cluster":
        print(f"Cluster ID: {replicator.config.cluster_id}")
        print(f"Replication Factor: {replicator.config.replication_factor}")
        print(f"Mode: {replicator.config.mode.name}")
        print(f"Nodes: {len(replicator.config.nodes)}")

    elif args.command == "stats":
        stats = replicator.get_stats()
        rep_stats = stats["replication_stats"]
        print(f"Total Writes: {rep_stats['total_writes']}")
        print(f"Replicated: {rep_stats['replicated_writes']}")
        print(f"Failed: {rep_stats['failed_writes']}")
        print(f"Bytes Replicated: {rep_stats['bytes_replicated']:,}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()


__all__ = [
    "Replicator",
    "RaftConsensus",
    "WriteAheadLog",
    "ReplicationManager",
    "SyncEngine",
    "HealthMonitor",
    "NodeState",
    "ReplicationMode",
    "ConsistencyLevel",
    "LogEntry",
    "NodeInfo",
    "ClusterConfig",
    "ReplicationStats",
]
