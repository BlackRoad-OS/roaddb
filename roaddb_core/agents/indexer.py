"""RoadDB Indexer Agent - Query optimization and index management.

Like Lucidia Core's mathematician agent that reasons through abstract structures,
the Indexer agent manages the mathematical foundations of query performance:
B-trees, hash indexes, full-text search, and query plan optimization.

Architecture:
    ┌─────────────────────────────────────────────────────────────────────┐
    │                         Indexer Agent                              │
    ├─────────────────────────────────────────────────────────────────────┤
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
    │  │   Index     │  │   Query     │  │  Statistics │                 │
    │  │   Manager   │──│   Analyzer  │──│   Collector │                 │
    │  └─────────────┘  └─────────────┘  └─────────────┘                 │
    │         │               │               │                           │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
    │  │   B-Tree    │  │   Hash      │  │  Full-Text  │                 │
    │  │   Engine    │──│   Engine    │──│   Engine    │                 │
    │  └─────────────┘  └─────────────┘  └─────────────┘                 │
    └─────────────────────────────────────────────────────────────────────┘

Core Principle: "Every query deserves the fastest path to its answer."
Moral Constant: Optimize for both speed and resource efficiency.

Copyright (c) 2024-2026 BlackRoad OS, Inc. All rights reserved.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import struct
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import yaml

# Configure logging
logger = logging.getLogger(__name__)

# Type variables for generic index structures
K = TypeVar("K")  # Key type
V = TypeVar("V")  # Value type

# Default paths
DEFAULT_STATE_ROOT = Path.home() / ".roaddb" / "indexer"
DEFAULT_INDEX_DIR = "indexes"


class IndexType(Enum):
    """Types of indexes supported by RoadDB."""

    BTREE = auto()  # Balanced tree for range queries
    HASH = auto()  # Hash table for equality queries
    FULLTEXT = auto()  # Full-text search index
    GIN = auto()  # Generalized Inverted Index (arrays, JSONB)
    GIST = auto()  # Generalized Search Tree (geometric)
    BRIN = auto()  # Block Range Index (large tables)
    BLOOM = auto()  # Bloom filter for multi-column equality
    SPATIAL = auto()  # R-tree for spatial data


class IndexState(Enum):
    """Index operational states."""

    BUILDING = auto()
    READY = auto()
    STALE = auto()
    CORRUPTED = auto()
    REBUILDING = auto()
    DISABLED = auto()


class QueryType(Enum):
    """Types of query operations for optimization."""

    POINT_LOOKUP = auto()  # WHERE x = value
    RANGE_SCAN = auto()  # WHERE x BETWEEN a AND b
    PREFIX_SCAN = auto()  # WHERE x LIKE 'prefix%'
    FULL_SCAN = auto()  # No index possible
    INDEX_ONLY = auto()  # Covered by index
    BITMAP_SCAN = auto()  # Multiple index combination
    INDEX_JOIN = auto()  # Index nested loop join


@dataclass
class IndexStats:
    """Statistics for an index."""

    name: str
    table: str
    columns: List[str]
    index_type: IndexType
    size_bytes: int = 0
    num_entries: int = 0
    height: int = 0  # For tree indexes
    fill_factor: float = 0.9
    last_used: Optional[datetime] = None
    use_count: int = 0
    avg_lookup_time_ms: float = 0.0
    fragmentation_ratio: float = 0.0
    null_count: int = 0
    distinct_values: int = 0
    correlation: float = 0.0  # Physical vs logical order correlation


@dataclass
class QueryPlanNode:
    """Node in a query execution plan."""

    operation: str
    estimated_rows: int
    estimated_cost: float
    actual_rows: Optional[int] = None
    actual_time_ms: Optional[float] = None
    children: List[QueryPlanNode] = field(default_factory=list)
    index_used: Optional[str] = None
    filter_condition: Optional[str] = None
    output_columns: List[str] = field(default_factory=list)


@dataclass
class IndexRecommendation:
    """Recommendation for a new index."""

    table: str
    columns: List[str]
    index_type: IndexType
    reason: str
    estimated_improvement: float  # Percentage
    estimated_size_bytes: int
    priority: int  # 1-10, higher is more important
    query_patterns: List[str] = field(default_factory=list)


# =============================================================================
# B-Tree Index Implementation
# =============================================================================


@dataclass
class BTreeNode(Generic[K, V]):
    """Node in a B-tree."""

    keys: List[K] = field(default_factory=list)
    values: List[V] = field(default_factory=list)  # Only for leaf nodes
    children: List[BTreeNode] = field(default_factory=list)
    is_leaf: bool = True
    parent: Optional[BTreeNode] = None
    next_leaf: Optional[BTreeNode] = None  # For range scans
    prev_leaf: Optional[BTreeNode] = None


class BTreeIndex(Generic[K, V]):
    """B+ Tree implementation for range queries.

    Properties:
    - All values stored in leaf nodes
    - Leaf nodes linked for efficient range scans
    - Self-balancing with configurable order
    - Supports duplicate keys
    """

    def __init__(self, order: int = 100, unique: bool = False):
        """Initialize B-tree.

        Args:
            order: Maximum number of children per node (min = order/2)
            unique: Whether keys must be unique
        """
        self.order = order
        self.unique = unique
        self.root: BTreeNode[K, V] = BTreeNode()
        self.height = 1
        self.size = 0
        self._lock = threading.RLock()

    def insert(self, key: K, value: V) -> bool:
        """Insert a key-value pair.

        Args:
            key: The key to insert
            value: The value to associate with the key

        Returns:
            True if inserted, False if duplicate and unique=True
        """
        with self._lock:
            # Find the leaf node
            leaf = self._find_leaf(key)

            # Check for duplicates if unique
            if self.unique:
                for i, k in enumerate(leaf.keys):
                    if k == key:
                        return False

            # Find insertion position
            pos = self._find_position(leaf.keys, key)

            # Insert into leaf
            leaf.keys.insert(pos, key)
            leaf.values.insert(pos, value)
            self.size += 1

            # Split if necessary
            if len(leaf.keys) >= self.order:
                self._split_leaf(leaf)

            return True

    def search(self, key: K) -> Optional[V]:
        """Search for a key.

        Args:
            key: The key to search for

        Returns:
            The value if found, None otherwise
        """
        with self._lock:
            leaf = self._find_leaf(key)
            for i, k in enumerate(leaf.keys):
                if k == key:
                    return leaf.values[i]
            return None

    def range_search(self, start: K, end: K, inclusive: Tuple[bool, bool] = (True, True)) -> Iterator[Tuple[K, V]]:
        """Search for keys in a range.

        Args:
            start: Start of range
            end: End of range
            inclusive: (start_inclusive, end_inclusive)

        Yields:
            (key, value) pairs in range
        """
        with self._lock:
            leaf = self._find_leaf(start)

            # Find starting position
            pos = 0
            for i, k in enumerate(leaf.keys):
                if inclusive[0]:
                    if k >= start:
                        pos = i
                        break
                else:
                    if k > start:
                        pos = i
                        break
            else:
                pos = len(leaf.keys)

            # Iterate through leaves
            while leaf is not None:
                while pos < len(leaf.keys):
                    key = leaf.keys[pos]

                    # Check end condition
                    if inclusive[1]:
                        if key > end:
                            return
                    else:
                        if key >= end:
                            return

                    yield (key, leaf.values[pos])
                    pos += 1

                # Move to next leaf
                leaf = leaf.next_leaf
                pos = 0

    def delete(self, key: K) -> bool:
        """Delete a key.

        Args:
            key: The key to delete

        Returns:
            True if deleted, False if not found
        """
        with self._lock:
            leaf = self._find_leaf(key)

            # Find and remove
            for i, k in enumerate(leaf.keys):
                if k == key:
                    leaf.keys.pop(i)
                    leaf.values.pop(i)
                    self.size -= 1

                    # Rebalance if necessary
                    min_keys = (self.order - 1) // 2
                    if len(leaf.keys) < min_keys and leaf != self.root:
                        self._rebalance(leaf)

                    return True

            return False

    def _find_leaf(self, key: K) -> BTreeNode[K, V]:
        """Find the leaf node that would contain a key."""
        node = self.root
        while not node.is_leaf:
            pos = self._find_position(node.keys, key)
            node = node.children[pos]
        return node

    def _find_position(self, keys: List[K], key: K) -> int:
        """Find the position where a key should be inserted."""
        for i, k in enumerate(keys):
            if key < k:
                return i
        return len(keys)

    def _split_leaf(self, leaf: BTreeNode[K, V]) -> None:
        """Split a leaf node that's too full."""
        mid = len(leaf.keys) // 2

        # Create new leaf
        new_leaf = BTreeNode[K, V](
            keys=leaf.keys[mid:],
            values=leaf.values[mid:],
            is_leaf=True,
            next_leaf=leaf.next_leaf,
            prev_leaf=leaf,
        )

        # Update old leaf
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]
        leaf.next_leaf = new_leaf

        if new_leaf.next_leaf:
            new_leaf.next_leaf.prev_leaf = new_leaf

        # Promote middle key to parent
        self._insert_into_parent(leaf, new_leaf.keys[0], new_leaf)

    def _insert_into_parent(self, left: BTreeNode[K, V], key: K, right: BTreeNode[K, V]) -> None:
        """Insert a key and child into parent node."""
        if left.parent is None:
            # Create new root
            new_root = BTreeNode[K, V](keys=[key], children=[left, right], is_leaf=False)
            left.parent = new_root
            right.parent = new_root
            self.root = new_root
            self.height += 1
            return

        parent = left.parent
        pos = parent.children.index(left) + 1

        parent.keys.insert(pos - 1, key)
        parent.children.insert(pos, right)
        right.parent = parent

        # Split parent if necessary
        if len(parent.keys) >= self.order:
            self._split_internal(parent)

    def _split_internal(self, node: BTreeNode[K, V]) -> None:
        """Split an internal node that's too full."""
        mid = len(node.keys) // 2
        promote_key = node.keys[mid]

        # Create new internal node
        new_node = BTreeNode[K, V](keys=node.keys[mid + 1 :], children=node.children[mid + 1 :], is_leaf=False)

        # Update children's parent
        for child in new_node.children:
            child.parent = new_node

        # Update old node
        node.keys = node.keys[:mid]
        node.children = node.children[: mid + 1]

        # Promote middle key
        self._insert_into_parent(node, promote_key, new_node)

    def _rebalance(self, node: BTreeNode[K, V]) -> None:
        """Rebalance a node after deletion."""
        # Simplified rebalancing - production would have full merge/redistribute logic
        pass

    def get_stats(self) -> Dict[str, Any]:
        """Get index statistics."""

        def count_nodes(node: BTreeNode) -> Tuple[int, int]:
            if node.is_leaf:
                return 1, 0
            leaf_count, internal_count = 0, 1
            for child in node.children:
                l, i = count_nodes(child)
                leaf_count += l
                internal_count += i
            return leaf_count, internal_count

        leaf_count, internal_count = count_nodes(self.root)

        return {
            "type": "btree",
            "order": self.order,
            "unique": self.unique,
            "height": self.height,
            "size": self.size,
            "leaf_nodes": leaf_count,
            "internal_nodes": internal_count,
            "total_nodes": leaf_count + internal_count,
        }


# =============================================================================
# Hash Index Implementation
# =============================================================================


class HashIndex(Generic[K, V]):
    """Hash table implementation for equality queries.

    Uses extendible hashing for dynamic growth without full rehashing.
    """

    def __init__(self, initial_depth: int = 4, bucket_size: int = 100):
        """Initialize hash index.

        Args:
            initial_depth: Initial global depth (2^depth buckets)
            bucket_size: Maximum entries per bucket before split
        """
        self.global_depth = initial_depth
        self.bucket_size = bucket_size
        self.directory: List[HashBucket] = []
        self.size = 0
        self._lock = threading.RLock()

        # Initialize directory
        num_buckets = 2**initial_depth
        for i in range(num_buckets):
            bucket = HashBucket(local_depth=initial_depth)
            self.directory.append(bucket)

    def _hash(self, key: K) -> int:
        """Compute hash of a key."""
        if isinstance(key, (int, float)):
            key_bytes = struct.pack("d", float(key))
        else:
            key_bytes = str(key).encode("utf-8")
        return int(hashlib.md5(key_bytes).hexdigest(), 16)

    def _get_bucket_index(self, key: K) -> int:
        """Get directory index for a key."""
        h = self._hash(key)
        return h & ((1 << self.global_depth) - 1)

    def insert(self, key: K, value: V) -> bool:
        """Insert a key-value pair."""
        with self._lock:
            idx = self._get_bucket_index(key)
            bucket = self.directory[idx]

            # Check for duplicate
            for i, (k, v) in enumerate(bucket.entries):
                if k == key:
                    bucket.entries[i] = (key, value)  # Update
                    return True

            # Insert
            bucket.entries.append((key, value))
            self.size += 1

            # Split if necessary
            if len(bucket.entries) > self.bucket_size:
                self._split_bucket(idx)

            return True

    def search(self, key: K) -> Optional[V]:
        """Search for a key."""
        with self._lock:
            idx = self._get_bucket_index(key)
            bucket = self.directory[idx]

            for k, v in bucket.entries:
                if k == key:
                    return v
            return None

    def delete(self, key: K) -> bool:
        """Delete a key."""
        with self._lock:
            idx = self._get_bucket_index(key)
            bucket = self.directory[idx]

            for i, (k, v) in enumerate(bucket.entries):
                if k == key:
                    bucket.entries.pop(i)
                    self.size -= 1
                    return True
            return False

    def _split_bucket(self, idx: int) -> None:
        """Split a bucket that's too full."""
        bucket = self.directory[idx]

        if bucket.local_depth == self.global_depth:
            # Double the directory
            self.directory = self.directory + self.directory.copy()
            self.global_depth += 1

        # Increase local depth
        bucket.local_depth += 1

        # Create new bucket
        new_bucket = HashBucket(local_depth=bucket.local_depth)

        # Redistribute entries
        mask = 1 << (bucket.local_depth - 1)
        new_entries = []
        keep_entries = []

        for key, value in bucket.entries:
            h = self._hash(key)
            if h & mask:
                new_entries.append((key, value))
            else:
                keep_entries.append((key, value))

        bucket.entries = keep_entries
        new_bucket.entries = new_entries

        # Update directory pointers
        for i in range(len(self.directory)):
            if i & ((1 << bucket.local_depth) - 1) == idx | mask:
                self.directory[i] = new_bucket

    def get_stats(self) -> Dict[str, Any]:
        """Get index statistics."""
        unique_buckets = len(set(id(b) for b in self.directory))
        total_entries = sum(len(b.entries) for b in set(self.directory))

        return {
            "type": "hash",
            "global_depth": self.global_depth,
            "directory_size": len(self.directory),
            "unique_buckets": unique_buckets,
            "bucket_size": self.bucket_size,
            "total_entries": total_entries,
            "load_factor": total_entries / (unique_buckets * self.bucket_size) if unique_buckets > 0 else 0,
        }


@dataclass
class HashBucket(Generic[K, V]):
    """Bucket in hash index."""

    local_depth: int
    entries: List[Tuple[K, V]] = field(default_factory=list)


# =============================================================================
# Full-Text Search Index
# =============================================================================


class FullTextIndex:
    """Inverted index for full-text search.

    Supports:
    - Tokenization with multiple analyzers
    - TF-IDF ranking
    - Phrase queries
    - Fuzzy matching
    """

    def __init__(self, analyzer: str = "standard"):
        """Initialize full-text index.

        Args:
            analyzer: Text analyzer to use (standard, simple, whitespace)
        """
        self.analyzer = analyzer
        self.inverted_index: Dict[str, Dict[int, List[int]]] = {}  # term -> {doc_id -> [positions]}
        self.documents: Dict[int, str] = {}  # doc_id -> original text
        self.doc_lengths: Dict[int, int] = {}  # doc_id -> length
        self.total_docs = 0
        self.avg_doc_length = 0.0
        self._lock = threading.RLock()

    def _tokenize(self, text: str) -> List[Tuple[str, int]]:
        """Tokenize text and return (token, position) pairs."""
        tokens = []
        pos = 0

        if self.analyzer == "whitespace":
            for word in text.split():
                tokens.append((word.lower(), pos))
                pos += 1
        elif self.analyzer == "simple":
            word = ""
            for char in text:
                if char.isalnum():
                    word += char.lower()
                else:
                    if word:
                        tokens.append((word, pos))
                        pos += 1
                        word = ""
            if word:
                tokens.append((word, pos))
        else:  # standard
            import re

            for match in re.finditer(r"\b\w+\b", text.lower()):
                tokens.append((match.group(), pos))
                pos += 1

        return tokens

    def index_document(self, doc_id: int, text: str) -> None:
        """Index a document."""
        with self._lock:
            tokens = self._tokenize(text)

            # Store document
            self.documents[doc_id] = text
            self.doc_lengths[doc_id] = len(tokens)

            # Update inverted index
            for token, position in tokens:
                if token not in self.inverted_index:
                    self.inverted_index[token] = {}
                if doc_id not in self.inverted_index[token]:
                    self.inverted_index[token][doc_id] = []
                self.inverted_index[token][doc_id].append(position)

            # Update statistics
            self.total_docs = len(self.documents)
            self.avg_doc_length = sum(self.doc_lengths.values()) / self.total_docs if self.total_docs > 0 else 0

    def search(self, query: str, limit: int = 10) -> List[Tuple[int, float]]:
        """Search for documents matching query.

        Args:
            query: Search query
            limit: Maximum results

        Returns:
            List of (doc_id, score) pairs sorted by relevance
        """
        with self._lock:
            query_tokens = [t for t, _ in self._tokenize(query)]

            if not query_tokens:
                return []

            # Calculate TF-IDF scores
            scores: Dict[int, float] = {}

            for token in query_tokens:
                if token not in self.inverted_index:
                    continue

                postings = self.inverted_index[token]
                idf = math.log((self.total_docs + 1) / (len(postings) + 1)) + 1

                for doc_id, positions in postings.items():
                    tf = len(positions)
                    doc_len = self.doc_lengths[doc_id]

                    # BM25-like scoring
                    k1 = 1.2
                    b = 0.75
                    normalized_tf = (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * doc_len / self.avg_doc_length))

                    score = normalized_tf * idf
                    scores[doc_id] = scores.get(doc_id, 0) + score

            # Sort by score
            results = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            return results[:limit]

    def phrase_search(self, phrase: str) -> List[int]:
        """Search for exact phrase matches.

        Args:
            phrase: Phrase to search for

        Returns:
            List of matching document IDs
        """
        with self._lock:
            tokens = [t for t, _ in self._tokenize(phrase)]

            if not tokens:
                return []

            # Get documents containing all tokens
            if tokens[0] not in self.inverted_index:
                return []

            candidate_docs = set(self.inverted_index[tokens[0]].keys())

            for token in tokens[1:]:
                if token not in self.inverted_index:
                    return []
                candidate_docs &= set(self.inverted_index[token].keys())

            # Check for phrase matches
            matches = []
            for doc_id in candidate_docs:
                positions_list = [self.inverted_index[t][doc_id] for t in tokens]

                # Check if tokens appear consecutively
                for start_pos in positions_list[0]:
                    is_phrase = True
                    for i, positions in enumerate(positions_list[1:], 1):
                        if start_pos + i not in positions:
                            is_phrase = False
                            break
                    if is_phrase:
                        matches.append(doc_id)
                        break

            return matches

    def get_stats(self) -> Dict[str, Any]:
        """Get index statistics."""
        return {
            "type": "fulltext",
            "analyzer": self.analyzer,
            "total_documents": self.total_docs,
            "total_terms": len(self.inverted_index),
            "avg_doc_length": self.avg_doc_length,
            "total_postings": sum(sum(len(p) for p in d.values()) for d in self.inverted_index.values()),
        }


# =============================================================================
# Query Analyzer
# =============================================================================


class QueryAnalyzer:
    """Analyzes queries and recommends optimal execution strategies."""

    def __init__(self, indexes: Dict[str, Any], table_stats: Dict[str, Dict[str, Any]]):
        """Initialize query analyzer.

        Args:
            indexes: Available indexes
            table_stats: Table statistics (row counts, column stats)
        """
        self.indexes = indexes
        self.table_stats = table_stats

    def analyze(self, query: str) -> QueryPlanNode:
        """Analyze a query and generate execution plan.

        Args:
            query: SQL query string

        Returns:
            Root node of query plan tree
        """
        # Simplified query analysis - production would parse full SQL
        query_lower = query.lower()

        # Detect query type
        if "where" not in query_lower:
            return QueryPlanNode(
                operation="Seq Scan", estimated_rows=1000, estimated_cost=1000.0, filter_condition=None
            )

        # Extract table and conditions
        # This is a simplified parser - real implementation would use proper SQL parsing
        where_clause = query_lower.split("where")[1].strip()

        # Check for index usage
        for idx_name, idx in self.indexes.items():
            if hasattr(idx, "columns"):
                for col in idx.columns:
                    if col in where_clause:
                        if "=" in where_clause and ">" not in where_clause and "<" not in where_clause:
                            return QueryPlanNode(
                                operation="Index Scan",
                                estimated_rows=1,
                                estimated_cost=1.0,
                                index_used=idx_name,
                                filter_condition=where_clause,
                            )
                        elif "between" in where_clause or ">" in where_clause or "<" in where_clause:
                            return QueryPlanNode(
                                operation="Index Range Scan",
                                estimated_rows=100,
                                estimated_cost=10.0,
                                index_used=idx_name,
                                filter_condition=where_clause,
                            )

        return QueryPlanNode(
            operation="Seq Scan", estimated_rows=1000, estimated_cost=1000.0, filter_condition=where_clause
        )

    def recommend_indexes(self, queries: List[str], existing_indexes: Dict[str, Any]) -> List[IndexRecommendation]:
        """Analyze query workload and recommend indexes.

        Args:
            queries: List of representative queries
            existing_indexes: Currently existing indexes

        Returns:
            List of index recommendations
        """
        recommendations = []
        column_access_patterns: Dict[str, Dict[str, int]] = {}  # table -> {column -> access_count}

        for query in queries:
            query_lower = query.lower()

            # Extract table names (simplified)
            if "from " in query_lower:
                table = query_lower.split("from ")[1].split()[0]
            else:
                continue

            if table not in column_access_patterns:
                column_access_patterns[table] = {}

            # Extract WHERE columns
            if "where " in query_lower:
                where_part = query_lower.split("where ")[1]
                # Find column references
                words = where_part.replace("=", " ").replace("<", " ").replace(">", " ").split()
                for word in words:
                    if word.isidentifier() and word not in ("and", "or", "not", "in", "like", "between"):
                        column_access_patterns[table][word] = column_access_patterns[table].get(word, 0) + 1

        # Generate recommendations
        for table, columns in column_access_patterns.items():
            for column, count in columns.items():
                if count >= 3:  # Threshold for recommendation
                    recommendations.append(
                        IndexRecommendation(
                            table=table,
                            columns=[column],
                            index_type=IndexType.BTREE,
                            reason=f"Column '{column}' accessed {count} times in WHERE clauses",
                            estimated_improvement=min(count * 10, 90),
                            estimated_size_bytes=10000,
                            priority=min(count, 10),
                            query_patterns=[q for q in queries if column in q.lower()],
                        )
                    )

        return sorted(recommendations, key=lambda r: r.priority, reverse=True)


# =============================================================================
# Index Manager
# =============================================================================


class IndexManager:
    """Manages all indexes for a database."""

    def __init__(self, state_root: Path = DEFAULT_STATE_ROOT):
        """Initialize index manager.

        Args:
            state_root: Root directory for index state
        """
        self.state_root = state_root
        self.index_dir = state_root / DEFAULT_INDEX_DIR
        self.indexes: Dict[str, Any] = {}
        self.index_stats: Dict[str, IndexStats] = {}
        self._lock = threading.RLock()

        # Ensure directories exist
        self.index_dir.mkdir(parents=True, exist_ok=True)

    def create_index(
        self,
        name: str,
        table: str,
        columns: List[str],
        index_type: IndexType = IndexType.BTREE,
        unique: bool = False,
        **options,
    ) -> bool:
        """Create a new index.

        Args:
            name: Index name
            table: Table to index
            columns: Columns to include
            index_type: Type of index
            unique: Whether index enforces uniqueness
            **options: Additional index options

        Returns:
            True if created successfully
        """
        with self._lock:
            if name in self.indexes:
                logger.warning(f"Index {name} already exists")
                return False

            # Create appropriate index type
            if index_type == IndexType.BTREE:
                order = options.get("order", 100)
                index = BTreeIndex(order=order, unique=unique)
            elif index_type == IndexType.HASH:
                index = HashIndex()
            elif index_type == IndexType.FULLTEXT:
                analyzer = options.get("analyzer", "standard")
                index = FullTextIndex(analyzer=analyzer)
            else:
                logger.error(f"Unsupported index type: {index_type}")
                return False

            self.indexes[name] = index
            self.index_stats[name] = IndexStats(
                name=name,
                table=table,
                columns=columns,
                index_type=index_type,
            )

            logger.info(f"Created {index_type.name} index '{name}' on {table}({', '.join(columns)})")
            return True

    def drop_index(self, name: str) -> bool:
        """Drop an index.

        Args:
            name: Index name

        Returns:
            True if dropped successfully
        """
        with self._lock:
            if name not in self.indexes:
                logger.warning(f"Index {name} does not exist")
                return False

            del self.indexes[name]
            del self.index_stats[name]

            logger.info(f"Dropped index '{name}'")
            return True

    def get_index(self, name: str) -> Optional[Any]:
        """Get an index by name."""
        return self.indexes.get(name)

    def list_indexes(self, table: Optional[str] = None) -> List[IndexStats]:
        """List all indexes, optionally filtered by table."""
        stats = list(self.index_stats.values())
        if table:
            stats = [s for s in stats if s.table == table]
        return stats


# =============================================================================
# Statistics Collector
# =============================================================================


class StatisticsCollector:
    """Collects and maintains table/column statistics for query optimization."""

    def __init__(self, state_root: Path = DEFAULT_STATE_ROOT):
        """Initialize statistics collector."""
        self.state_root = state_root
        self.stats_file = state_root / "statistics.yaml"
        self.table_stats: Dict[str, Dict[str, Any]] = {}
        self.column_stats: Dict[str, Dict[str, Dict[str, Any]]] = {}  # table -> column -> stats
        self.last_analyze: Dict[str, datetime] = {}
        self._lock = threading.RLock()

    def analyze_table(self, table: str, sample_rate: float = 0.1) -> Dict[str, Any]:
        """Analyze a table and collect statistics.

        Args:
            table: Table name
            sample_rate: Fraction of rows to sample

        Returns:
            Collected statistics
        """
        # This would connect to actual storage in production
        # Simulated statistics for demonstration
        stats = {
            "row_count": 1000000,
            "page_count": 10000,
            "avg_row_size": 100,
            "last_analyzed": datetime.now().isoformat(),
        }

        with self._lock:
            self.table_stats[table] = stats
            self.last_analyze[table] = datetime.now()

        return stats

    def analyze_column(self, table: str, column: str, sample_rate: float = 0.1) -> Dict[str, Any]:
        """Analyze a column and collect statistics.

        Args:
            table: Table name
            column: Column name
            sample_rate: Fraction of rows to sample

        Returns:
            Collected statistics
        """
        # Simulated statistics
        stats = {
            "distinct_values": 50000,
            "null_ratio": 0.01,
            "avg_length": 20,
            "min_value": 1,
            "max_value": 1000000,
            "most_common_values": [(1, 0.1), (2, 0.05), (3, 0.03)],
            "histogram": list(range(0, 101, 10)),  # Decile boundaries
        }

        with self._lock:
            if table not in self.column_stats:
                self.column_stats[table] = {}
            self.column_stats[table][column] = stats

        return stats

    def estimate_selectivity(self, table: str, column: str, operator: str, value: Any) -> float:
        """Estimate selectivity of a predicate.

        Args:
            table: Table name
            column: Column name
            operator: Comparison operator (=, <, >, <=, >=, LIKE)
            value: Comparison value

        Returns:
            Estimated selectivity (0.0 to 1.0)
        """
        col_stats = self.column_stats.get(table, {}).get(column)
        if not col_stats:
            return 0.1  # Default selectivity

        distinct = col_stats.get("distinct_values", 1000)

        if operator == "=":
            # Point query selectivity
            return 1.0 / distinct
        elif operator in ("<", ">", "<=", ">="):
            # Range query - use histogram if available
            return 0.3  # Default
        elif operator == "LIKE":
            if isinstance(value, str) and not value.startswith("%"):
                # Prefix search - more selective
                return 0.01
            else:
                # Full wildcard - less selective
                return 0.5

        return 0.1


# =============================================================================
# Main Indexer Agent
# =============================================================================


class Indexer:
    """Indexer Agent - Query optimization and index management.

    Core Responsibilities:
    1. Index lifecycle management (create, drop, rebuild)
    2. Query analysis and optimization
    3. Statistics collection and maintenance
    4. Index recommendation based on workload

    Seed configuration is loaded from a YAML codex file that defines:
    - Default index types and configurations
    - Statistics refresh intervals
    - Auto-indexing policies
    - Performance thresholds
    """

    # Core principle
    CORE_PRINCIPLE = "Every query deserves the fastest path to its answer."
    MORAL_CONSTANT = "Optimize for both speed and resource efficiency."

    def __init__(self, state_root: Path = DEFAULT_STATE_ROOT, seed_path: Optional[Path] = None):
        """Initialize Indexer agent.

        Args:
            state_root: Root directory for index state
            seed_path: Path to seed configuration YAML
        """
        self.state_root = state_root
        self.state_root.mkdir(parents=True, exist_ok=True)

        # Load seed configuration
        self.seed = self._load_seed(seed_path)

        # Initialize components
        self.index_manager = IndexManager(state_root)
        self.stats_collector = StatisticsCollector(state_root)
        self.query_analyzer: Optional[QueryAnalyzer] = None

        # Performance tracking
        self.query_log: List[Dict[str, Any]] = []
        self.optimization_history: List[Dict[str, Any]] = []

        # Background maintenance
        self._running = False
        self._maintenance_thread: Optional[threading.Thread] = None

        logger.info(f"Indexer agent initialized at {state_root}")
        logger.info(f"Core principle: {self.CORE_PRINCIPLE}")

    def _load_seed(self, seed_path: Optional[Path]) -> Dict[str, Any]:
        """Load seed configuration from YAML."""
        default_seed = {
            "default_index_type": "btree",
            "btree_order": 100,
            "hash_bucket_size": 100,
            "fulltext_analyzer": "standard",
            "auto_analyze_threshold": 1000,  # Rows changed before auto-analyze
            "stats_refresh_interval_hours": 24,
            "auto_index_threshold": 10,  # Query count before recommending index
            "max_indexes_per_table": 10,
            "index_size_warning_mb": 1000,
        }

        if seed_path and seed_path.exists():
            with open(seed_path) as f:
                loaded = yaml.safe_load(f)
                default_seed.update(loaded)

        return default_seed

    def create_index(
        self,
        name: str,
        table: str,
        columns: List[str],
        index_type: str = "btree",
        unique: bool = False,
        **options,
    ) -> bool:
        """Create a new index.

        Args:
            name: Index name
            table: Table to index
            columns: Columns to include in index
            index_type: Type (btree, hash, fulltext)
            unique: Whether to enforce uniqueness
            **options: Additional options

        Returns:
            True if successful
        """
        type_map = {
            "btree": IndexType.BTREE,
            "hash": IndexType.HASH,
            "fulltext": IndexType.FULLTEXT,
            "gin": IndexType.GIN,
            "gist": IndexType.GIST,
            "brin": IndexType.BRIN,
        }

        idx_type = type_map.get(index_type.lower(), IndexType.BTREE)
        return self.index_manager.create_index(name, table, columns, idx_type, unique, **options)

    def drop_index(self, name: str) -> bool:
        """Drop an index."""
        return self.index_manager.drop_index(name)

    def analyze_query(self, query: str) -> QueryPlanNode:
        """Analyze a query and return execution plan.

        Args:
            query: SQL query string

        Returns:
            Query execution plan
        """
        if self.query_analyzer is None:
            self.query_analyzer = QueryAnalyzer(self.index_manager.indexes, self.stats_collector.table_stats)

        plan = self.query_analyzer.analyze(query)

        # Log query for workload analysis
        self.query_log.append(
            {
                "query": query,
                "timestamp": datetime.now().isoformat(),
                "plan": plan.operation,
                "cost": plan.estimated_cost,
            }
        )

        return plan

    def recommend_indexes(self) -> List[IndexRecommendation]:
        """Analyze query workload and recommend indexes.

        Returns:
            List of index recommendations
        """
        if not self.query_log:
            return []

        queries = [entry["query"] for entry in self.query_log]

        if self.query_analyzer is None:
            self.query_analyzer = QueryAnalyzer(self.index_manager.indexes, self.stats_collector.table_stats)

        return self.query_analyzer.recommend_indexes(queries, self.index_manager.indexes)

    def analyze_table(self, table: str) -> Dict[str, Any]:
        """Collect statistics for a table."""
        return self.stats_collector.analyze_table(table)

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive indexer statistics."""
        index_stats = {}
        for name, idx in self.index_manager.indexes.items():
            if hasattr(idx, "get_stats"):
                index_stats[name] = idx.get_stats()

        return {
            "total_indexes": len(self.index_manager.indexes),
            "index_stats": index_stats,
            "query_log_size": len(self.query_log),
            "tables_analyzed": len(self.stats_collector.table_stats),
        }

    def start_maintenance(self, interval_seconds: int = 3600) -> None:
        """Start background maintenance thread."""
        if self._running:
            return

        self._running = True

        def maintenance_loop():
            while self._running:
                self._perform_maintenance()
                time.sleep(interval_seconds)

        self._maintenance_thread = threading.Thread(target=maintenance_loop, daemon=True)
        self._maintenance_thread.start()
        logger.info("Started index maintenance thread")

    def stop_maintenance(self) -> None:
        """Stop background maintenance."""
        self._running = False
        if self._maintenance_thread:
            self._maintenance_thread.join(timeout=5)
        logger.info("Stopped index maintenance thread")

    def _perform_maintenance(self) -> None:
        """Perform periodic maintenance tasks."""
        # Check for stale statistics
        now = datetime.now()
        refresh_interval = timedelta(hours=self.seed.get("stats_refresh_interval_hours", 24))

        for table, last_time in self.stats_collector.last_analyze.items():
            if now - last_time > refresh_interval:
                logger.info(f"Refreshing statistics for table {table}")
                self.stats_collector.analyze_table(table)

        # Check for index recommendations
        recommendations = self.recommend_indexes()
        for rec in recommendations:
            if rec.priority >= 8:
                logger.warning(f"High-priority index recommendation: {rec.table}({', '.join(rec.columns)})")


# =============================================================================
# CLI Interface
# =============================================================================


def create_cli():
    """Create CLI for Indexer agent."""
    import argparse

    parser = argparse.ArgumentParser(description="RoadDB Indexer Agent - Query optimization and index management")

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Create index
    create_parser = subparsers.add_parser("create", help="Create an index")
    create_parser.add_argument("name", help="Index name")
    create_parser.add_argument("table", help="Table name")
    create_parser.add_argument("columns", nargs="+", help="Column names")
    create_parser.add_argument("--type", default="btree", choices=["btree", "hash", "fulltext"])
    create_parser.add_argument("--unique", action="store_true")

    # Drop index
    drop_parser = subparsers.add_parser("drop", help="Drop an index")
    drop_parser.add_argument("name", help="Index name")

    # List indexes
    list_parser = subparsers.add_parser("list", help="List indexes")
    list_parser.add_argument("--table", help="Filter by table")

    # Analyze query
    analyze_parser = subparsers.add_parser("analyze", help="Analyze a query")
    analyze_parser.add_argument("query", help="SQL query")

    # Recommend indexes
    subparsers.add_parser("recommend", help="Get index recommendations")

    # Stats
    subparsers.add_parser("stats", help="Show indexer statistics")

    return parser


def main():
    """Main entry point for CLI."""
    parser = create_cli()
    args = parser.parse_args()

    indexer = Indexer()

    if args.command == "create":
        success = indexer.create_index(
            args.name, args.table, args.columns, index_type=args.type, unique=args.unique
        )
        print(f"Index created: {success}")

    elif args.command == "drop":
        success = indexer.drop_index(args.name)
        print(f"Index dropped: {success}")

    elif args.command == "list":
        indexes = indexer.index_manager.list_indexes(args.table)
        for idx in indexes:
            print(f"  {idx.name}: {idx.index_type.name} on {idx.table}({', '.join(idx.columns)})")

    elif args.command == "analyze":
        plan = indexer.analyze_query(args.query)
        print(f"Plan: {plan.operation}")
        print(f"Estimated cost: {plan.estimated_cost}")
        if plan.index_used:
            print(f"Index used: {plan.index_used}")

    elif args.command == "recommend":
        recs = indexer.recommend_indexes()
        for rec in recs:
            print(f"  [{rec.priority}] {rec.table}({', '.join(rec.columns)})")
            print(f"      Type: {rec.index_type.name}")
            print(f"      Reason: {rec.reason}")

    elif args.command == "stats":
        stats = indexer.get_stats()
        print(f"Total indexes: {stats['total_indexes']}")
        print(f"Query log size: {stats['query_log_size']}")
        print(f"Tables analyzed: {stats['tables_analyzed']}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()


__all__ = [
    "Indexer",
    "IndexManager",
    "IndexType",
    "IndexState",
    "IndexStats",
    "IndexRecommendation",
    "BTreeIndex",
    "HashIndex",
    "FullTextIndex",
    "QueryAnalyzer",
    "QueryPlanNode",
    "StatisticsCollector",
]
