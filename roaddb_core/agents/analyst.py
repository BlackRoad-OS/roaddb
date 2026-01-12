"""RoadDB Analyst Agent - Pattern detection and anomaly analysis.

Like Lucidia Core's physicist agent that reasons about physical phenomena,
the Analyst agent reasons about data patterns, detects anomalies, and
provides intelligent insights for query optimization and data quality.

Architecture:
    ┌─────────────────────────────────────────────────────────────────────┐
    │                         Analyst Agent                              │
    ├─────────────────────────────────────────────────────────────────────┤
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
    │  │   Pattern   │  │   Anomaly   │  │   Query     │                 │
    │  │   Detector  │──│   Detector  │──│   Profiler  │                 │
    │  └─────────────┘  └─────────────┘  └─────────────┘                 │
    │         │               │               │                           │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
    │  │ Statistical │  │   ML        │  │   Report    │                 │
    │  │   Engine    │──│   Models    │──│   Generator │                 │
    │  └─────────────┘  └─────────────┘  └─────────────┘                 │
    └─────────────────────────────────────────────────────────────────────┘

Core Principle: "Data speaks - learn to listen and interpret."
Moral Constant: Provide insights that are actionable and accurate.

Copyright (c) 2024-2026 BlackRoad OS, Inc. All rights reserved.
"""

from __future__ import annotations

import json
import logging
import math
import statistics
import threading
import time
from abc import ABC, abstractmethod
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Dict, Generic, Iterator, List, Optional, Set, Tuple, TypeVar, Union

import yaml

# Configure logging
logger = logging.getLogger(__name__)

# Default paths
DEFAULT_STATE_ROOT = Path.home() / ".roaddb" / "analyst"


class AnomalyType(Enum):
    """Types of anomalies detected."""

    STATISTICAL_OUTLIER = auto()
    PATTERN_DEVIATION = auto()
    DATA_QUALITY = auto()
    PERFORMANCE = auto()
    SCHEMA_VIOLATION = auto()
    CARDINALITY_CHANGE = auto()
    DISTRIBUTION_SHIFT = auto()
    TEMPORAL_ANOMALY = auto()


class PatternType(Enum):
    """Types of patterns detected."""

    TREND = auto()
    SEASONALITY = auto()
    CYCLICAL = auto()
    CORRELATION = auto()
    CLUSTERING = auto()
    SEQUENCE = auto()
    CATEGORICAL = auto()


class AlertSeverity(Enum):
    """Alert severity levels."""

    INFO = auto()
    WARNING = auto()
    CRITICAL = auto()
    EMERGENCY = auto()


@dataclass
class DataProfile:
    """Statistical profile of a data column."""

    column_name: str
    data_type: str
    total_count: int = 0
    null_count: int = 0
    distinct_count: int = 0
    min_value: Any = None
    max_value: Any = None
    mean: Optional[float] = None
    median: Optional[float] = None
    std_dev: Optional[float] = None
    variance: Optional[float] = None
    skewness: Optional[float] = None
    kurtosis: Optional[float] = None
    percentiles: Dict[int, float] = field(default_factory=dict)
    most_common: List[Tuple[Any, int]] = field(default_factory=list)
    histogram: List[Tuple[float, float, int]] = field(default_factory=list)  # (min, max, count)
    pattern_stats: Dict[str, Any] = field(default_factory=dict)

    @property
    def null_ratio(self) -> float:
        return self.null_count / self.total_count if self.total_count > 0 else 0.0

    @property
    def cardinality_ratio(self) -> float:
        return self.distinct_count / self.total_count if self.total_count > 0 else 0.0


@dataclass
class Anomaly:
    """Detected anomaly."""

    type: AnomalyType
    severity: AlertSeverity
    timestamp: datetime
    description: str
    affected_entity: str
    details: Dict[str, Any] = field(default_factory=dict)
    recommended_action: Optional[str] = None
    acknowledged: bool = False


@dataclass
class Pattern:
    """Detected pattern."""

    type: PatternType
    confidence: float
    description: str
    entity: str
    details: Dict[str, Any] = field(default_factory=dict)
    detected_at: datetime = field(default_factory=datetime.now)


@dataclass
class QueryProfile:
    """Profile of query execution."""

    query_hash: str
    sql_template: str
    execution_count: int = 0
    total_time_ms: float = 0.0
    avg_time_ms: float = 0.0
    min_time_ms: float = float("inf")
    max_time_ms: float = 0.0
    rows_returned_avg: float = 0.0
    last_executed: Optional[datetime] = None
    plan_changes: int = 0
    tables_accessed: Set[str] = field(default_factory=set)


@dataclass
class HealthReport:
    """Database health report."""

    generated_at: datetime
    overall_score: float  # 0-100
    anomalies: List[Anomaly] = field(default_factory=list)
    patterns: List[Pattern] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# Statistical Engine
# =============================================================================


class StatisticalEngine:
    """Statistical analysis engine for data profiling."""

    @staticmethod
    def compute_basic_stats(values: List[float]) -> Dict[str, float]:
        """Compute basic statistics for numeric values."""
        if not values:
            return {}

        n = len(values)
        sorted_values = sorted(values)

        mean = sum(values) / n
        variance = sum((x - mean) ** 2 for x in values) / n
        std_dev = math.sqrt(variance)

        # Median
        if n % 2 == 0:
            median = (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
        else:
            median = sorted_values[n // 2]

        # Skewness and Kurtosis
        if std_dev > 0:
            skewness = sum(((x - mean) / std_dev) ** 3 for x in values) / n
            kurtosis = sum(((x - mean) / std_dev) ** 4 for x in values) / n - 3
        else:
            skewness = 0.0
            kurtosis = 0.0

        return {
            "count": n,
            "mean": mean,
            "median": median,
            "std_dev": std_dev,
            "variance": variance,
            "min": min(values),
            "max": max(values),
            "skewness": skewness,
            "kurtosis": kurtosis,
        }

    @staticmethod
    def compute_percentiles(values: List[float], percentiles: List[int] = None) -> Dict[int, float]:
        """Compute percentiles."""
        if not values:
            return {}

        if percentiles is None:
            percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]

        sorted_values = sorted(values)
        n = len(sorted_values)
        result = {}

        for p in percentiles:
            idx = (p / 100) * (n - 1)
            lower_idx = int(idx)
            upper_idx = min(lower_idx + 1, n - 1)
            weight = idx - lower_idx
            result[p] = sorted_values[lower_idx] * (1 - weight) + sorted_values[upper_idx] * weight

        return result

    @staticmethod
    def compute_histogram(values: List[float], num_bins: int = 10) -> List[Tuple[float, float, int]]:
        """Compute histogram bins."""
        if not values:
            return []

        min_val = min(values)
        max_val = max(values)

        if min_val == max_val:
            return [(min_val, max_val, len(values))]

        bin_width = (max_val - min_val) / num_bins
        histogram = []

        for i in range(num_bins):
            bin_min = min_val + i * bin_width
            bin_max = min_val + (i + 1) * bin_width

            if i == num_bins - 1:
                count = sum(1 for v in values if bin_min <= v <= bin_max)
            else:
                count = sum(1 for v in values if bin_min <= v < bin_max)

            histogram.append((bin_min, bin_max, count))

        return histogram

    @staticmethod
    def compute_correlation(x: List[float], y: List[float]) -> float:
        """Compute Pearson correlation coefficient."""
        if len(x) != len(y) or len(x) < 2:
            return 0.0

        n = len(x)
        mean_x = sum(x) / n
        mean_y = sum(y) / n

        numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
        denom_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
        denom_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))

        if denom_x == 0 or denom_y == 0:
            return 0.0

        return numerator / (denom_x * denom_y)


# =============================================================================
# Pattern Detector
# =============================================================================


class PatternDetector:
    """Detects patterns in data and queries."""

    def __init__(self):
        self.patterns_detected: List[Pattern] = []
        self._lock = threading.RLock()

    def detect_trend(self, values: List[float], timestamps: Optional[List[datetime]] = None) -> Optional[Pattern]:
        """Detect trend in time series data."""
        if len(values) < 5:
            return None

        # Simple linear regression
        n = len(values)
        x = list(range(n))
        mean_x = sum(x) / n
        mean_y = sum(values) / n

        numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, values))
        denominator = sum((xi - mean_x) ** 2 for xi in x)

        if denominator == 0:
            return None

        slope = numerator / denominator

        # Determine trend significance
        y_pred = [mean_y + slope * (xi - mean_x) for xi in x]
        ss_res = sum((yi - yp) ** 2 for yi, yp in zip(values, y_pred))
        ss_tot = sum((yi - mean_y) ** 2 for yi in values)

        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

        if r_squared < 0.5:
            return None  # Weak trend

        trend_type = "increasing" if slope > 0 else "decreasing"
        confidence = min(r_squared, 0.99)

        return Pattern(
            type=PatternType.TREND,
            confidence=confidence,
            description=f"{trend_type.capitalize()} trend detected (R² = {r_squared:.3f})",
            entity="time_series",
            details={
                "slope": slope,
                "r_squared": r_squared,
                "trend_type": trend_type,
                "data_points": n,
            },
        )

    def detect_seasonality(self, values: List[float], period: int = 7) -> Optional[Pattern]:
        """Detect seasonal patterns."""
        if len(values) < period * 2:
            return None

        # Compute autocorrelation at the specified period
        n = len(values)
        mean = sum(values) / n
        variance = sum((v - mean) ** 2 for v in values) / n

        if variance == 0:
            return None

        autocorr_values = []
        for lag in range(1, min(period * 2 + 1, n // 2)):
            covariance = sum(
                (values[i] - mean) * (values[i + lag] - mean) for i in range(n - lag)
            ) / (n - lag)
            autocorr_values.append(covariance / variance)

        # Check for significant autocorrelation at the period
        if period - 1 < len(autocorr_values) and autocorr_values[period - 1] > 0.3:
            return Pattern(
                type=PatternType.SEASONALITY,
                confidence=min(autocorr_values[period - 1], 0.99),
                description=f"Seasonal pattern detected with period {period}",
                entity="time_series",
                details={
                    "period": period,
                    "autocorrelation": autocorr_values[period - 1],
                },
            )

        return None

    def detect_correlation(self, column_a: str, values_a: List[float], column_b: str, values_b: List[float]) -> Optional[Pattern]:
        """Detect correlation between columns."""
        correlation = StatisticalEngine.compute_correlation(values_a, values_b)

        if abs(correlation) < 0.7:
            return None

        corr_type = "positive" if correlation > 0 else "negative"

        return Pattern(
            type=PatternType.CORRELATION,
            confidence=abs(correlation),
            description=f"Strong {corr_type} correlation between {column_a} and {column_b}",
            entity=f"{column_a}:{column_b}",
            details={
                "correlation": correlation,
                "column_a": column_a,
                "column_b": column_b,
            },
        )

    def detect_categorical_pattern(self, values: List[Any]) -> Optional[Pattern]:
        """Detect patterns in categorical data."""
        counter = Counter(values)
        total = len(values)

        if total == 0:
            return None

        # Check for dominant category
        most_common = counter.most_common(1)[0]
        dominant_ratio = most_common[1] / total

        if dominant_ratio > 0.8:
            return Pattern(
                type=PatternType.CATEGORICAL,
                confidence=dominant_ratio,
                description=f"Dominant category '{most_common[0]}' accounts for {dominant_ratio:.1%}",
                entity="categorical_column",
                details={
                    "dominant_value": most_common[0],
                    "ratio": dominant_ratio,
                    "unique_values": len(counter),
                },
            )

        # Check for uniform distribution
        expected = total / len(counter)
        chi_squared = sum((count - expected) ** 2 / expected for count in counter.values())

        # If chi-squared is low, distribution is relatively uniform
        if chi_squared < len(counter):
            return Pattern(
                type=PatternType.CATEGORICAL,
                confidence=0.7,
                description=f"Relatively uniform distribution across {len(counter)} categories",
                entity="categorical_column",
                details={
                    "unique_values": len(counter),
                    "chi_squared": chi_squared,
                },
            )

        return None


# =============================================================================
# Anomaly Detector
# =============================================================================


class AnomalyDetector:
    """Detects anomalies in data and system behavior."""

    def __init__(self, sensitivity: float = 2.0):
        """Initialize anomaly detector.

        Args:
            sensitivity: Number of standard deviations for outlier detection
        """
        self.sensitivity = sensitivity
        self.anomalies: List[Anomaly] = []
        self.baseline_stats: Dict[str, Dict[str, float]] = {}
        self._lock = threading.RLock()

    def set_baseline(self, entity: str, stats: Dict[str, float]) -> None:
        """Set baseline statistics for an entity."""
        with self._lock:
            self.baseline_stats[entity] = stats

    def detect_statistical_outlier(self, entity: str, current_value: float, metric_name: str) -> Optional[Anomaly]:
        """Detect statistical outliers based on baseline."""
        baseline = self.baseline_stats.get(entity)
        if not baseline:
            return None

        mean = baseline.get("mean", 0)
        std_dev = baseline.get("std_dev", 1)

        if std_dev == 0:
            return None

        z_score = abs((current_value - mean) / std_dev)

        if z_score > self.sensitivity:
            severity = AlertSeverity.INFO
            if z_score > self.sensitivity * 2:
                severity = AlertSeverity.WARNING
            if z_score > self.sensitivity * 3:
                severity = AlertSeverity.CRITICAL

            return Anomaly(
                type=AnomalyType.STATISTICAL_OUTLIER,
                severity=severity,
                timestamp=datetime.now(),
                description=f"Statistical outlier detected for {metric_name}",
                affected_entity=entity,
                details={
                    "current_value": current_value,
                    "expected_mean": mean,
                    "expected_std_dev": std_dev,
                    "z_score": z_score,
                },
                recommended_action="Investigate the cause of the deviation",
            )

        return None

    def detect_cardinality_change(self, entity: str, current_distinct: int, expected_distinct: int, threshold: float = 0.3) -> Optional[Anomaly]:
        """Detect significant changes in cardinality."""
        if expected_distinct == 0:
            return None

        change_ratio = abs(current_distinct - expected_distinct) / expected_distinct

        if change_ratio > threshold:
            direction = "increased" if current_distinct > expected_distinct else "decreased"

            return Anomaly(
                type=AnomalyType.CARDINALITY_CHANGE,
                severity=AlertSeverity.WARNING if change_ratio > 0.5 else AlertSeverity.INFO,
                timestamp=datetime.now(),
                description=f"Cardinality {direction} by {change_ratio:.1%}",
                affected_entity=entity,
                details={
                    "current_distinct": current_distinct,
                    "expected_distinct": expected_distinct,
                    "change_ratio": change_ratio,
                },
                recommended_action="Review data ingestion pipeline for changes",
            )

        return None

    def detect_null_spike(self, entity: str, current_null_ratio: float, expected_null_ratio: float, threshold: float = 0.1) -> Optional[Anomaly]:
        """Detect spike in null values."""
        if current_null_ratio > expected_null_ratio + threshold:
            return Anomaly(
                type=AnomalyType.DATA_QUALITY,
                severity=AlertSeverity.WARNING if current_null_ratio > 0.5 else AlertSeverity.INFO,
                timestamp=datetime.now(),
                description=f"Null ratio spike: {current_null_ratio:.1%} (expected {expected_null_ratio:.1%})",
                affected_entity=entity,
                details={
                    "current_null_ratio": current_null_ratio,
                    "expected_null_ratio": expected_null_ratio,
                    "increase": current_null_ratio - expected_null_ratio,
                },
                recommended_action="Check data source for missing values",
            )

        return None

    def detect_performance_anomaly(self, entity: str, current_time_ms: float, baseline_time_ms: float, threshold: float = 2.0) -> Optional[Anomaly]:
        """Detect performance degradation."""
        if baseline_time_ms == 0:
            return None

        ratio = current_time_ms / baseline_time_ms

        if ratio > threshold:
            severity = AlertSeverity.INFO
            if ratio > threshold * 2:
                severity = AlertSeverity.WARNING
            if ratio > threshold * 5:
                severity = AlertSeverity.CRITICAL

            return Anomaly(
                type=AnomalyType.PERFORMANCE,
                severity=severity,
                timestamp=datetime.now(),
                description=f"Performance degradation: {ratio:.1f}x slower than baseline",
                affected_entity=entity,
                details={
                    "current_time_ms": current_time_ms,
                    "baseline_time_ms": baseline_time_ms,
                    "slowdown_ratio": ratio,
                },
                recommended_action="Analyze query plan and index usage",
            )

        return None


# =============================================================================
# Query Profiler
# =============================================================================


class QueryProfiler:
    """Profiles query execution patterns."""

    def __init__(self, max_queries: int = 10000):
        """Initialize query profiler.

        Args:
            max_queries: Maximum number of query profiles to store
        """
        self.max_queries = max_queries
        self.profiles: Dict[str, QueryProfile] = {}
        self.recent_queries: deque = deque(maxlen=1000)
        self._lock = threading.RLock()

    def _hash_query(self, sql: str) -> str:
        """Generate a hash for normalized query."""
        import hashlib

        # Normalize: remove literals, lowercase
        normalized = sql.lower()
        normalized = re.sub(r"'[^']*'", "?", normalized)
        normalized = re.sub(r"\d+", "?", normalized)
        normalized = " ".join(normalized.split())

        return hashlib.md5(normalized.encode()).hexdigest()[:16]

    def record_execution(self, sql: str, execution_time_ms: float, rows_returned: int, tables: Set[str]) -> None:
        """Record a query execution."""
        with self._lock:
            query_hash = self._hash_query(sql)

            if query_hash not in self.profiles:
                # Evict oldest if at capacity
                if len(self.profiles) >= self.max_queries:
                    oldest = min(self.profiles.items(), key=lambda x: x[1].last_executed or datetime.min)
                    del self.profiles[oldest[0]]

                self.profiles[query_hash] = QueryProfile(query_hash=query_hash, sql_template=sql)

            profile = self.profiles[query_hash]
            profile.execution_count += 1
            profile.total_time_ms += execution_time_ms
            profile.avg_time_ms = profile.total_time_ms / profile.execution_count
            profile.min_time_ms = min(profile.min_time_ms, execution_time_ms)
            profile.max_time_ms = max(profile.max_time_ms, execution_time_ms)
            profile.rows_returned_avg = (profile.rows_returned_avg * (profile.execution_count - 1) + rows_returned) / profile.execution_count
            profile.last_executed = datetime.now()
            profile.tables_accessed.update(tables)

            self.recent_queries.append({
                "hash": query_hash,
                "time_ms": execution_time_ms,
                "rows": rows_returned,
                "timestamp": datetime.now(),
            })

    def get_slow_queries(self, threshold_ms: float = 1000.0, limit: int = 10) -> List[QueryProfile]:
        """Get queries with average execution time above threshold."""
        with self._lock:
            slow = [p for p in self.profiles.values() if p.avg_time_ms > threshold_ms]
            return sorted(slow, key=lambda x: x.avg_time_ms, reverse=True)[:limit]

    def get_frequent_queries(self, limit: int = 10) -> List[QueryProfile]:
        """Get most frequently executed queries."""
        with self._lock:
            return sorted(self.profiles.values(), key=lambda x: x.execution_count, reverse=True)[:limit]

    def get_table_access_stats(self) -> Dict[str, int]:
        """Get table access statistics."""
        with self._lock:
            access_counts: Dict[str, int] = {}
            for profile in self.profiles.values():
                for table in profile.tables_accessed:
                    access_counts[table] = access_counts.get(table, 0) + profile.execution_count
            return dict(sorted(access_counts.items(), key=lambda x: x[1], reverse=True))


# =============================================================================
# Report Generator
# =============================================================================


class ReportGenerator:
    """Generates health and analysis reports."""

    def __init__(self, analyst: Analyst):
        """Initialize report generator."""
        self.analyst = analyst

    def generate_health_report(self) -> HealthReport:
        """Generate comprehensive health report."""
        anomalies = list(self.analyst.anomaly_detector.anomalies)
        patterns = list(self.analyst.pattern_detector.patterns_detected)

        # Calculate overall score
        severity_weights = {
            AlertSeverity.INFO: 1,
            AlertSeverity.WARNING: 5,
            AlertSeverity.CRITICAL: 15,
            AlertSeverity.EMERGENCY: 30,
        }

        penalty = sum(severity_weights.get(a.severity, 0) for a in anomalies if not a.acknowledged)
        score = max(0, 100 - penalty)

        # Generate recommendations
        recommendations = []

        slow_queries = self.analyst.query_profiler.get_slow_queries(limit=3)
        if slow_queries:
            for query in slow_queries:
                recommendations.append(f"Optimize slow query (avg {query.avg_time_ms:.0f}ms): {query.sql_template[:50]}...")

        critical_anomalies = [a for a in anomalies if a.severity == AlertSeverity.CRITICAL and not a.acknowledged]
        for anomaly in critical_anomalies[:3]:
            if anomaly.recommended_action:
                recommendations.append(anomaly.recommended_action)

        # Collect metrics
        metrics = {
            "total_queries_profiled": len(self.analyst.query_profiler.profiles),
            "total_anomalies": len(anomalies),
            "unacknowledged_anomalies": sum(1 for a in anomalies if not a.acknowledged),
            "patterns_detected": len(patterns),
            "table_access_stats": self.analyst.query_profiler.get_table_access_stats(),
        }

        return HealthReport(
            generated_at=datetime.now(),
            overall_score=score,
            anomalies=anomalies,
            patterns=patterns,
            recommendations=recommendations,
            metrics=metrics,
        )

    def generate_profile_report(self, profile: DataProfile) -> str:
        """Generate text report for a data profile."""
        lines = [
            f"=== Data Profile: {profile.column_name} ===",
            f"Type: {profile.data_type}",
            f"Total Count: {profile.total_count:,}",
            f"Null Count: {profile.null_count:,} ({profile.null_ratio:.1%})",
            f"Distinct Values: {profile.distinct_count:,} ({profile.cardinality_ratio:.1%})",
        ]

        if profile.min_value is not None:
            lines.append(f"Range: [{profile.min_value}, {profile.max_value}]")

        if profile.mean is not None:
            lines.extend([
                f"Mean: {profile.mean:.4f}",
                f"Median: {profile.median:.4f}",
                f"Std Dev: {profile.std_dev:.4f}",
                f"Skewness: {profile.skewness:.4f}",
                f"Kurtosis: {profile.kurtosis:.4f}",
            ])

        if profile.percentiles:
            lines.append("Percentiles:")
            for p, v in sorted(profile.percentiles.items()):
                lines.append(f"  P{p}: {v:.4f}")

        if profile.most_common:
            lines.append("Most Common Values:")
            for value, count in profile.most_common[:5]:
                lines.append(f"  {value}: {count:,}")

        return "\n".join(lines)


# =============================================================================
# Main Analyst Agent
# =============================================================================


import re


class Analyst:
    """Analyst Agent - Pattern detection and anomaly analysis.

    Core Responsibilities:
    1. Data profiling and statistical analysis
    2. Pattern detection (trends, seasonality, correlations)
    3. Anomaly detection and alerting
    4. Query performance profiling

    Seed configuration is loaded from a YAML codex file.
    """

    # Core principle
    CORE_PRINCIPLE = "Data speaks - learn to listen and interpret."
    MORAL_CONSTANT = "Provide insights that are actionable and accurate."

    def __init__(self, state_root: Path = DEFAULT_STATE_ROOT, seed_path: Optional[Path] = None):
        """Initialize Analyst agent.

        Args:
            state_root: Root directory for analyst state
            seed_path: Path to seed configuration YAML
        """
        self.state_root = state_root
        self.state_root.mkdir(parents=True, exist_ok=True)

        # Load seed configuration
        self.seed = self._load_seed(seed_path)

        # Initialize components
        self.statistical_engine = StatisticalEngine()
        self.pattern_detector = PatternDetector()
        self.anomaly_detector = AnomalyDetector(sensitivity=self.seed.get("anomaly_sensitivity", 2.0))
        self.query_profiler = QueryProfiler(max_queries=self.seed.get("max_query_profiles", 10000))
        self.report_generator = ReportGenerator(self)

        # Data profiles cache
        self.profiles: Dict[str, DataProfile] = {}

        logger.info(f"Analyst agent initialized at {state_root}")
        logger.info(f"Core principle: {self.CORE_PRINCIPLE}")

    def _load_seed(self, seed_path: Optional[Path]) -> Dict[str, Any]:
        """Load seed configuration from YAML."""
        default_seed = {
            "anomaly_sensitivity": 2.0,
            "max_query_profiles": 10000,
            "profile_sample_size": 10000,
            "trend_min_points": 10,
            "correlation_threshold": 0.7,
            "alert_retention_days": 30,
        }

        if seed_path and seed_path.exists():
            with open(seed_path) as f:
                loaded = yaml.safe_load(f)
                default_seed.update(loaded)

        return default_seed

    def profile_column(self, column_name: str, values: List[Any], data_type: str = "auto") -> DataProfile:
        """Profile a data column.

        Args:
            column_name: Column name
            values: Column values
            data_type: Data type (auto-detected if 'auto')

        Returns:
            DataProfile with statistics
        """
        # Auto-detect type
        if data_type == "auto":
            sample = [v for v in values[:100] if v is not None]
            if sample:
                if all(isinstance(v, (int, float)) for v in sample):
                    data_type = "numeric"
                elif all(isinstance(v, str) for v in sample):
                    data_type = "string"
                elif all(isinstance(v, datetime) for v in sample):
                    data_type = "datetime"
                else:
                    data_type = "mixed"
            else:
                data_type = "unknown"

        # Basic counts
        total_count = len(values)
        null_count = sum(1 for v in values if v is None)
        non_null = [v for v in values if v is not None]
        distinct_count = len(set(non_null))

        profile = DataProfile(
            column_name=column_name,
            data_type=data_type,
            total_count=total_count,
            null_count=null_count,
            distinct_count=distinct_count,
        )

        if non_null:
            profile.min_value = min(non_null)
            profile.max_value = max(non_null)

            # Most common values
            counter = Counter(non_null)
            profile.most_common = counter.most_common(10)

            # Numeric statistics
            if data_type == "numeric":
                numeric_values = [float(v) for v in non_null if isinstance(v, (int, float))]
                if numeric_values:
                    stats = self.statistical_engine.compute_basic_stats(numeric_values)
                    profile.mean = stats.get("mean")
                    profile.median = stats.get("median")
                    profile.std_dev = stats.get("std_dev")
                    profile.variance = stats.get("variance")
                    profile.skewness = stats.get("skewness")
                    profile.kurtosis = stats.get("kurtosis")
                    profile.percentiles = self.statistical_engine.compute_percentiles(numeric_values)
                    profile.histogram = self.statistical_engine.compute_histogram(numeric_values)

        self.profiles[column_name] = profile
        return profile

    def detect_patterns(self, column_name: str, values: List[Any]) -> List[Pattern]:
        """Detect patterns in column data.

        Args:
            column_name: Column name
            values: Column values

        Returns:
            List of detected patterns
        """
        patterns = []

        non_null = [v for v in values if v is not None]
        if not non_null:
            return patterns

        # Numeric patterns
        if all(isinstance(v, (int, float)) for v in non_null[:100]):
            numeric_values = [float(v) for v in non_null]

            # Trend detection
            trend = self.pattern_detector.detect_trend(numeric_values)
            if trend:
                trend.entity = column_name
                patterns.append(trend)

            # Seasonality (if enough data)
            if len(numeric_values) >= 30:
                for period in [7, 30, 365]:
                    seasonality = self.pattern_detector.detect_seasonality(numeric_values, period)
                    if seasonality:
                        seasonality.entity = column_name
                        patterns.append(seasonality)
                        break

        # Categorical patterns
        else:
            cat_pattern = self.pattern_detector.detect_categorical_pattern(non_null)
            if cat_pattern:
                cat_pattern.entity = column_name
                patterns.append(cat_pattern)

        return patterns

    def record_query(self, sql: str, execution_time_ms: float, rows_returned: int, tables: Optional[Set[str]] = None) -> None:
        """Record query execution for profiling."""
        self.query_profiler.record_execution(sql, execution_time_ms, rows_returned, tables or set())

    def check_anomalies(self, entity: str, metrics: Dict[str, float]) -> List[Anomaly]:
        """Check for anomalies in metrics.

        Args:
            entity: Entity name
            metrics: Current metric values

        Returns:
            List of detected anomalies
        """
        anomalies = []

        for metric_name, value in metrics.items():
            anomaly = self.anomaly_detector.detect_statistical_outlier(entity, value, metric_name)
            if anomaly:
                anomalies.append(anomaly)
                self.anomaly_detector.anomalies.append(anomaly)

        return anomalies

    def generate_report(self) -> HealthReport:
        """Generate health report."""
        return self.report_generator.generate_health_report()

    def get_stats(self) -> Dict[str, Any]:
        """Get analyst statistics."""
        return {
            "profiles_cached": len(self.profiles),
            "patterns_detected": len(self.pattern_detector.patterns_detected),
            "anomalies_detected": len(self.anomaly_detector.anomalies),
            "queries_profiled": len(self.query_profiler.profiles),
            "slow_queries": len(self.query_profiler.get_slow_queries()),
        }


# =============================================================================
# CLI Interface
# =============================================================================


def create_cli():
    """Create CLI for Analyst agent."""
    import argparse

    parser = argparse.ArgumentParser(description="RoadDB Analyst Agent - Pattern detection and analysis")

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Profile
    profile_parser = subparsers.add_parser("profile", help="Profile data")
    profile_parser.add_argument("--file", help="JSON file with data")

    # Report
    subparsers.add_parser("report", help="Generate health report")

    # Queries
    queries_parser = subparsers.add_parser("queries", help="Show query profiles")
    queries_parser.add_argument("--slow", action="store_true", help="Show slow queries")
    queries_parser.add_argument("--frequent", action="store_true", help="Show frequent queries")

    # Stats
    subparsers.add_parser("stats", help="Show analyst statistics")

    return parser


def main():
    """Main entry point."""
    parser = create_cli()
    args = parser.parse_args()

    analyst = Analyst()

    if args.command == "profile":
        if args.file:
            with open(args.file) as f:
                data = json.load(f)
            for col, values in data.items():
                profile = analyst.profile_column(col, values)
                print(analyst.report_generator.generate_profile_report(profile))
                print()
        else:
            print("Provide --file with JSON data")

    elif args.command == "report":
        report = analyst.generate_report()
        print(f"Health Score: {report.overall_score}/100")
        print(f"Generated: {report.generated_at}")
        print(f"Anomalies: {len(report.anomalies)}")
        print(f"Patterns: {len(report.patterns)}")
        if report.recommendations:
            print("\nRecommendations:")
            for rec in report.recommendations:
                print(f"  - {rec}")

    elif args.command == "queries":
        if args.slow:
            queries = analyst.query_profiler.get_slow_queries()
            print("Slow Queries:")
            for q in queries:
                print(f"  [{q.avg_time_ms:.0f}ms] {q.sql_template[:60]}...")
        elif args.frequent:
            queries = analyst.query_profiler.get_frequent_queries()
            print("Frequent Queries:")
            for q in queries:
                print(f"  [{q.execution_count}x] {q.sql_template[:60]}...")

    elif args.command == "stats":
        stats = analyst.get_stats()
        for key, value in stats.items():
            print(f"{key}: {value}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()


__all__ = [
    "Analyst",
    "AnomalyType",
    "PatternType",
    "AlertSeverity",
    "DataProfile",
    "Anomaly",
    "Pattern",
    "QueryProfile",
    "HealthReport",
    "StatisticalEngine",
    "PatternDetector",
    "AnomalyDetector",
    "QueryProfiler",
    "ReportGenerator",
]
