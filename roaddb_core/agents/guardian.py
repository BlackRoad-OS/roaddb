"""RoadDB Guardian Agent - Security, access control, and audit.

Like Lucidia Core's guardian of ethical principles, the Guardian agent
protects data integrity, enforces access policies, and maintains audit trails.

Architecture:
    ┌─────────────────────────────────────────────────────────────────────┐
    │                         Guardian Agent                             │
    ├─────────────────────────────────────────────────────────────────────┤
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
    │  │   Access    │  │  Encryption │  │    Audit    │                 │
    │  │   Control   │──│   Manager   │──│    Logger   │                 │
    │  └─────────────┘  └─────────────┘  └─────────────┘                 │
    │         │               │               │                           │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
    │  │   Policy    │  │   Key       │  │   Anomaly   │                 │
    │  │   Engine    │──│   Rotation  │──│   Detector  │                 │
    │  └─────────────┘  └─────────────┘  └─────────────┘                 │
    └─────────────────────────────────────────────────────────────────────┘

Core Principle: "Trust but verify - protect data while enabling access."
Moral Constant: Security without obstruction, protection without paranoia.

Copyright (c) 2024-2026 BlackRoad OS, Inc. All rights reserved.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import yaml

# Configure logging
logger = logging.getLogger(__name__)

# Default paths
DEFAULT_STATE_ROOT = Path.home() / ".roaddb" / "guardian"
DEFAULT_AUDIT_DIR = "audit"
DEFAULT_KEYS_DIR = "keys"


class Permission(Enum):
    """Database permissions."""

    # Data permissions
    SELECT = auto()
    INSERT = auto()
    UPDATE = auto()
    DELETE = auto()

    # Schema permissions
    CREATE = auto()
    ALTER = auto()
    DROP = auto()
    INDEX = auto()

    # Admin permissions
    GRANT = auto()
    REVOKE = auto()
    ADMIN = auto()

    # Special permissions
    EXECUTE = auto()
    REFERENCES = auto()
    TRIGGER = auto()
    TRUNCATE = auto()

    # Meta permissions
    ALL = auto()
    USAGE = auto()


class ResourceType(Enum):
    """Types of database resources."""

    DATABASE = auto()
    SCHEMA = auto()
    TABLE = auto()
    COLUMN = auto()
    INDEX = auto()
    VIEW = auto()
    FUNCTION = auto()
    SEQUENCE = auto()
    ROLE = auto()


class EncryptionAlgorithm(Enum):
    """Supported encryption algorithms."""

    AES_256_GCM = "aes-256-gcm"
    AES_256_CBC = "aes-256-cbc"
    CHACHA20_POLY1305 = "chacha20-poly1305"


class AuditAction(Enum):
    """Actions to audit."""

    LOGIN = auto()
    LOGOUT = auto()
    QUERY = auto()
    DDL = auto()
    GRANT = auto()
    REVOKE = auto()
    ERROR = auto()
    POLICY_VIOLATION = auto()
    KEY_ROTATION = auto()
    CONFIG_CHANGE = auto()


@dataclass
class Principal:
    """Security principal (user/role/service)."""

    name: str
    principal_type: str = "user"  # user, role, service
    attributes: Dict[str, Any] = field(default_factory=dict)
    roles: Set[str] = field(default_factory=set)
    created_at: datetime = field(default_factory=datetime.now)
    last_login: Optional[datetime] = None
    password_hash: Optional[str] = None
    mfa_enabled: bool = False
    locked: bool = False
    failed_attempts: int = 0


@dataclass
class AccessGrant:
    """Permission grant on a resource."""

    principal: str
    resource_type: ResourceType
    resource_name: str
    permissions: Set[Permission]
    granted_by: str
    granted_at: datetime = field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None
    with_grant_option: bool = False
    conditions: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AuditEntry:
    """Entry in the audit log."""

    timestamp: datetime
    action: AuditAction
    principal: str
    resource: Optional[str]
    details: Dict[str, Any]
    success: bool
    client_ip: Optional[str] = None
    session_id: Optional[str] = None
    duration_ms: Optional[float] = None


@dataclass
class RowLevelPolicy:
    """Row-level security policy."""

    name: str
    table: str
    command: str  # SELECT, INSERT, UPDATE, DELETE, ALL
    using_expression: str  # Filter expression
    check_expression: Optional[str] = None  # For INSERT/UPDATE
    roles: Set[str] = field(default_factory=set)
    enabled: bool = True


@dataclass
class ColumnMask:
    """Column-level data masking policy."""

    table: str
    column: str
    mask_type: str  # full, partial, hash, random, custom
    mask_function: Optional[str] = None
    roles_exempt: Set[str] = field(default_factory=set)


# =============================================================================
# Password Hashing
# =============================================================================


class PasswordHasher:
    """Secure password hashing using PBKDF2."""

    def __init__(self, iterations: int = 310000):
        """Initialize hasher.

        Args:
            iterations: PBKDF2 iteration count
        """
        self.iterations = iterations
        self.algorithm = "sha256"

    def hash(self, password: str) -> str:
        """Hash a password.

        Args:
            password: Plain text password

        Returns:
            Encoded hash string
        """
        salt = secrets.token_bytes(32)
        hash_bytes = hashlib.pbkdf2_hmac(self.algorithm, password.encode("utf-8"), salt, self.iterations)

        return f"pbkdf2:{self.algorithm}:{self.iterations}${base64.b64encode(salt).decode()}${base64.b64encode(hash_bytes).decode()}"

    def verify(self, password: str, hash_string: str) -> bool:
        """Verify a password against a hash.

        Args:
            password: Plain text password
            hash_string: Stored hash string

        Returns:
            True if password matches
        """
        try:
            parts = hash_string.split("$")
            if len(parts) != 3:
                return False

            header = parts[0].split(":")
            if header[0] != "pbkdf2":
                return False

            algorithm = header[1]
            iterations = int(header[2])
            salt = base64.b64decode(parts[1])
            stored_hash = base64.b64decode(parts[2])

            computed_hash = hashlib.pbkdf2_hmac(algorithm, password.encode("utf-8"), salt, iterations)

            return hmac.compare_digest(computed_hash, stored_hash)

        except Exception:
            return False


# =============================================================================
# Encryption Manager
# =============================================================================


class EncryptionManager:
    """Manages encryption keys and data encryption.

    Supports:
    - Key generation and rotation
    - Transparent data encryption (TDE)
    - Column-level encryption
    - Key escrow and recovery
    """

    def __init__(self, keys_dir: Path):
        """Initialize encryption manager.

        Args:
            keys_dir: Directory for key storage
        """
        self.keys_dir = keys_dir
        self.keys_dir.mkdir(parents=True, exist_ok=True)

        self._master_key: Optional[bytes] = None
        self._data_keys: Dict[str, bytes] = {}
        self._key_versions: Dict[str, int] = {}
        self._lock = threading.RLock()

    def initialize_master_key(self, key: Optional[bytes] = None) -> bytes:
        """Initialize or set the master key.

        Args:
            key: Optional existing master key

        Returns:
            The master key
        """
        with self._lock:
            if key is not None:
                self._master_key = key
            elif self._master_key is None:
                self._master_key = secrets.token_bytes(32)

            # Store encrypted master key (in production, use HSM or KMS)
            key_file = self.keys_dir / "master.key"
            # This is simplified - production would use proper key protection
            key_file.write_bytes(self._master_key)

            return self._master_key

    def generate_data_key(self, name: str) -> bytes:
        """Generate a new data encryption key.

        Args:
            name: Name/identifier for the key

        Returns:
            Generated data key
        """
        with self._lock:
            key = secrets.token_bytes(32)
            self._data_keys[name] = key
            self._key_versions[name] = self._key_versions.get(name, 0) + 1

            # Store encrypted data key
            if self._master_key:
                encrypted_key = self._encrypt_key(key, self._master_key)
                key_file = self.keys_dir / f"{name}.v{self._key_versions[name]}.key"
                key_file.write_bytes(encrypted_key)

            logger.info(f"Generated data key: {name} v{self._key_versions[name]}")
            return key

    def rotate_key(self, name: str) -> bytes:
        """Rotate a data encryption key.

        Args:
            name: Key name to rotate

        Returns:
            New key value
        """
        old_version = self._key_versions.get(name, 0)
        new_key = self.generate_data_key(name)
        logger.info(f"Rotated key {name}: v{old_version} -> v{self._key_versions[name]}")
        return new_key

    def get_key(self, name: str, version: Optional[int] = None) -> Optional[bytes]:
        """Get a data encryption key.

        Args:
            name: Key name
            version: Specific version (latest if None)

        Returns:
            Key bytes or None
        """
        with self._lock:
            if version is None:
                return self._data_keys.get(name)

            # Load from storage
            key_file = self.keys_dir / f"{name}.v{version}.key"
            if key_file.exists() and self._master_key:
                encrypted_key = key_file.read_bytes()
                return self._decrypt_key(encrypted_key, self._master_key)

            return None

    def _encrypt_key(self, data_key: bytes, master_key: bytes) -> bytes:
        """Encrypt a data key with the master key."""
        # Simplified - production would use proper authenticated encryption
        nonce = secrets.token_bytes(12)
        # XOR for demonstration - use AES-GCM in production
        encrypted = bytes(a ^ b for a, b in zip(data_key, master_key[:32]))
        return nonce + encrypted

    def _decrypt_key(self, encrypted_key: bytes, master_key: bytes) -> bytes:
        """Decrypt a data key with the master key."""
        nonce = encrypted_key[:12]
        encrypted = encrypted_key[12:]
        return bytes(a ^ b for a, b in zip(encrypted, master_key[:32]))

    def encrypt_value(self, value: bytes, key_name: str) -> bytes:
        """Encrypt a value using a named key.

        Args:
            value: Data to encrypt
            key_name: Key to use

        Returns:
            Encrypted data with metadata
        """
        key = self.get_key(key_name)
        if not key:
            raise ValueError(f"Key not found: {key_name}")

        nonce = secrets.token_bytes(12)
        version = self._key_versions.get(key_name, 1)

        # Simplified encryption - use proper AES-GCM in production
        encrypted = bytes(a ^ b for a, b in zip(value, (key * (len(value) // 32 + 1))[: len(value)]))

        # Format: version (2 bytes) + nonce (12 bytes) + ciphertext
        return version.to_bytes(2, "big") + nonce + encrypted

    def decrypt_value(self, encrypted: bytes, key_name: str) -> bytes:
        """Decrypt a value.

        Args:
            encrypted: Encrypted data with metadata
            key_name: Key to use

        Returns:
            Decrypted data
        """
        version = int.from_bytes(encrypted[:2], "big")
        nonce = encrypted[2:14]
        ciphertext = encrypted[14:]

        key = self.get_key(key_name, version)
        if not key:
            raise ValueError(f"Key not found: {key_name} v{version}")

        # Simplified decryption
        return bytes(a ^ b for a, b in zip(ciphertext, (key * (len(ciphertext) // 32 + 1))[: len(ciphertext)]))


# =============================================================================
# Access Control
# =============================================================================


class AccessController:
    """Role-based and attribute-based access control.

    Implements:
    - RBAC (Role-Based Access Control)
    - ABAC (Attribute-Based Access Control)
    - Row-level security (RLS)
    - Column-level permissions
    """

    def __init__(self, state_root: Path):
        """Initialize access controller.

        Args:
            state_root: State directory
        """
        self.state_root = state_root
        self.principals: Dict[str, Principal] = {}
        self.grants: List[AccessGrant] = []
        self.row_policies: List[RowLevelPolicy] = []
        self.column_masks: List[ColumnMask] = []
        self._lock = threading.RLock()

        # Create default admin role
        self.create_principal("admin", "role")

    def create_principal(self, name: str, principal_type: str = "user", **attributes) -> Principal:
        """Create a new principal.

        Args:
            name: Principal name
            principal_type: user, role, or service
            **attributes: Additional attributes

        Returns:
            Created principal
        """
        with self._lock:
            if name in self.principals:
                raise ValueError(f"Principal already exists: {name}")

            principal = Principal(name=name, principal_type=principal_type, attributes=attributes)
            self.principals[name] = principal

            logger.info(f"Created {principal_type}: {name}")
            return principal

    def grant(
        self,
        principal: str,
        permissions: Set[Permission],
        resource_type: ResourceType,
        resource_name: str,
        granted_by: str,
        **options,
    ) -> AccessGrant:
        """Grant permissions to a principal.

        Args:
            principal: Principal name
            permissions: Set of permissions to grant
            resource_type: Type of resource
            resource_name: Resource identifier
            granted_by: Granting principal
            **options: Additional grant options

        Returns:
            Created grant
        """
        with self._lock:
            if principal not in self.principals:
                raise ValueError(f"Unknown principal: {principal}")

            grant = AccessGrant(
                principal=principal,
                resource_type=resource_type,
                resource_name=resource_name,
                permissions=permissions,
                granted_by=granted_by,
                with_grant_option=options.get("with_grant_option", False),
                expires_at=options.get("expires_at"),
                conditions=options.get("conditions", {}),
            )

            self.grants.append(grant)
            logger.info(f"Granted {permissions} on {resource_name} to {principal}")
            return grant

    def revoke(
        self, principal: str, permissions: Set[Permission], resource_type: ResourceType, resource_name: str
    ) -> bool:
        """Revoke permissions from a principal.

        Args:
            principal: Principal name
            permissions: Permissions to revoke
            resource_type: Resource type
            resource_name: Resource identifier

        Returns:
            True if any grants were revoked
        """
        with self._lock:
            revoked = False
            new_grants = []

            for grant in self.grants:
                if (
                    grant.principal == principal
                    and grant.resource_type == resource_type
                    and grant.resource_name == resource_name
                ):
                    remaining = grant.permissions - permissions
                    if remaining:
                        grant.permissions = remaining
                        new_grants.append(grant)
                    revoked = True
                else:
                    new_grants.append(grant)

            self.grants = new_grants
            return revoked

    def check_permission(
        self,
        principal: str,
        permission: Permission,
        resource_type: ResourceType,
        resource_name: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Check if a principal has a permission.

        Args:
            principal: Principal to check
            permission: Required permission
            resource_type: Resource type
            resource_name: Resource identifier
            context: Additional context for ABAC

        Returns:
            True if permission granted
        """
        with self._lock:
            if principal not in self.principals:
                return False

            principal_obj = self.principals[principal]

            # Check direct grants and role grants
            principals_to_check = {principal} | principal_obj.roles

            now = datetime.now()

            for grant in self.grants:
                if grant.principal not in principals_to_check:
                    continue

                if grant.resource_type != resource_type:
                    continue

                # Check resource match (supports wildcards)
                if not self._resource_matches(grant.resource_name, resource_name):
                    continue

                # Check expiration
                if grant.expires_at and grant.expires_at < now:
                    continue

                # Check permissions
                if permission in grant.permissions or Permission.ALL in grant.permissions:
                    # Check ABAC conditions
                    if grant.conditions and context:
                        if not self._evaluate_conditions(grant.conditions, context):
                            continue
                    return True

            return False

    def _resource_matches(self, pattern: str, resource: str) -> bool:
        """Check if resource matches pattern (supports * wildcard)."""
        if pattern == "*":
            return True
        if "*" in pattern:
            regex = pattern.replace(".", r"\.").replace("*", ".*")
            return bool(re.match(f"^{regex}$", resource))
        return pattern == resource

    def _evaluate_conditions(self, conditions: Dict[str, Any], context: Dict[str, Any]) -> bool:
        """Evaluate ABAC conditions."""
        for key, expected in conditions.items():
            actual = context.get(key)
            if isinstance(expected, list):
                if actual not in expected:
                    return False
            elif actual != expected:
                return False
        return True

    def add_row_policy(self, policy: RowLevelPolicy) -> None:
        """Add a row-level security policy."""
        with self._lock:
            self.row_policies.append(policy)
            logger.info(f"Added RLS policy: {policy.name} on {policy.table}")

    def get_row_filter(self, table: str, principal: str, command: str) -> Optional[str]:
        """Get the combined row filter for a principal on a table.

        Args:
            table: Table name
            principal: Principal name
            command: SQL command (SELECT, INSERT, etc.)

        Returns:
            Combined filter expression or None
        """
        with self._lock:
            if principal not in self.principals:
                return "FALSE"  # Deny all

            principal_obj = self.principals[principal]
            principals_to_check = {principal} | principal_obj.roles

            filters = []
            for policy in self.row_policies:
                if not policy.enabled:
                    continue
                if policy.table != table:
                    continue
                if policy.command not in (command, "ALL"):
                    continue
                if policy.roles and not (policy.roles & principals_to_check):
                    continue

                filters.append(f"({policy.using_expression})")

            if not filters:
                return None  # No policies apply

            return " AND ".join(filters)

    def add_column_mask(self, mask: ColumnMask) -> None:
        """Add a column masking policy."""
        with self._lock:
            self.column_masks.append(mask)
            logger.info(f"Added column mask: {mask.table}.{mask.column} ({mask.mask_type})")

    def get_column_mask(self, table: str, column: str, principal: str) -> Optional[str]:
        """Get the masking expression for a column.

        Args:
            table: Table name
            column: Column name
            principal: Principal name

        Returns:
            Masking expression or None if no masking needed
        """
        with self._lock:
            if principal not in self.principals:
                return "'***DENIED***'"

            principal_obj = self.principals[principal]
            principals_to_check = {principal} | principal_obj.roles

            for mask in self.column_masks:
                if mask.table != table or mask.column != column:
                    continue

                # Check if principal is exempt
                if mask.roles_exempt & principals_to_check:
                    return None

                # Return appropriate mask
                if mask.mask_type == "full":
                    return "'****'"
                elif mask.mask_type == "partial":
                    return f"CONCAT(LEFT({column}, 2), '****', RIGHT({column}, 2))"
                elif mask.mask_type == "hash":
                    return f"MD5({column})"
                elif mask.mask_type == "random":
                    return "'[REDACTED]'"
                elif mask.mask_type == "custom" and mask.mask_function:
                    return mask.mask_function

            return None


# =============================================================================
# Audit Logger
# =============================================================================


class AuditLogger:
    """Comprehensive audit logging system.

    Features:
    - Tamper-evident log chain
    - Configurable verbosity
    - Log rotation and archival
    - Real-time anomaly detection
    """

    def __init__(self, audit_dir: Path, retention_days: int = 90):
        """Initialize audit logger.

        Args:
            audit_dir: Directory for audit logs
            retention_days: Days to retain logs
        """
        self.audit_dir = audit_dir
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        self.retention_days = retention_days

        self._log_file: Optional[Path] = None
        self._chain_hash: Optional[str] = None
        self._entry_count = 0
        self._lock = threading.RLock()

        self._rotate_log()

    def log(
        self,
        action: AuditAction,
        principal: str,
        success: bool,
        resource: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> AuditEntry:
        """Log an audit entry.

        Args:
            action: Action being audited
            principal: Principal performing action
            success: Whether action succeeded
            resource: Resource affected
            details: Additional details
            **kwargs: Extra fields (client_ip, session_id, duration_ms)

        Returns:
            Created audit entry
        """
        with self._lock:
            entry = AuditEntry(
                timestamp=datetime.now(),
                action=action,
                principal=principal,
                resource=resource,
                details=details or {},
                success=success,
                client_ip=kwargs.get("client_ip"),
                session_id=kwargs.get("session_id"),
                duration_ms=kwargs.get("duration_ms"),
            )

            # Create tamper-evident chain
            entry_dict = {
                "timestamp": entry.timestamp.isoformat(),
                "action": entry.action.name,
                "principal": entry.principal,
                "resource": entry.resource,
                "details": entry.details,
                "success": entry.success,
                "client_ip": entry.client_ip,
                "session_id": entry.session_id,
                "duration_ms": entry.duration_ms,
                "prev_hash": self._chain_hash,
            }

            entry_json = json.dumps(entry_dict, sort_keys=True)
            self._chain_hash = hashlib.sha256(entry_json.encode()).hexdigest()
            entry_dict["hash"] = self._chain_hash

            # Write to log
            with open(self._log_file, "a") as f:
                f.write(json.dumps(entry_dict) + "\n")

            self._entry_count += 1

            # Rotate if needed
            if self._entry_count >= 10000:
                self._rotate_log()

            return entry

    def query(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        principal: Optional[str] = None,
        action: Optional[AuditAction] = None,
        resource: Optional[str] = None,
        success_only: bool = False,
        limit: int = 1000,
    ) -> List[AuditEntry]:
        """Query audit log entries.

        Args:
            start_time: Start of time range
            end_time: End of time range
            principal: Filter by principal
            action: Filter by action
            resource: Filter by resource
            success_only: Only successful actions
            limit: Maximum entries to return

        Returns:
            Matching audit entries
        """
        entries = []

        # Collect log files in range
        log_files = sorted(self.audit_dir.glob("audit_*.jsonl"))

        for log_file in log_files:
            with open(log_file) as f:
                for line in f:
                    if len(entries) >= limit:
                        break

                    try:
                        data = json.loads(line.strip())
                        entry_time = datetime.fromisoformat(data["timestamp"])

                        # Apply filters
                        if start_time and entry_time < start_time:
                            continue
                        if end_time and entry_time > end_time:
                            continue
                        if principal and data["principal"] != principal:
                            continue
                        if action and data["action"] != action.name:
                            continue
                        if resource and data["resource"] != resource:
                            continue
                        if success_only and not data["success"]:
                            continue

                        entries.append(
                            AuditEntry(
                                timestamp=entry_time,
                                action=AuditAction[data["action"]],
                                principal=data["principal"],
                                resource=data["resource"],
                                details=data["details"],
                                success=data["success"],
                                client_ip=data.get("client_ip"),
                                session_id=data.get("session_id"),
                                duration_ms=data.get("duration_ms"),
                            )
                        )

                    except (json.JSONDecodeError, KeyError):
                        continue

        return entries

    def verify_chain(self, log_file: Optional[Path] = None) -> Tuple[bool, int]:
        """Verify the tamper-evident chain.

        Args:
            log_file: Specific file to verify (current if None)

        Returns:
            (is_valid, verified_count)
        """
        target = log_file or self._log_file
        prev_hash = None
        verified = 0

        with open(target) as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    stored_hash = data.pop("hash")

                    # Verify prev_hash matches
                    if data.get("prev_hash") != prev_hash:
                        return False, verified

                    # Verify hash
                    entry_json = json.dumps(data, sort_keys=True)
                    computed_hash = hashlib.sha256(entry_json.encode()).hexdigest()

                    if computed_hash != stored_hash:
                        return False, verified

                    prev_hash = stored_hash
                    verified += 1

                except (json.JSONDecodeError, KeyError):
                    return False, verified

        return True, verified

    def _rotate_log(self) -> None:
        """Rotate to a new log file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._log_file = self.audit_dir / f"audit_{timestamp}.jsonl"
        self._chain_hash = None
        self._entry_count = 0

        # Clean up old logs
        cutoff = datetime.now() - timedelta(days=self.retention_days)
        for log_file in self.audit_dir.glob("audit_*.jsonl"):
            try:
                file_date = datetime.strptime(log_file.stem.split("_", 1)[1], "%Y%m%d_%H%M%S")
                if file_date < cutoff:
                    log_file.unlink()
                    logger.info(f"Deleted old audit log: {log_file}")
            except ValueError:
                continue


# =============================================================================
# Anomaly Detector
# =============================================================================


class AnomalyDetector:
    """Detects anomalous access patterns.

    Monitors:
    - Failed login attempts
    - Unusual access times
    - Privilege escalation attempts
    - Data exfiltration patterns
    """

    def __init__(self, window_minutes: int = 60):
        """Initialize anomaly detector.

        Args:
            window_minutes: Time window for analysis
        """
        self.window_minutes = window_minutes
        self.events: List[Dict[str, Any]] = []
        self.thresholds = {
            "failed_logins": 5,
            "permission_denials": 10,
            "large_query_count": 100,
            "after_hours_access": True,
        }
        self._lock = threading.RLock()

    def record_event(self, event_type: str, principal: str, details: Dict[str, Any]) -> Optional[str]:
        """Record an event and check for anomalies.

        Args:
            event_type: Type of event
            principal: Principal involved
            details: Event details

        Returns:
            Anomaly description if detected, None otherwise
        """
        with self._lock:
            now = datetime.now()

            self.events.append({"timestamp": now, "type": event_type, "principal": principal, "details": details})

            # Clean old events
            cutoff = now - timedelta(minutes=self.window_minutes)
            self.events = [e for e in self.events if e["timestamp"] > cutoff]

            # Check for anomalies
            return self._check_anomalies(principal, now)

    def _check_anomalies(self, principal: str, now: datetime) -> Optional[str]:
        """Check for anomalies related to a principal."""
        # Count failed logins
        failed_logins = sum(
            1 for e in self.events if e["type"] == "failed_login" and e["principal"] == principal
        )
        if failed_logins >= self.thresholds["failed_logins"]:
            return f"Excessive failed logins ({failed_logins}) for {principal}"

        # Count permission denials
        denials = sum(
            1 for e in self.events if e["type"] == "permission_denied" and e["principal"] == principal
        )
        if denials >= self.thresholds["permission_denials"]:
            return f"Excessive permission denials ({denials}) for {principal}"

        # Check after-hours access
        if self.thresholds["after_hours_access"]:
            hour = now.hour
            if hour < 6 or hour > 22:  # Outside 6 AM - 10 PM
                recent_queries = sum(
                    1
                    for e in self.events
                    if e["type"] == "query" and e["principal"] == principal and (now - e["timestamp"]).seconds < 300
                )
                if recent_queries > 10:
                    return f"Unusual after-hours activity from {principal}"

        return None


# =============================================================================
# Policy Engine
# =============================================================================


class PolicyEngine:
    """Evaluates complex security policies."""

    def __init__(self):
        """Initialize policy engine."""
        self.policies: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    def add_policy(self, name: str, policy: Dict[str, Any]) -> None:
        """Add a security policy.

        Policy format:
        {
            "effect": "allow" | "deny",
            "principals": ["user1", "role:admin"],
            "actions": ["SELECT", "INSERT"],
            "resources": ["table:users", "table:orders"],
            "conditions": {...}
        }
        """
        with self._lock:
            self.policies[name] = policy
            logger.info(f"Added policy: {name}")

    def evaluate(self, principal: str, action: str, resource: str, context: Dict[str, Any]) -> Tuple[bool, str]:
        """Evaluate policies for an access request.

        Args:
            principal: Requesting principal
            action: Requested action
            resource: Target resource
            context: Request context

        Returns:
            (allowed, reason)
        """
        with self._lock:
            # Default deny
            result = False
            reason = "No matching policy"

            for policy_name, policy in self.policies.items():
                # Check if policy applies
                if not self._matches_principals(policy.get("principals", []), principal):
                    continue
                if not self._matches_actions(policy.get("actions", []), action):
                    continue
                if not self._matches_resources(policy.get("resources", []), resource):
                    continue
                if not self._matches_conditions(policy.get("conditions", {}), context):
                    continue

                # Policy applies
                effect = policy.get("effect", "deny")
                if effect == "deny":
                    return False, f"Denied by policy: {policy_name}"
                elif effect == "allow":
                    result = True
                    reason = f"Allowed by policy: {policy_name}"

            return result, reason

    def _matches_principals(self, patterns: List[str], principal: str) -> bool:
        """Check if principal matches any pattern."""
        if not patterns or "*" in patterns:
            return True
        return principal in patterns or f"user:{principal}" in patterns

    def _matches_actions(self, patterns: List[str], action: str) -> bool:
        """Check if action matches any pattern."""
        if not patterns or "*" in patterns:
            return True
        return action in patterns or action.upper() in patterns

    def _matches_resources(self, patterns: List[str], resource: str) -> bool:
        """Check if resource matches any pattern."""
        if not patterns or "*" in patterns:
            return True
        for pattern in patterns:
            if pattern == resource:
                return True
            if pattern.endswith("*") and resource.startswith(pattern[:-1]):
                return True
        return False

    def _matches_conditions(self, conditions: Dict[str, Any], context: Dict[str, Any]) -> bool:
        """Check if context matches conditions."""
        if not conditions:
            return True

        for key, expected in conditions.items():
            actual = context.get(key)

            if key.endswith("_in"):
                base_key = key[:-3]
                actual = context.get(base_key)
                if actual not in expected:
                    return False
            elif key.endswith("_not_in"):
                base_key = key[:-7]
                actual = context.get(base_key)
                if actual in expected:
                    return False
            elif actual != expected:
                return False

        return True


# =============================================================================
# Main Guardian Agent
# =============================================================================


class Guardian:
    """Guardian Agent - Security, access control, and audit.

    Core Responsibilities:
    1. Authentication and authorization
    2. Encryption key management
    3. Audit logging and compliance
    4. Anomaly detection

    Seed configuration is loaded from a YAML codex file.
    """

    # Core principle
    CORE_PRINCIPLE = "Trust but verify - protect data while enabling access."
    MORAL_CONSTANT = "Security without obstruction, protection without paranoia."

    def __init__(self, state_root: Path = DEFAULT_STATE_ROOT, seed_path: Optional[Path] = None):
        """Initialize Guardian agent.

        Args:
            state_root: Root directory for guardian state
            seed_path: Path to seed configuration YAML
        """
        self.state_root = state_root
        self.state_root.mkdir(parents=True, exist_ok=True)

        # Load seed configuration
        self.seed = self._load_seed(seed_path)

        # Initialize components
        self.password_hasher = PasswordHasher()
        self.encryption = EncryptionManager(state_root / DEFAULT_KEYS_DIR)
        self.access_control = AccessController(state_root)
        self.audit = AuditLogger(state_root / DEFAULT_AUDIT_DIR, retention_days=self.seed.get("audit_retention_days", 90))
        self.anomaly_detector = AnomalyDetector()
        self.policy_engine = PolicyEngine()

        # Initialize master key
        self.encryption.initialize_master_key()

        logger.info(f"Guardian agent initialized at {state_root}")
        logger.info(f"Core principle: {self.CORE_PRINCIPLE}")

    def _load_seed(self, seed_path: Optional[Path]) -> Dict[str, Any]:
        """Load seed configuration from YAML."""
        default_seed = {
            "audit_retention_days": 90,
            "password_min_length": 12,
            "password_require_special": True,
            "max_failed_logins": 5,
            "lockout_duration_minutes": 30,
            "session_timeout_minutes": 60,
            "key_rotation_days": 90,
            "mfa_required_for_admin": True,
        }

        if seed_path and seed_path.exists():
            with open(seed_path) as f:
                loaded = yaml.safe_load(f)
                default_seed.update(loaded)

        return default_seed

    def authenticate(self, username: str, password: str, client_ip: Optional[str] = None) -> Tuple[bool, Optional[str]]:
        """Authenticate a user.

        Args:
            username: Username
            password: Password
            client_ip: Client IP address

        Returns:
            (success, session_id or error message)
        """
        principal = self.access_control.principals.get(username)

        if not principal:
            self.audit.log(
                AuditAction.LOGIN,
                username,
                success=False,
                details={"reason": "unknown_user"},
                client_ip=client_ip,
            )
            return False, "Invalid credentials"

        if principal.locked:
            self.audit.log(
                AuditAction.LOGIN,
                username,
                success=False,
                details={"reason": "account_locked"},
                client_ip=client_ip,
            )
            return False, "Account is locked"

        if not principal.password_hash or not self.password_hasher.verify(password, principal.password_hash):
            principal.failed_attempts += 1
            if principal.failed_attempts >= self.seed.get("max_failed_logins", 5):
                principal.locked = True
                logger.warning(f"Account locked due to failed attempts: {username}")

            self.anomaly_detector.record_event("failed_login", username, {"client_ip": client_ip})
            self.audit.log(
                AuditAction.LOGIN,
                username,
                success=False,
                details={"reason": "invalid_password", "attempts": principal.failed_attempts},
                client_ip=client_ip,
            )
            return False, "Invalid credentials"

        # Success
        session_id = secrets.token_urlsafe(32)
        principal.failed_attempts = 0
        principal.last_login = datetime.now()

        self.audit.log(
            AuditAction.LOGIN,
            username,
            success=True,
            session_id=session_id,
            client_ip=client_ip,
        )

        return True, session_id

    def authorize(
        self,
        principal: str,
        permission: Permission,
        resource_type: ResourceType,
        resource_name: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Check authorization for an action.

        Args:
            principal: Principal requesting access
            permission: Required permission
            resource_type: Type of resource
            resource_name: Resource identifier
            context: Additional context

        Returns:
            True if authorized
        """
        allowed = self.access_control.check_permission(principal, permission, resource_type, resource_name, context)

        if not allowed:
            self.anomaly_detector.record_event(
                "permission_denied",
                principal,
                {
                    "permission": permission.name,
                    "resource_type": resource_type.name,
                    "resource_name": resource_name,
                },
            )

        return allowed

    def create_user(self, username: str, password: str, roles: Optional[Set[str]] = None) -> Principal:
        """Create a new user.

        Args:
            username: Username
            password: Password
            roles: Optional initial roles

        Returns:
            Created principal
        """
        # Validate password
        if len(password) < self.seed.get("password_min_length", 12):
            raise ValueError(f"Password must be at least {self.seed['password_min_length']} characters")

        if self.seed.get("password_require_special", True):
            if not any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password):
                raise ValueError("Password must contain at least one special character")

        principal = self.access_control.create_principal(username, "user")
        principal.password_hash = self.password_hasher.hash(password)
        if roles:
            principal.roles = roles

        self.audit.log(
            AuditAction.DDL,
            "system",
            success=True,
            resource=f"user:{username}",
            details={"action": "create_user", "roles": list(roles) if roles else []},
        )

        return principal

    def grant(
        self,
        principal: str,
        permissions: Set[Permission],
        resource_type: ResourceType,
        resource_name: str,
        granted_by: str,
    ) -> None:
        """Grant permissions."""
        self.access_control.grant(principal, permissions, resource_type, resource_name, granted_by)

        self.audit.log(
            AuditAction.GRANT,
            granted_by,
            success=True,
            resource=f"{resource_type.name}:{resource_name}",
            details={
                "grantee": principal,
                "permissions": [p.name for p in permissions],
            },
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get guardian statistics."""
        is_valid, verified = self.audit.verify_chain()

        return {
            "principals": len(self.access_control.principals),
            "grants": len(self.access_control.grants),
            "row_policies": len(self.access_control.row_policies),
            "column_masks": len(self.access_control.column_masks),
            "audit_chain_valid": is_valid,
            "audit_entries_verified": verified,
            "policies": len(self.policy_engine.policies),
        }


# =============================================================================
# CLI Interface
# =============================================================================


def create_cli():
    """Create CLI for Guardian agent."""
    import argparse

    parser = argparse.ArgumentParser(description="RoadDB Guardian Agent - Security and access control")

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Create user
    create_user_parser = subparsers.add_parser("create-user", help="Create a user")
    create_user_parser.add_argument("username", help="Username")
    create_user_parser.add_argument("--password", required=True, help="Password")
    create_user_parser.add_argument("--roles", nargs="*", help="Roles to assign")

    # Grant
    grant_parser = subparsers.add_parser("grant", help="Grant permissions")
    grant_parser.add_argument("principal", help="Principal name")
    grant_parser.add_argument("--permissions", nargs="+", required=True)
    grant_parser.add_argument("--on", required=True, help="Resource (format: type:name)")
    grant_parser.add_argument("--by", required=True, help="Granting principal")

    # Audit
    audit_parser = subparsers.add_parser("audit", help="Query audit log")
    audit_parser.add_argument("--principal", help="Filter by principal")
    audit_parser.add_argument("--action", help="Filter by action")
    audit_parser.add_argument("--limit", type=int, default=100)

    # Stats
    subparsers.add_parser("stats", help="Show guardian statistics")

    return parser


def main():
    """Main entry point."""
    parser = create_cli()
    args = parser.parse_args()

    guardian = Guardian()

    if args.command == "create-user":
        roles = set(args.roles) if args.roles else None
        guardian.create_user(args.username, args.password, roles)
        print(f"Created user: {args.username}")

    elif args.command == "grant":
        resource_parts = args.on.split(":")
        resource_type = ResourceType[resource_parts[0].upper()]
        resource_name = resource_parts[1]
        permissions = {Permission[p.upper()] for p in args.permissions}

        guardian.grant(args.principal, permissions, resource_type, resource_name, args.by)
        print(f"Granted {args.permissions} on {args.on} to {args.principal}")

    elif args.command == "audit":
        action = AuditAction[args.action.upper()] if args.action else None
        entries = guardian.audit.query(principal=args.principal, action=action, limit=args.limit)
        for entry in entries:
            print(f"{entry.timestamp} [{entry.action.name}] {entry.principal}: {entry.details}")

    elif args.command == "stats":
        stats = guardian.get_stats()
        print(f"Principals: {stats['principals']}")
        print(f"Grants: {stats['grants']}")
        print(f"Row policies: {stats['row_policies']}")
        print(f"Audit chain valid: {stats['audit_chain_valid']}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()


__all__ = [
    "Guardian",
    "Principal",
    "Permission",
    "ResourceType",
    "AccessGrant",
    "AuditEntry",
    "AuditAction",
    "RowLevelPolicy",
    "ColumnMask",
    "EncryptionManager",
    "AccessController",
    "AuditLogger",
    "AnomalyDetector",
    "PolicyEngine",
    "PasswordHasher",
]
