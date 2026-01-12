"""RoadDB Schema - Schema definition and management.

Provides a declarative way to define database schemas including:
- Table definitions with columns and constraints
- Index definitions
- Foreign key relationships
- Schema migrations
- Schema validation

Architecture:
    ┌─────────────────────────────────────────────────────────────────────┐
    │                        Schema System                               │
    ├─────────────────────────────────────────────────────────────────────┤
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
    │  │   Table     │  │   Column    │  │   Index     │                 │
    │  │ Definition  │──│ Definition  │──│ Definition  │                 │
    │  └─────────────┘  └─────────────┘  └─────────────┘                 │
    │         │               │               │                           │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
    │  │ Constraint  │  │  Migration  │  │   Schema    │                 │
    │  │ Definition  │──│   Manager   │──│  Validator  │                 │
    │  └─────────────┘  └─────────────┘  └─────────────┘                 │
    └─────────────────────────────────────────────────────────────────────┘

Usage:
    from roaddb_core.schema import Schema, Column, Table, Index
    from roaddb_core.types import Integer, String, DateTime

    users = Table(
        "users",
        Column("id", Integer(primary_key=True, auto_increment=True)),
        Column("email", String(255), unique=True, nullable=False),
        Column("created_at", DateTime(auto_now_add=True)),
    )

    schema = Schema([users])
    schema.create_all(db)

Copyright (c) 2024-2026 BlackRoad OS, Inc. All rights reserved.
"""

from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, Union

from roaddb_core.types import DataType, Integer, String, DateTime, Boolean, SQLDialect


# =============================================================================
# Constraint Types
# =============================================================================


class ConstraintType(Enum):
    """Types of constraints."""

    PRIMARY_KEY = auto()
    FOREIGN_KEY = auto()
    UNIQUE = auto()
    CHECK = auto()
    NOT_NULL = auto()
    DEFAULT = auto()
    INDEX = auto()


class OnAction(Enum):
    """Referential action for foreign keys."""

    CASCADE = "CASCADE"
    RESTRICT = "RESTRICT"
    SET_NULL = "SET NULL"
    SET_DEFAULT = "SET DEFAULT"
    NO_ACTION = "NO ACTION"


# =============================================================================
# Column Definition
# =============================================================================


@dataclass
class Column:
    """Column definition.

    Defines a column in a table with its type and constraints.
    """

    name: str
    type: DataType
    primary_key: bool = False
    nullable: bool = True
    unique: bool = False
    default: Any = None
    index: bool = False
    comment: Optional[str] = None

    # For foreign keys
    references: Optional[str] = None  # "table.column"
    on_delete: OnAction = OnAction.RESTRICT
    on_update: OnAction = OnAction.CASCADE

    # Computed columns
    computed: Optional[str] = None  # SQL expression
    stored: bool = False  # Whether computed column is stored

    def __post_init__(self):
        # Apply type constraints
        if hasattr(self.type, "nullable"):
            if not self.nullable:
                self.type.nullable = False

        if hasattr(self.type, "default") and self.default is not None:
            self.type.default = self.default

    def sql(self, dialect: SQLDialect = SQLDialect.STANDARD) -> str:
        """Generate SQL column definition.

        Args:
            dialect: Target SQL dialect

        Returns:
            SQL column definition string
        """
        parts = [f'"{self.name}"', self.type.sql_type(dialect)]

        if self.computed:
            if self.stored:
                parts.append(f"GENERATED ALWAYS AS ({self.computed}) STORED")
            else:
                parts.append(f"GENERATED ALWAYS AS ({self.computed}) VIRTUAL")
        else:
            if self.primary_key:
                parts.append("PRIMARY KEY")

            if not self.nullable and not self.primary_key:
                parts.append("NOT NULL")

            if self.unique and not self.primary_key:
                parts.append("UNIQUE")

            if self.default is not None:
                default_sql = self._format_default(self.default)
                parts.append(f"DEFAULT {default_sql}")

            if self.references:
                table, column = self.references.split(".")
                parts.append(f"REFERENCES {table}({column})")
                parts.append(f"ON DELETE {self.on_delete.value}")
                parts.append(f"ON UPDATE {self.on_update.value}")

        return " ".join(parts)

    def _format_default(self, value: Any) -> str:
        """Format default value for SQL."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, str):
            if value.upper() in ("CURRENT_TIMESTAMP", "CURRENT_DATE", "NOW()"):
                return value.upper()
            return f"'{value}'"
        return f"'{value}'"

    def validate(self, value: Any) -> List[str]:
        """Validate a value against this column.

        Args:
            value: Value to validate

        Returns:
            List of error messages
        """
        errors = []

        if value is None:
            if not self.nullable:
                errors.append(f"Column {self.name} does not allow NULL")
            return errors

        type_errors = self.type.validate(value)
        for error in type_errors:
            errors.append(f"Column {self.name}: {error.message}")

        return errors


# =============================================================================
# Table Constraints
# =============================================================================


@dataclass
class PrimaryKey:
    """Primary key constraint."""

    columns: List[str]
    name: Optional[str] = None

    def sql(self, table_name: str) -> str:
        """Generate SQL."""
        constraint_name = self.name or f"pk_{table_name}"
        cols = ", ".join(f'"{c}"' for c in self.columns)
        return f'CONSTRAINT "{constraint_name}" PRIMARY KEY ({cols})'


@dataclass
class ForeignKey:
    """Foreign key constraint."""

    columns: List[str]
    references_table: str
    references_columns: List[str]
    name: Optional[str] = None
    on_delete: OnAction = OnAction.RESTRICT
    on_update: OnAction = OnAction.CASCADE

    def sql(self, table_name: str) -> str:
        """Generate SQL."""
        constraint_name = self.name or f"fk_{table_name}_{'_'.join(self.columns)}"
        cols = ", ".join(f'"{c}"' for c in self.columns)
        ref_cols = ", ".join(f'"{c}"' for c in self.references_columns)

        return (
            f'CONSTRAINT "{constraint_name}" FOREIGN KEY ({cols}) '
            f"REFERENCES {self.references_table}({ref_cols}) "
            f"ON DELETE {self.on_delete.value} ON UPDATE {self.on_update.value}"
        )


@dataclass
class UniqueConstraint:
    """Unique constraint."""

    columns: List[str]
    name: Optional[str] = None

    def sql(self, table_name: str) -> str:
        """Generate SQL."""
        constraint_name = self.name or f"uq_{table_name}_{'_'.join(self.columns)}"
        cols = ", ".join(f'"{c}"' for c in self.columns)
        return f'CONSTRAINT "{constraint_name}" UNIQUE ({cols})'


@dataclass
class CheckConstraint:
    """Check constraint."""

    expression: str
    name: Optional[str] = None

    def sql(self, table_name: str) -> str:
        """Generate SQL."""
        constraint_name = self.name or f"ck_{table_name}"
        return f'CONSTRAINT "{constraint_name}" CHECK ({self.expression})'


# =============================================================================
# Index Definition
# =============================================================================


@dataclass
class Index:
    """Index definition."""

    name: str
    table: str
    columns: List[str]
    unique: bool = False
    where: Optional[str] = None  # Partial index condition
    include: Optional[List[str]] = None  # Covering index columns
    using: str = "btree"  # Index type (btree, hash, gin, gist)
    concurrently: bool = False

    def sql(self, dialect: SQLDialect = SQLDialect.STANDARD) -> str:
        """Generate SQL CREATE INDEX statement."""
        parts = ["CREATE"]

        if self.unique:
            parts.append("UNIQUE")

        parts.append("INDEX")

        if self.concurrently and dialect == SQLDialect.POSTGRESQL:
            parts.append("CONCURRENTLY")

        parts.append(f'"{self.name}"')
        parts.append(f'ON "{self.table}"')

        if dialect == SQLDialect.POSTGRESQL and self.using != "btree":
            parts.append(f"USING {self.using}")

        cols = ", ".join(f'"{c}"' for c in self.columns)
        parts.append(f"({cols})")

        if self.include and dialect == SQLDialect.POSTGRESQL:
            include_cols = ", ".join(f'"{c}"' for c in self.include)
            parts.append(f"INCLUDE ({include_cols})")

        if self.where:
            parts.append(f"WHERE {self.where}")

        return " ".join(parts)

    def drop_sql(self, dialect: SQLDialect = SQLDialect.STANDARD) -> str:
        """Generate SQL DROP INDEX statement."""
        if self.concurrently and dialect == SQLDialect.POSTGRESQL:
            return f'DROP INDEX CONCURRENTLY IF EXISTS "{self.name}"'
        return f'DROP INDEX IF EXISTS "{self.name}"'


# =============================================================================
# Table Definition
# =============================================================================


@dataclass
class Table:
    """Table definition.

    Defines a database table with columns, constraints, and indexes.
    """

    name: str
    columns: List[Column] = field(default_factory=list)
    primary_key: Optional[PrimaryKey] = None
    foreign_keys: List[ForeignKey] = field(default_factory=list)
    unique_constraints: List[UniqueConstraint] = field(default_factory=list)
    check_constraints: List[CheckConstraint] = field(default_factory=list)
    indexes: List[Index] = field(default_factory=list)
    schema: Optional[str] = None
    comment: Optional[str] = None

    def __init__(self, name: str, *columns: Column, schema: Optional[str] = None, comment: Optional[str] = None):
        """Initialize table.

        Args:
            name: Table name
            *columns: Column definitions
            schema: Schema name
            comment: Table comment
        """
        self.name = name
        self.columns = list(columns)
        self.schema = schema
        self.comment = comment
        self.primary_key = None
        self.foreign_keys = []
        self.unique_constraints = []
        self.check_constraints = []
        self.indexes = []

        # Extract primary key from columns
        pk_columns = [c.name for c in self.columns if c.primary_key]
        if pk_columns:
            self.primary_key = PrimaryKey(pk_columns)

        # Extract unique constraints from columns
        for col in self.columns:
            if col.unique and not col.primary_key:
                self.unique_constraints.append(UniqueConstraint([col.name]))

        # Extract indexes from columns
        for col in self.columns:
            if col.index:
                self.indexes.append(Index(f"idx_{name}_{col.name}", name, [col.name]))

    @property
    def full_name(self) -> str:
        """Get fully qualified table name."""
        if self.schema:
            return f"{self.schema}.{self.name}"
        return self.name

    def add_column(self, column: Column) -> Table:
        """Add a column to the table."""
        self.columns.append(column)
        return self

    def add_foreign_key(self, fk: ForeignKey) -> Table:
        """Add a foreign key constraint."""
        self.foreign_keys.append(fk)
        return self

    def add_unique(self, *columns: str, name: Optional[str] = None) -> Table:
        """Add a unique constraint."""
        self.unique_constraints.append(UniqueConstraint(list(columns), name))
        return self

    def add_check(self, expression: str, name: Optional[str] = None) -> Table:
        """Add a check constraint."""
        self.check_constraints.append(CheckConstraint(expression, name))
        return self

    def add_index(self, *columns: str, name: Optional[str] = None, unique: bool = False) -> Table:
        """Add an index."""
        index_name = name or f"idx_{self.name}_{'_'.join(columns)}"
        self.indexes.append(Index(index_name, self.name, list(columns), unique=unique))
        return self

    def get_column(self, name: str) -> Optional[Column]:
        """Get column by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def create_sql(self, dialect: SQLDialect = SQLDialect.STANDARD, if_not_exists: bool = True) -> str:
        """Generate SQL CREATE TABLE statement.

        Args:
            dialect: Target SQL dialect
            if_not_exists: Add IF NOT EXISTS clause

        Returns:
            SQL CREATE TABLE statement
        """
        parts = ["CREATE TABLE"]

        if if_not_exists:
            parts.append("IF NOT EXISTS")

        parts.append(f'"{self.full_name}"')
        parts.append("(")

        # Columns
        column_defs = [col.sql(dialect) for col in self.columns]

        # Table-level primary key (for composite keys)
        if self.primary_key and len(self.primary_key.columns) > 1:
            column_defs.append(self.primary_key.sql(self.name))

        # Foreign keys
        for fk in self.foreign_keys:
            column_defs.append(fk.sql(self.name))

        # Unique constraints (for composite unique)
        for uq in self.unique_constraints:
            if len(uq.columns) > 1:
                column_defs.append(uq.sql(self.name))

        # Check constraints
        for ck in self.check_constraints:
            column_defs.append(ck.sql(self.name))

        parts.append("    " + ",\n    ".join(column_defs))
        parts.append(")")

        return "\n".join(parts)

    def drop_sql(self, dialect: SQLDialect = SQLDialect.STANDARD, if_exists: bool = True, cascade: bool = False) -> str:
        """Generate SQL DROP TABLE statement."""
        parts = ["DROP TABLE"]

        if if_exists:
            parts.append("IF EXISTS")

        parts.append(f'"{self.full_name}"')

        if cascade:
            parts.append("CASCADE")

        return " ".join(parts)

    def alter_add_column_sql(self, column: Column, dialect: SQLDialect = SQLDialect.STANDARD) -> str:
        """Generate SQL ALTER TABLE ADD COLUMN."""
        return f'ALTER TABLE "{self.full_name}" ADD COLUMN {column.sql(dialect)}'

    def alter_drop_column_sql(self, column_name: str, dialect: SQLDialect = SQLDialect.STANDARD) -> str:
        """Generate SQL ALTER TABLE DROP COLUMN."""
        return f'ALTER TABLE "{self.full_name}" DROP COLUMN "{column_name}"'

    def validate_row(self, row: Dict[str, Any]) -> List[str]:
        """Validate a row against this table's schema.

        Args:
            row: Dictionary of column name -> value

        Returns:
            List of error messages
        """
        errors = []

        # Check for required columns
        for col in self.columns:
            if col.name not in row and not col.nullable and col.default is None and not col.computed:
                errors.append(f"Missing required column: {col.name}")

        # Validate values
        for col_name, value in row.items():
            col = self.get_column(col_name)
            if col:
                errors.extend(col.validate(value))
            else:
                errors.append(f"Unknown column: {col_name}")

        return errors

    def to_dict(self) -> Dict[str, Any]:
        """Convert table definition to dictionary."""
        return {
            "name": self.name,
            "schema": self.schema,
            "comment": self.comment,
            "columns": [
                {
                    "name": c.name,
                    "type": c.type.__class__.__name__,
                    "nullable": c.nullable,
                    "primary_key": c.primary_key,
                    "unique": c.unique,
                    "default": c.default,
                }
                for c in self.columns
            ],
            "indexes": [{"name": i.name, "columns": i.columns, "unique": i.unique} for i in self.indexes],
        }


# =============================================================================
# Schema Definition
# =============================================================================


@dataclass
class Schema:
    """Database schema definition.

    Contains multiple tables and their relationships.
    """

    tables: List[Table] = field(default_factory=list)
    name: Optional[str] = None
    version: str = "1.0.0"

    def __init__(self, tables: Optional[List[Table]] = None, name: Optional[str] = None, version: str = "1.0.0"):
        """Initialize schema.

        Args:
            tables: List of tables
            name: Schema name
            version: Schema version
        """
        self.tables = tables or []
        self.name = name
        self.version = version

    def add_table(self, table: Table) -> Schema:
        """Add a table to the schema."""
        self.tables.append(table)
        return self

    def get_table(self, name: str) -> Optional[Table]:
        """Get table by name."""
        for table in self.tables:
            if table.name == name:
                return table
        return None

    def create_all_sql(self, dialect: SQLDialect = SQLDialect.STANDARD) -> List[str]:
        """Generate SQL to create all tables.

        Args:
            dialect: Target SQL dialect

        Returns:
            List of SQL statements
        """
        statements = []

        # Create schema if named
        if self.name and dialect == SQLDialect.POSTGRESQL:
            statements.append(f'CREATE SCHEMA IF NOT EXISTS "{self.name}"')

        # Create tables in dependency order
        for table in self._sorted_tables():
            statements.append(table.create_sql(dialect))

            # Create indexes
            for index in table.indexes:
                statements.append(index.sql(dialect))

        return statements

    def drop_all_sql(self, dialect: SQLDialect = SQLDialect.STANDARD) -> List[str]:
        """Generate SQL to drop all tables.

        Args:
            dialect: Target SQL dialect

        Returns:
            List of SQL statements
        """
        statements = []

        # Drop tables in reverse dependency order
        for table in reversed(self._sorted_tables()):
            statements.append(table.drop_sql(dialect, cascade=True))

        return statements

    def _sorted_tables(self) -> List[Table]:
        """Sort tables by dependencies (foreign keys)."""
        # Simple topological sort
        sorted_tables = []
        remaining = set(t.name for t in self.tables)

        while remaining:
            # Find tables with no remaining dependencies
            for table in self.tables:
                if table.name not in remaining:
                    continue

                deps = set()
                for fk in table.foreign_keys:
                    deps.add(fk.references_table)

                if not deps & remaining:
                    sorted_tables.append(table)
                    remaining.remove(table.name)
                    break
            else:
                # Circular dependency - add remaining in order
                for table in self.tables:
                    if table.name in remaining:
                        sorted_tables.append(table)
                        remaining.remove(table.name)

        return sorted_tables

    def fingerprint(self) -> str:
        """Generate a unique fingerprint for this schema."""
        schema_dict = {
            "version": self.version,
            "tables": [t.to_dict() for t in self.tables],
        }
        json_str = json.dumps(schema_dict, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()[:16]

    def diff(self, other: Schema) -> List[str]:
        """Compare with another schema and return differences.

        Args:
            other: Schema to compare with

        Returns:
            List of difference descriptions
        """
        differences = []

        self_tables = {t.name: t for t in self.tables}
        other_tables = {t.name: t for t in other.tables}

        # Tables only in self
        for name in self_tables:
            if name not in other_tables:
                differences.append(f"Table added: {name}")

        # Tables only in other
        for name in other_tables:
            if name not in self_tables:
                differences.append(f"Table removed: {name}")

        # Tables in both - compare columns
        for name in self_tables:
            if name not in other_tables:
                continue

            self_table = self_tables[name]
            other_table = other_tables[name]

            self_cols = {c.name: c for c in self_table.columns}
            other_cols = {c.name: c for c in other_table.columns}

            for col_name in self_cols:
                if col_name not in other_cols:
                    differences.append(f"Column added: {name}.{col_name}")

            for col_name in other_cols:
                if col_name not in self_cols:
                    differences.append(f"Column removed: {name}.{col_name}")

        return differences


# =============================================================================
# Migration System
# =============================================================================


@dataclass
class Migration:
    """Database migration."""

    version: str
    description: str
    up_sql: List[str]
    down_sql: List[str]
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "version": self.version,
            "description": self.description,
            "created_at": self.created_at.isoformat(),
            "up_sql": self.up_sql,
            "down_sql": self.down_sql,
        }


class MigrationManager:
    """Manages database migrations."""

    MIGRATIONS_TABLE = "_migrations"

    def __init__(self, migrations_dir: Path):
        """Initialize migration manager.

        Args:
            migrations_dir: Directory for migration files
        """
        self.migrations_dir = migrations_dir
        self.migrations_dir.mkdir(parents=True, exist_ok=True)
        self.migrations: List[Migration] = []

    def create_migration(self, description: str, up_sql: List[str], down_sql: List[str]) -> Migration:
        """Create a new migration.

        Args:
            description: Migration description
            up_sql: SQL statements for upgrade
            down_sql: SQL statements for rollback

        Returns:
            Created migration
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        version = f"{timestamp}_{description.lower().replace(' ', '_')}"

        migration = Migration(version=version, description=description, up_sql=up_sql, down_sql=down_sql)

        # Save to file
        migration_file = self.migrations_dir / f"{version}.json"
        with open(migration_file, "w") as f:
            json.dump(migration.to_dict(), f, indent=2)

        self.migrations.append(migration)
        return migration

    def load_migrations(self) -> List[Migration]:
        """Load all migrations from disk."""
        self.migrations = []

        for path in sorted(self.migrations_dir.glob("*.json")):
            with open(path) as f:
                data = json.load(f)

            migration = Migration(
                version=data["version"],
                description=data["description"],
                up_sql=data["up_sql"],
                down_sql=data["down_sql"],
                created_at=datetime.fromisoformat(data["created_at"]),
            )
            self.migrations.append(migration)

        return self.migrations

    def get_applied_migrations(self, db: Any) -> Set[str]:
        """Get set of applied migration versions.

        Args:
            db: Database connection

        Returns:
            Set of applied migration versions
        """
        try:
            result = db.execute(f"SELECT version FROM {self.MIGRATIONS_TABLE}")
            return {row[0] for row in result.rows or []}
        except Exception:
            return set()

    def get_pending_migrations(self, db: Any) -> List[Migration]:
        """Get list of pending migrations.

        Args:
            db: Database connection

        Returns:
            List of pending migrations
        """
        applied = self.get_applied_migrations(db)
        return [m for m in self.migrations if m.version not in applied]

    def apply_migration(self, db: Any, migration: Migration) -> None:
        """Apply a migration.

        Args:
            db: Database connection
            migration: Migration to apply
        """
        # Execute up SQL
        for sql in migration.up_sql:
            db.execute(sql)

        # Record migration
        db.execute(
            f"INSERT INTO {self.MIGRATIONS_TABLE} (version, applied_at) VALUES (?, ?)",
            (migration.version, datetime.now().isoformat()),
        )

    def rollback_migration(self, db: Any, migration: Migration) -> None:
        """Rollback a migration.

        Args:
            db: Database connection
            migration: Migration to rollback
        """
        # Execute down SQL
        for sql in migration.down_sql:
            db.execute(sql)

        # Remove migration record
        db.execute(f"DELETE FROM {self.MIGRATIONS_TABLE} WHERE version = ?", (migration.version,))

    def ensure_migrations_table(self, db: Any) -> None:
        """Ensure migrations tracking table exists.

        Args:
            db: Database connection
        """
        db.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.MIGRATIONS_TABLE} (
                version TEXT PRIMARY KEY,
                applied_at TEXT NOT NULL
            )
        """)


# =============================================================================
# Schema Validator
# =============================================================================


class SchemaValidator:
    """Validates schema definitions."""

    RESERVED_WORDS = {
        "select", "from", "where", "insert", "update", "delete", "create",
        "table", "index", "view", "drop", "alter", "add", "column", "primary",
        "key", "foreign", "references", "unique", "check", "default",
        "constraint", "null", "not", "and", "or", "in", "like", "between",
        "order", "by", "group", "having", "limit", "offset", "join", "left",
        "right", "inner", "outer", "full", "cross", "on", "as",
    }

    def validate_table(self, table: Table) -> List[str]:
        """Validate a table definition.

        Args:
            table: Table to validate

        Returns:
            List of validation errors
        """
        errors = []

        # Check table name
        if not table.name:
            errors.append("Table name is required")
        elif table.name.lower() in self.RESERVED_WORDS:
            errors.append(f"Table name '{table.name}' is a reserved word")

        # Check columns
        if not table.columns:
            errors.append("Table must have at least one column")

        column_names = set()
        for col in table.columns:
            if not col.name:
                errors.append("Column name is required")
            elif col.name.lower() in self.RESERVED_WORDS:
                errors.append(f"Column name '{col.name}' is a reserved word")
            elif col.name in column_names:
                errors.append(f"Duplicate column name: {col.name}")
            else:
                column_names.add(col.name)

        # Check for primary key
        pk_columns = [c for c in table.columns if c.primary_key]
        if not pk_columns and table.primary_key is None:
            errors.append("Table should have a primary key")

        # Check foreign key references
        for fk in table.foreign_keys:
            for col in fk.columns:
                if col not in column_names:
                    errors.append(f"Foreign key references unknown column: {col}")

        return errors

    def validate_schema(self, schema: Schema) -> List[str]:
        """Validate entire schema.

        Args:
            schema: Schema to validate

        Returns:
            List of validation errors
        """
        errors = []

        table_names = set()
        for table in schema.tables:
            # Check for duplicate tables
            if table.name in table_names:
                errors.append(f"Duplicate table name: {table.name}")
            else:
                table_names.add(table.name)

            # Validate each table
            errors.extend(self.validate_table(table))

        # Check foreign key references exist
        for table in schema.tables:
            for fk in table.foreign_keys:
                if fk.references_table not in table_names:
                    errors.append(f"Foreign key references unknown table: {fk.references_table}")

        return errors


__all__ = [
    # Core
    "Schema",
    "Table",
    "Column",
    "Index",
    # Constraints
    "PrimaryKey",
    "ForeignKey",
    "UniqueConstraint",
    "CheckConstraint",
    "ConstraintType",
    "OnAction",
    # Migration
    "Migration",
    "MigrationManager",
    # Validation
    "SchemaValidator",
]
