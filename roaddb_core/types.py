"""RoadDB Types - Data type definitions and validation.

Provides a comprehensive type system for database columns including:
- Primitive types (Integer, String, Float, Boolean)
- Complex types (JSON, Array, UUID)
- Temporal types (DateTime, Timestamp, Date, Time, Interval)
- Binary types (Binary, Blob)
- Numeric types (Decimal, BigInteger)
- Special types (Enum, Composite, Range)

Each type supports:
- Validation constraints
- Serialization/deserialization
- SQL type mapping
- Python type coercion

Copyright (c) 2024-2026 BlackRoad OS, Inc. All rights reserved.
"""

from __future__ import annotations

import base64
import json
import re
import struct
import uuid as uuid_module
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal as PyDecimal
from decimal import InvalidOperation
from enum import Enum as PyEnum
from typing import Any, Callable, Dict, Generic, List, Optional, Set, Tuple, Type, TypeVar, Union

# Type variable for generic type operations
T = TypeVar("T")


class SQLDialect(PyEnum):
    """Supported SQL dialects for type mapping."""

    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    MSSQL = "mssql"
    ORACLE = "oracle"
    STANDARD = "standard"


class TypeCategory(PyEnum):
    """Categories of data types."""

    NUMERIC = "numeric"
    STRING = "string"
    BINARY = "binary"
    TEMPORAL = "temporal"
    BOOLEAN = "boolean"
    COMPOSITE = "composite"
    SPECIAL = "special"


@dataclass
class ValidationError:
    """Validation error details."""

    field: str
    message: str
    value: Any
    constraint: Optional[str] = None


class DataType(ABC):
    """Base class for all RoadDB data types.

    All types must implement:
    - validate(): Check if a value conforms to the type
    - serialize(): Convert Python value to storage format
    - deserialize(): Convert storage format to Python value
    - sql_type(): Get SQL type declaration for dialect
    """

    category: TypeCategory = TypeCategory.SPECIAL
    nullable: bool = True
    default: Any = None

    @abstractmethod
    def validate(self, value: Any) -> List[ValidationError]:
        """Validate a value against this type.

        Args:
            value: Value to validate

        Returns:
            List of validation errors (empty if valid)
        """
        pass

    @abstractmethod
    def serialize(self, value: Any) -> Any:
        """Serialize value for storage.

        Args:
            value: Python value

        Returns:
            Storage-compatible value
        """
        pass

    @abstractmethod
    def deserialize(self, value: Any) -> Any:
        """Deserialize value from storage.

        Args:
            value: Stored value

        Returns:
            Python value
        """
        pass

    @abstractmethod
    def sql_type(self, dialect: SQLDialect = SQLDialect.STANDARD) -> str:
        """Get SQL type declaration.

        Args:
            dialect: Target SQL dialect

        Returns:
            SQL type string
        """
        pass

    def python_type(self) -> Type:
        """Get corresponding Python type."""
        return object

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"


# =============================================================================
# Numeric Types
# =============================================================================


class Integer(DataType):
    """Integer type with optional size and constraints.

    Supports:
    - TINYINT (1 byte, -128 to 127)
    - SMALLINT (2 bytes, -32768 to 32767)
    - INTEGER (4 bytes, standard int)
    - BIGINT (8 bytes, large integers)
    """

    category = TypeCategory.NUMERIC

    def __init__(
        self,
        *,
        size: str = "integer",
        unsigned: bool = False,
        primary_key: bool = False,
        auto_increment: bool = False,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
        nullable: bool = True,
        default: Optional[int] = None,
    ):
        """Initialize Integer type.

        Args:
            size: tinyint, smallint, integer, bigint
            unsigned: Whether unsigned (no negative values)
            primary_key: Whether this is a primary key
            auto_increment: Whether to auto-increment
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            nullable: Whether NULL is allowed
            default: Default value
        """
        self.size = size.lower()
        self.unsigned = unsigned
        self.primary_key = primary_key
        self.auto_increment = auto_increment
        self.nullable = nullable
        self.default = default

        # Set bounds based on size
        size_bounds = {
            "tinyint": (-128, 127, 0, 255),
            "smallint": (-32768, 32767, 0, 65535),
            "integer": (-2147483648, 2147483647, 0, 4294967295),
            "bigint": (-9223372036854775808, 9223372036854775807, 0, 18446744073709551615),
        }

        bounds = size_bounds.get(self.size, size_bounds["integer"])
        if unsigned:
            self.min_bound = bounds[2]
            self.max_bound = bounds[3]
        else:
            self.min_bound = bounds[0]
            self.max_bound = bounds[1]

        # Apply custom constraints
        self.min_value = max(min_value, self.min_bound) if min_value is not None else self.min_bound
        self.max_value = min(max_value, self.max_bound) if max_value is not None else self.max_bound

    def validate(self, value: Any) -> List[ValidationError]:
        errors = []

        if value is None:
            if not self.nullable:
                errors.append(ValidationError("value", "NULL not allowed", value, "not_null"))
            return errors

        if not isinstance(value, (int, float)):
            errors.append(ValidationError("value", f"Expected integer, got {type(value).__name__}", value, "type"))
            return errors

        if isinstance(value, float) and not value.is_integer():
            errors.append(ValidationError("value", "Float value has fractional part", value, "type"))
            return errors

        int_value = int(value)
        if int_value < self.min_value:
            errors.append(ValidationError("value", f"Value {int_value} below minimum {self.min_value}", value, "min"))
        if int_value > self.max_value:
            errors.append(ValidationError("value", f"Value {int_value} above maximum {self.max_value}", value, "max"))

        return errors

    def serialize(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        return int(value)

    def deserialize(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        return int(value)

    def sql_type(self, dialect: SQLDialect = SQLDialect.STANDARD) -> str:
        type_map = {
            "tinyint": "TINYINT",
            "smallint": "SMALLINT",
            "integer": "INTEGER",
            "bigint": "BIGINT",
        }

        sql = type_map.get(self.size, "INTEGER")

        if dialect == SQLDialect.POSTGRESQL and self.unsigned:
            # PostgreSQL doesn't have unsigned, use CHECK constraint
            pass
        elif self.unsigned and dialect in (SQLDialect.MYSQL,):
            sql += " UNSIGNED"

        if self.auto_increment:
            if dialect == SQLDialect.POSTGRESQL:
                sql = "SERIAL" if self.size == "integer" else "BIGSERIAL"
            elif dialect == SQLDialect.SQLITE:
                sql += " AUTOINCREMENT"
            else:
                sql += " AUTO_INCREMENT"

        return sql

    def python_type(self) -> Type:
        return int

    def __repr__(self) -> str:
        parts = [self.size.upper()]
        if self.unsigned:
            parts.append("UNSIGNED")
        if self.primary_key:
            parts.append("PRIMARY KEY")
        if self.auto_increment:
            parts.append("AUTO_INCREMENT")
        return f"Integer({', '.join(parts)})"


class BigInteger(Integer):
    """Convenience class for BIGINT type."""

    def __init__(self, **kwargs):
        kwargs["size"] = "bigint"
        super().__init__(**kwargs)


class Float(DataType):
    """Floating-point number type.

    Supports:
    - FLOAT (single precision, 4 bytes)
    - DOUBLE (double precision, 8 bytes)
    - REAL (implementation-defined)
    """

    category = TypeCategory.NUMERIC

    def __init__(
        self,
        *,
        precision: str = "double",
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        nullable: bool = True,
        default: Optional[float] = None,
    ):
        """Initialize Float type.

        Args:
            precision: float, double, real
            min_value: Minimum allowed value
            max_value: Maximum allowed value
            nullable: Whether NULL is allowed
            default: Default value
        """
        self.precision = precision.lower()
        self.min_value = min_value
        self.max_value = max_value
        self.nullable = nullable
        self.default = default

    def validate(self, value: Any) -> List[ValidationError]:
        errors = []

        if value is None:
            if not self.nullable:
                errors.append(ValidationError("value", "NULL not allowed", value, "not_null"))
            return errors

        if not isinstance(value, (int, float)):
            errors.append(ValidationError("value", f"Expected float, got {type(value).__name__}", value, "type"))
            return errors

        float_value = float(value)
        if self.min_value is not None and float_value < self.min_value:
            errors.append(ValidationError("value", f"Value below minimum {self.min_value}", value, "min"))
        if self.max_value is not None and float_value > self.max_value:
            errors.append(ValidationError("value", f"Value above maximum {self.max_value}", value, "max"))

        return errors

    def serialize(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        return float(value)

    def deserialize(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        return float(value)

    def sql_type(self, dialect: SQLDialect = SQLDialect.STANDARD) -> str:
        type_map = {
            "float": "FLOAT",
            "double": "DOUBLE PRECISION",
            "real": "REAL",
        }
        return type_map.get(self.precision, "DOUBLE PRECISION")

    def python_type(self) -> Type:
        return float


class Decimal(DataType):
    """Fixed-precision decimal type for financial calculations.

    Stores exact decimal values without floating-point errors.
    """

    category = TypeCategory.NUMERIC

    def __init__(
        self,
        precision: int = 10,
        scale: int = 2,
        *,
        nullable: bool = True,
        default: Optional[PyDecimal] = None,
    ):
        """Initialize Decimal type.

        Args:
            precision: Total number of digits
            scale: Number of decimal places
            nullable: Whether NULL is allowed
            default: Default value
        """
        self.precision = precision
        self.scale = scale
        self.nullable = nullable
        self.default = default

    def validate(self, value: Any) -> List[ValidationError]:
        errors = []

        if value is None:
            if not self.nullable:
                errors.append(ValidationError("value", "NULL not allowed", value, "not_null"))
            return errors

        try:
            dec = PyDecimal(str(value))
        except InvalidOperation:
            errors.append(ValidationError("value", f"Cannot convert to Decimal: {value}", value, "type"))
            return errors

        # Check precision
        sign, digits, exponent = dec.as_tuple()
        total_digits = len(digits)
        if exponent < 0:
            decimal_places = -exponent
        else:
            decimal_places = 0

        if total_digits > self.precision:
            errors.append(ValidationError("value", f"Too many digits (max {self.precision})", value, "precision"))
        if decimal_places > self.scale:
            errors.append(ValidationError("value", f"Too many decimal places (max {self.scale})", value, "scale"))

        return errors

    def serialize(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        return str(PyDecimal(str(value)))

    def deserialize(self, value: Any) -> Optional[PyDecimal]:
        if value is None:
            return None
        return PyDecimal(str(value))

    def sql_type(self, dialect: SQLDialect = SQLDialect.STANDARD) -> str:
        return f"DECIMAL({self.precision}, {self.scale})"

    def python_type(self) -> Type:
        return PyDecimal

    def __repr__(self) -> str:
        return f"Decimal({self.precision}, {self.scale})"


# =============================================================================
# String Types
# =============================================================================


class String(DataType):
    """Variable-length string type.

    Supports:
    - VARCHAR (variable length with max)
    - CHAR (fixed length)
    - TEXT (unlimited length)
    """

    category = TypeCategory.STRING

    def __init__(
        self,
        length: Optional[int] = None,
        *,
        fixed: bool = False,
        collation: Optional[str] = None,
        pattern: Optional[str] = None,
        min_length: int = 0,
        nullable: bool = True,
        default: Optional[str] = None,
        unique: bool = False,
    ):
        """Initialize String type.

        Args:
            length: Maximum length (None for unlimited TEXT)
            fixed: Whether fixed-length (CHAR vs VARCHAR)
            collation: Collation for sorting/comparison
            pattern: Regex pattern for validation
            min_length: Minimum required length
            nullable: Whether NULL is allowed
            default: Default value
            unique: Whether values must be unique
        """
        self.length = length
        self.fixed = fixed
        self.collation = collation
        self.pattern = pattern
        self._compiled_pattern = re.compile(pattern) if pattern else None
        self.min_length = min_length
        self.nullable = nullable
        self.default = default
        self.unique = unique

    def validate(self, value: Any) -> List[ValidationError]:
        errors = []

        if value is None:
            if not self.nullable:
                errors.append(ValidationError("value", "NULL not allowed", value, "not_null"))
            return errors

        if not isinstance(value, str):
            errors.append(ValidationError("value", f"Expected string, got {type(value).__name__}", value, "type"))
            return errors

        if len(value) < self.min_length:
            errors.append(
                ValidationError("value", f"String too short (min {self.min_length})", value, "min_length")
            )

        if self.length is not None and len(value) > self.length:
            errors.append(ValidationError("value", f"String too long (max {self.length})", value, "max_length"))

        if self._compiled_pattern and not self._compiled_pattern.match(value):
            errors.append(ValidationError("value", f"String doesn't match pattern '{self.pattern}'", value, "pattern"))

        return errors

    def serialize(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if self.fixed and self.length:
            return str(value).ljust(self.length)
        return str(value)

    def deserialize(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        result = str(value)
        if self.fixed:
            result = result.rstrip()
        return result

    def sql_type(self, dialect: SQLDialect = SQLDialect.STANDARD) -> str:
        if self.length is None:
            return "TEXT"
        if self.fixed:
            return f"CHAR({self.length})"
        return f"VARCHAR({self.length})"

    def python_type(self) -> Type:
        return str

    def __repr__(self) -> str:
        if self.length is None:
            return "Text()"
        if self.fixed:
            return f"Char({self.length})"
        return f"String({self.length})"


class Text(String):
    """Unlimited length text type."""

    def __init__(self, **kwargs):
        kwargs["length"] = None
        kwargs["fixed"] = False
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return "Text()"


# =============================================================================
# Binary Types
# =============================================================================


class Binary(DataType):
    """Binary data type.

    Supports:
    - BINARY (fixed length)
    - VARBINARY (variable length)
    - BLOB (large binary object)
    """

    category = TypeCategory.BINARY

    def __init__(
        self,
        length: Optional[int] = None,
        *,
        fixed: bool = False,
        nullable: bool = True,
        default: Optional[bytes] = None,
    ):
        """Initialize Binary type.

        Args:
            length: Maximum length (None for BLOB)
            fixed: Whether fixed-length
            nullable: Whether NULL is allowed
            default: Default value
        """
        self.length = length
        self.fixed = fixed
        self.nullable = nullable
        self.default = default

    def validate(self, value: Any) -> List[ValidationError]:
        errors = []

        if value is None:
            if not self.nullable:
                errors.append(ValidationError("value", "NULL not allowed", value, "not_null"))
            return errors

        if not isinstance(value, (bytes, bytearray)):
            errors.append(ValidationError("value", f"Expected bytes, got {type(value).__name__}", value, "type"))
            return errors

        if self.length is not None and len(value) > self.length:
            errors.append(ValidationError("value", f"Binary too long (max {self.length})", value, "max_length"))

        return errors

    def serialize(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, bytearray):
            value = bytes(value)
        return base64.b64encode(value).decode("ascii")

    def deserialize(self, value: Any) -> Optional[bytes]:
        if value is None:
            return None
        if isinstance(value, bytes):
            return value
        return base64.b64decode(value)

    def sql_type(self, dialect: SQLDialect = SQLDialect.STANDARD) -> str:
        if self.length is None:
            return "BLOB"
        if self.fixed:
            return f"BINARY({self.length})"
        return f"VARBINARY({self.length})"

    def python_type(self) -> Type:
        return bytes


# =============================================================================
# Temporal Types
# =============================================================================


class DateTime(DataType):
    """Date and time type with optional timezone."""

    category = TypeCategory.TEMPORAL

    def __init__(
        self,
        *,
        timezone: bool = False,
        auto_now: bool = False,
        auto_now_add: bool = False,
        nullable: bool = True,
        default: Optional[datetime] = None,
    ):
        """Initialize DateTime type.

        Args:
            timezone: Whether to store timezone info
            auto_now: Update to current time on every save
            auto_now_add: Set to current time on insert
            nullable: Whether NULL is allowed
            default: Default value
        """
        self.timezone = timezone
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add
        self.nullable = nullable
        self.default = default

    def validate(self, value: Any) -> List[ValidationError]:
        errors = []

        if value is None:
            if not self.nullable and not self.auto_now_add:
                errors.append(ValidationError("value", "NULL not allowed", value, "not_null"))
            return errors

        if isinstance(value, str):
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                errors.append(ValidationError("value", f"Invalid datetime string: {value}", value, "format"))
        elif not isinstance(value, datetime):
            errors.append(ValidationError("value", f"Expected datetime, got {type(value).__name__}", value, "type"))

        return errors

    def serialize(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value.isoformat()

    def deserialize(self, value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    def sql_type(self, dialect: SQLDialect = SQLDialect.STANDARD) -> str:
        if self.timezone:
            if dialect == SQLDialect.POSTGRESQL:
                return "TIMESTAMP WITH TIME ZONE"
            return "DATETIME"  # Most DBs don't distinguish
        return "DATETIME" if dialect != SQLDialect.POSTGRESQL else "TIMESTAMP"

    def python_type(self) -> Type:
        return datetime


class Timestamp(DateTime):
    """Unix timestamp type (seconds since epoch)."""

    def serialize(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return int(value.timestamp())

    def deserialize(self, value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        return datetime.fromtimestamp(float(value))

    def sql_type(self, dialect: SQLDialect = SQLDialect.STANDARD) -> str:
        return "BIGINT"  # Store as Unix timestamp

    def __repr__(self) -> str:
        return "Timestamp()"


# =============================================================================
# Boolean Type
# =============================================================================


class Boolean(DataType):
    """Boolean type."""

    category = TypeCategory.BOOLEAN

    def __init__(
        self,
        *,
        nullable: bool = True,
        default: Optional[bool] = None,
    ):
        """Initialize Boolean type."""
        self.nullable = nullable
        self.default = default

    def validate(self, value: Any) -> List[ValidationError]:
        errors = []

        if value is None:
            if not self.nullable:
                errors.append(ValidationError("value", "NULL not allowed", value, "not_null"))
            return errors

        if not isinstance(value, bool) and value not in (0, 1, "true", "false", "True", "False"):
            errors.append(ValidationError("value", f"Expected boolean, got {type(value).__name__}", value, "type"))

        return errors

    def serialize(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, str):
            return 1 if value.lower() == "true" else 0
        return 1 if value else 0

    def deserialize(self, value: Any) -> Optional[bool]:
        if value is None:
            return None
        return bool(value)

    def sql_type(self, dialect: SQLDialect = SQLDialect.STANDARD) -> str:
        if dialect == SQLDialect.SQLITE:
            return "INTEGER"
        return "BOOLEAN"

    def python_type(self) -> Type:
        return bool


# =============================================================================
# Composite Types
# =============================================================================


class JSON(DataType):
    """JSON type for structured data."""

    category = TypeCategory.COMPOSITE

    def __init__(
        self,
        *,
        schema: Optional[Dict[str, Any]] = None,
        nullable: bool = True,
        default: Optional[Dict] = None,
    ):
        """Initialize JSON type.

        Args:
            schema: JSON Schema for validation
            nullable: Whether NULL is allowed
            default: Default value
        """
        self.schema = schema
        self.nullable = nullable
        self.default = default

    def validate(self, value: Any) -> List[ValidationError]:
        errors = []

        if value is None:
            if not self.nullable:
                errors.append(ValidationError("value", "NULL not allowed", value, "not_null"))
            return errors

        if isinstance(value, str):
            try:
                json.loads(value)
            except json.JSONDecodeError as e:
                errors.append(ValidationError("value", f"Invalid JSON: {e}", value, "format"))

        # TODO: Add JSON Schema validation if self.schema is set

        return errors

    def serialize(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return json.dumps(value)

    def deserialize(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            return json.loads(value)
        return value

    def sql_type(self, dialect: SQLDialect = SQLDialect.STANDARD) -> str:
        if dialect == SQLDialect.POSTGRESQL:
            return "JSONB"
        if dialect == SQLDialect.MYSQL:
            return "JSON"
        return "TEXT"  # SQLite, others

    def python_type(self) -> Type:
        return dict


class Array(DataType):
    """Array type for lists of values."""

    category = TypeCategory.COMPOSITE

    def __init__(
        self,
        element_type: DataType,
        *,
        max_length: Optional[int] = None,
        nullable: bool = True,
        default: Optional[List] = None,
    ):
        """Initialize Array type.

        Args:
            element_type: Type of array elements
            max_length: Maximum array length
            nullable: Whether NULL is allowed
            default: Default value
        """
        self.element_type = element_type
        self.max_length = max_length
        self.nullable = nullable
        self.default = default

    def validate(self, value: Any) -> List[ValidationError]:
        errors = []

        if value is None:
            if not self.nullable:
                errors.append(ValidationError("value", "NULL not allowed", value, "not_null"))
            return errors

        if not isinstance(value, (list, tuple)):
            errors.append(ValidationError("value", f"Expected array, got {type(value).__name__}", value, "type"))
            return errors

        if self.max_length and len(value) > self.max_length:
            errors.append(ValidationError("value", f"Array too long (max {self.max_length})", value, "max_length"))

        for i, item in enumerate(value):
            item_errors = self.element_type.validate(item)
            for err in item_errors:
                err.field = f"[{i}].{err.field}"
                errors.append(err)

        return errors

    def serialize(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        serialized = [self.element_type.serialize(item) for item in value]
        return json.dumps(serialized)

    def deserialize(self, value: Any) -> Optional[List]:
        if value is None:
            return None
        if isinstance(value, str):
            value = json.loads(value)
        return [self.element_type.deserialize(item) for item in value]

    def sql_type(self, dialect: SQLDialect = SQLDialect.STANDARD) -> str:
        if dialect == SQLDialect.POSTGRESQL:
            return f"{self.element_type.sql_type(dialect)}[]"
        return "TEXT"  # Store as JSON

    def python_type(self) -> Type:
        return list

    def __repr__(self) -> str:
        return f"Array({self.element_type})"


class UUID(DataType):
    """UUID type."""

    category = TypeCategory.SPECIAL

    def __init__(
        self,
        *,
        version: int = 4,
        auto_generate: bool = False,
        nullable: bool = True,
        default: Optional[str] = None,
    ):
        """Initialize UUID type.

        Args:
            version: UUID version (1, 4)
            auto_generate: Automatically generate UUIDs
            nullable: Whether NULL is allowed
            default: Default value
        """
        self.version = version
        self.auto_generate = auto_generate
        self.nullable = nullable
        self.default = default

    def validate(self, value: Any) -> List[ValidationError]:
        errors = []

        if value is None:
            if not self.nullable and not self.auto_generate:
                errors.append(ValidationError("value", "NULL not allowed", value, "not_null"))
            return errors

        if isinstance(value, uuid_module.UUID):
            return errors

        if isinstance(value, str):
            try:
                uuid_module.UUID(value)
            except ValueError:
                errors.append(ValidationError("value", f"Invalid UUID: {value}", value, "format"))
        else:
            errors.append(ValidationError("value", f"Expected UUID, got {type(value).__name__}", value, "type"))

        return errors

    def serialize(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, uuid_module.UUID):
            return str(value)
        return value

    def deserialize(self, value: Any) -> Optional[uuid_module.UUID]:
        if value is None:
            return None
        if isinstance(value, uuid_module.UUID):
            return value
        return uuid_module.UUID(str(value))

    def generate(self) -> uuid_module.UUID:
        """Generate a new UUID."""
        if self.version == 1:
            return uuid_module.uuid1()
        return uuid_module.uuid4()

    def sql_type(self, dialect: SQLDialect = SQLDialect.STANDARD) -> str:
        if dialect == SQLDialect.POSTGRESQL:
            return "UUID"
        return "VARCHAR(36)"

    def python_type(self) -> Type:
        return uuid_module.UUID


# =============================================================================
# Type Registry
# =============================================================================


class TypeRegistry:
    """Registry of all available types for dynamic type resolution."""

    _types: Dict[str, Type[DataType]] = {
        "integer": Integer,
        "int": Integer,
        "bigint": BigInteger,
        "float": Float,
        "double": Float,
        "decimal": Decimal,
        "string": String,
        "varchar": String,
        "text": Text,
        "binary": Binary,
        "blob": Binary,
        "datetime": DateTime,
        "timestamp": Timestamp,
        "boolean": Boolean,
        "bool": Boolean,
        "json": JSON,
        "jsonb": JSON,
        "array": Array,
        "uuid": UUID,
    }

    @classmethod
    def get(cls, type_name: str) -> Optional[Type[DataType]]:
        """Get a type class by name."""
        return cls._types.get(type_name.lower())

    @classmethod
    def register(cls, name: str, type_class: Type[DataType]) -> None:
        """Register a new type."""
        cls._types[name.lower()] = type_class

    @classmethod
    def all_types(cls) -> Dict[str, Type[DataType]]:
        """Get all registered types."""
        return cls._types.copy()


__all__ = [
    # Base
    "DataType",
    "ValidationError",
    "SQLDialect",
    "TypeCategory",
    "TypeRegistry",
    # Numeric
    "Integer",
    "BigInteger",
    "Float",
    "Decimal",
    # String
    "String",
    "Text",
    # Binary
    "Binary",
    # Temporal
    "DateTime",
    "Timestamp",
    # Boolean
    "Boolean",
    # Composite
    "JSON",
    "Array",
    "UUID",
]
