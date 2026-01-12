"""RoadDB Query Parser - SQL parsing and AST generation.

Provides comprehensive SQL parsing capabilities including:
- SELECT, INSERT, UPDATE, DELETE statements
- JOIN operations (INNER, LEFT, RIGHT, FULL, CROSS)
- Subqueries and CTEs (Common Table Expressions)
- Window functions
- Aggregate functions with GROUP BY/HAVING
- Complex WHERE clauses with AND/OR/NOT

Architecture:
    ┌─────────────────────────────────────────────────────────────────────┐
    │                         Query Parser                               │
    ├─────────────────────────────────────────────────────────────────────┤
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
    │  │   Lexer     │──│   Parser    │──│    AST      │                 │
    │  │  (Tokens)   │  │  (Grammar)  │  │   Builder   │                 │
    │  └─────────────┘  └─────────────┘  └─────────────┘                 │
    │         │               │               │                           │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
    │  │   SQL       │  │  Validator  │  │   Query     │                 │
    │  │  Dialect    │──│   Rules     │──│   Object    │                 │
    │  └─────────────┘  └─────────────┘  └─────────────┘                 │
    └─────────────────────────────────────────────────────────────────────┘

Copyright (c) 2024-2026 BlackRoad OS, Inc. All rights reserved.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple, Union


# =============================================================================
# Token Types
# =============================================================================


class TokenType(Enum):
    """SQL token types."""

    # Literals
    NUMBER = auto()
    STRING = auto()
    IDENTIFIER = auto()
    PARAMETER = auto()

    # Keywords
    SELECT = auto()
    FROM = auto()
    WHERE = auto()
    AND = auto()
    OR = auto()
    NOT = auto()
    IN = auto()
    LIKE = auto()
    BETWEEN = auto()
    IS = auto()
    NULL = auto()
    TRUE = auto()
    FALSE = auto()
    AS = auto()
    ON = auto()
    JOIN = auto()
    LEFT = auto()
    RIGHT = auto()
    INNER = auto()
    OUTER = auto()
    FULL = auto()
    CROSS = auto()
    NATURAL = auto()
    USING = auto()
    ORDER = auto()
    BY = auto()
    ASC = auto()
    DESC = auto()
    LIMIT = auto()
    OFFSET = auto()
    GROUP = auto()
    HAVING = auto()
    DISTINCT = auto()
    ALL = auto()
    UNION = auto()
    INTERSECT = auto()
    EXCEPT = auto()
    INSERT = auto()
    INTO = auto()
    VALUES = auto()
    UPDATE = auto()
    SET = auto()
    DELETE = auto()
    CREATE = auto()
    TABLE = auto()
    INDEX = auto()
    VIEW = auto()
    DROP = auto()
    ALTER = auto()
    ADD = auto()
    COLUMN = auto()
    PRIMARY = auto()
    KEY = auto()
    FOREIGN = auto()
    REFERENCES = auto()
    UNIQUE = auto()
    CHECK = auto()
    DEFAULT = auto()
    CONSTRAINT = auto()
    CASCADE = auto()
    RESTRICT = auto()
    NULLS = auto()
    FIRST = auto()
    LAST = auto()
    CASE = auto()
    WHEN = auto()
    THEN = auto()
    ELSE = auto()
    END = auto()
    CAST = auto()
    EXISTS = auto()
    ANY = auto()
    SOME = auto()
    WITH = auto()
    RECURSIVE = auto()
    OVER = auto()
    PARTITION = auto()
    ROWS = auto()
    RANGE = auto()
    UNBOUNDED = auto()
    PRECEDING = auto()
    FOLLOWING = auto()
    CURRENT = auto()
    ROW = auto()

    # Operators
    PLUS = auto()
    MINUS = auto()
    STAR = auto()
    SLASH = auto()
    PERCENT = auto()
    EQUAL = auto()
    NOT_EQUAL = auto()
    LESS = auto()
    LESS_EQUAL = auto()
    GREATER = auto()
    GREATER_EQUAL = auto()
    CONCAT = auto()

    # Punctuation
    LPAREN = auto()
    RPAREN = auto()
    COMMA = auto()
    DOT = auto()
    SEMICOLON = auto()
    COLON = auto()

    # Special
    EOF = auto()
    ERROR = auto()


@dataclass
class Token:
    """Lexical token."""

    type: TokenType
    value: Any
    position: int
    line: int
    column: int

    def __repr__(self) -> str:
        return f"Token({self.type.name}, {self.value!r})"


# =============================================================================
# Lexer
# =============================================================================


class Lexer:
    """SQL lexer (tokenizer)."""

    KEYWORDS = {
        "SELECT": TokenType.SELECT,
        "FROM": TokenType.FROM,
        "WHERE": TokenType.WHERE,
        "AND": TokenType.AND,
        "OR": TokenType.OR,
        "NOT": TokenType.NOT,
        "IN": TokenType.IN,
        "LIKE": TokenType.LIKE,
        "BETWEEN": TokenType.BETWEEN,
        "IS": TokenType.IS,
        "NULL": TokenType.NULL,
        "TRUE": TokenType.TRUE,
        "FALSE": TokenType.FALSE,
        "AS": TokenType.AS,
        "ON": TokenType.ON,
        "JOIN": TokenType.JOIN,
        "LEFT": TokenType.LEFT,
        "RIGHT": TokenType.RIGHT,
        "INNER": TokenType.INNER,
        "OUTER": TokenType.OUTER,
        "FULL": TokenType.FULL,
        "CROSS": TokenType.CROSS,
        "NATURAL": TokenType.NATURAL,
        "USING": TokenType.USING,
        "ORDER": TokenType.ORDER,
        "BY": TokenType.BY,
        "ASC": TokenType.ASC,
        "DESC": TokenType.DESC,
        "LIMIT": TokenType.LIMIT,
        "OFFSET": TokenType.OFFSET,
        "GROUP": TokenType.GROUP,
        "HAVING": TokenType.HAVING,
        "DISTINCT": TokenType.DISTINCT,
        "ALL": TokenType.ALL,
        "UNION": TokenType.UNION,
        "INTERSECT": TokenType.INTERSECT,
        "EXCEPT": TokenType.EXCEPT,
        "INSERT": TokenType.INSERT,
        "INTO": TokenType.INTO,
        "VALUES": TokenType.VALUES,
        "UPDATE": TokenType.UPDATE,
        "SET": TokenType.SET,
        "DELETE": TokenType.DELETE,
        "CREATE": TokenType.CREATE,
        "TABLE": TokenType.TABLE,
        "INDEX": TokenType.INDEX,
        "VIEW": TokenType.VIEW,
        "DROP": TokenType.DROP,
        "ALTER": TokenType.ALTER,
        "ADD": TokenType.ADD,
        "COLUMN": TokenType.COLUMN,
        "PRIMARY": TokenType.PRIMARY,
        "KEY": TokenType.KEY,
        "FOREIGN": TokenType.FOREIGN,
        "REFERENCES": TokenType.REFERENCES,
        "UNIQUE": TokenType.UNIQUE,
        "CHECK": TokenType.CHECK,
        "DEFAULT": TokenType.DEFAULT,
        "CONSTRAINT": TokenType.CONSTRAINT,
        "CASCADE": TokenType.CASCADE,
        "RESTRICT": TokenType.RESTRICT,
        "NULLS": TokenType.NULLS,
        "FIRST": TokenType.FIRST,
        "LAST": TokenType.LAST,
        "CASE": TokenType.CASE,
        "WHEN": TokenType.WHEN,
        "THEN": TokenType.THEN,
        "ELSE": TokenType.ELSE,
        "END": TokenType.END,
        "CAST": TokenType.CAST,
        "EXISTS": TokenType.EXISTS,
        "ANY": TokenType.ANY,
        "SOME": TokenType.SOME,
        "WITH": TokenType.WITH,
        "RECURSIVE": TokenType.RECURSIVE,
        "OVER": TokenType.OVER,
        "PARTITION": TokenType.PARTITION,
        "ROWS": TokenType.ROWS,
        "RANGE": TokenType.RANGE,
        "UNBOUNDED": TokenType.UNBOUNDED,
        "PRECEDING": TokenType.PRECEDING,
        "FOLLOWING": TokenType.FOLLOWING,
        "CURRENT": TokenType.CURRENT,
        "ROW": TokenType.ROW,
    }

    def __init__(self, sql: str):
        """Initialize lexer.

        Args:
            sql: SQL string to tokenize
        """
        self.sql = sql
        self.pos = 0
        self.line = 1
        self.column = 1

    def tokenize(self) -> List[Token]:
        """Tokenize the SQL string.

        Returns:
            List of tokens
        """
        tokens = []
        while self.pos < len(self.sql):
            token = self._next_token()
            if token:
                tokens.append(token)

        tokens.append(Token(TokenType.EOF, None, self.pos, self.line, self.column))
        return tokens

    def _next_token(self) -> Optional[Token]:
        """Get the next token."""
        self._skip_whitespace()
        self._skip_comments()
        self._skip_whitespace()

        if self.pos >= len(self.sql):
            return None

        start_pos = self.pos
        start_line = self.line
        start_col = self.column

        char = self.sql[self.pos]

        # String literal
        if char in ("'", '"'):
            return self._read_string(char, start_pos, start_line, start_col)

        # Number
        if char.isdigit() or (char == "." and self._peek(1).isdigit()):
            return self._read_number(start_pos, start_line, start_col)

        # Identifier or keyword
        if char.isalpha() or char == "_":
            return self._read_identifier(start_pos, start_line, start_col)

        # Parameter placeholder
        if char in ("?", "$", ":"):
            return self._read_parameter(char, start_pos, start_line, start_col)

        # Operators and punctuation
        return self._read_operator(start_pos, start_line, start_col)

    def _skip_whitespace(self) -> None:
        """Skip whitespace characters."""
        while self.pos < len(self.sql) and self.sql[self.pos].isspace():
            if self.sql[self.pos] == "\n":
                self.line += 1
                self.column = 1
            else:
                self.column += 1
            self.pos += 1

    def _skip_comments(self) -> None:
        """Skip SQL comments."""
        if self.pos + 1 >= len(self.sql):
            return

        # Single-line comment
        if self.sql[self.pos : self.pos + 2] == "--":
            while self.pos < len(self.sql) and self.sql[self.pos] != "\n":
                self.pos += 1
            return

        # Multi-line comment
        if self.sql[self.pos : self.pos + 2] == "/*":
            self.pos += 2
            while self.pos + 1 < len(self.sql) and self.sql[self.pos : self.pos + 2] != "*/":
                if self.sql[self.pos] == "\n":
                    self.line += 1
                    self.column = 1
                self.pos += 1
            self.pos += 2
            self.column += 2

    def _peek(self, offset: int = 0) -> str:
        """Peek at character at offset."""
        pos = self.pos + offset
        if pos < len(self.sql):
            return self.sql[pos]
        return ""

    def _read_string(self, quote: str, start_pos: int, start_line: int, start_col: int) -> Token:
        """Read a string literal."""
        self.pos += 1
        self.column += 1
        value = ""

        while self.pos < len(self.sql):
            char = self.sql[self.pos]

            if char == quote:
                # Check for escaped quote
                if self._peek(1) == quote:
                    value += quote
                    self.pos += 2
                    self.column += 2
                else:
                    self.pos += 1
                    self.column += 1
                    return Token(TokenType.STRING, value, start_pos, start_line, start_col)
            elif char == "\\":
                # Handle escape sequences
                self.pos += 1
                self.column += 1
                if self.pos < len(self.sql):
                    escape_char = self.sql[self.pos]
                    escape_map = {"n": "\n", "t": "\t", "r": "\r", "\\": "\\"}
                    value += escape_map.get(escape_char, escape_char)
                    self.pos += 1
                    self.column += 1
            else:
                value += char
                self.pos += 1
                self.column += 1

        return Token(TokenType.ERROR, f"Unterminated string", start_pos, start_line, start_col)

    def _read_number(self, start_pos: int, start_line: int, start_col: int) -> Token:
        """Read a numeric literal."""
        value = ""
        has_dot = False
        has_e = False

        while self.pos < len(self.sql):
            char = self.sql[self.pos]

            if char.isdigit():
                value += char
            elif char == "." and not has_dot and not has_e:
                has_dot = True
                value += char
            elif char.lower() == "e" and not has_e:
                has_e = True
                value += char
                # Handle optional sign after 'e'
                if self._peek(1) in ("+", "-"):
                    self.pos += 1
                    self.column += 1
                    value += self.sql[self.pos]
            else:
                break

            self.pos += 1
            self.column += 1

        # Convert to appropriate type
        if has_dot or has_e:
            num_value = float(value)
        else:
            num_value = int(value)

        return Token(TokenType.NUMBER, num_value, start_pos, start_line, start_col)

    def _read_identifier(self, start_pos: int, start_line: int, start_col: int) -> Token:
        """Read an identifier or keyword."""
        value = ""

        while self.pos < len(self.sql):
            char = self.sql[self.pos]
            if char.isalnum() or char == "_":
                value += char
                self.pos += 1
                self.column += 1
            else:
                break

        # Check if keyword
        upper_value = value.upper()
        if upper_value in self.KEYWORDS:
            return Token(self.KEYWORDS[upper_value], value, start_pos, start_line, start_col)

        return Token(TokenType.IDENTIFIER, value, start_pos, start_line, start_col)

    def _read_parameter(self, char: str, start_pos: int, start_line: int, start_col: int) -> Token:
        """Read a parameter placeholder."""
        self.pos += 1
        self.column += 1

        if char == "?":
            return Token(TokenType.PARAMETER, "?", start_pos, start_line, start_col)

        # Named parameter ($name or :name)
        name = ""
        while self.pos < len(self.sql):
            c = self.sql[self.pos]
            if c.isalnum() or c == "_":
                name += c
                self.pos += 1
                self.column += 1
            else:
                break

        return Token(TokenType.PARAMETER, f"{char}{name}", start_pos, start_line, start_col)

    def _read_operator(self, start_pos: int, start_line: int, start_col: int) -> Token:
        """Read an operator or punctuation."""
        char = self.sql[self.pos]
        next_char = self._peek(1)

        # Two-character operators
        two_char = char + next_char
        two_char_ops = {
            "!=": TokenType.NOT_EQUAL,
            "<>": TokenType.NOT_EQUAL,
            "<=": TokenType.LESS_EQUAL,
            ">=": TokenType.GREATER_EQUAL,
            "||": TokenType.CONCAT,
        }

        if two_char in two_char_ops:
            self.pos += 2
            self.column += 2
            return Token(two_char_ops[two_char], two_char, start_pos, start_line, start_col)

        # Single-character operators
        single_char_ops = {
            "+": TokenType.PLUS,
            "-": TokenType.MINUS,
            "*": TokenType.STAR,
            "/": TokenType.SLASH,
            "%": TokenType.PERCENT,
            "=": TokenType.EQUAL,
            "<": TokenType.LESS,
            ">": TokenType.GREATER,
            "(": TokenType.LPAREN,
            ")": TokenType.RPAREN,
            ",": TokenType.COMMA,
            ".": TokenType.DOT,
            ";": TokenType.SEMICOLON,
            ":": TokenType.COLON,
        }

        if char in single_char_ops:
            self.pos += 1
            self.column += 1
            return Token(single_char_ops[char], char, start_pos, start_line, start_col)

        # Unknown character
        self.pos += 1
        self.column += 1
        return Token(TokenType.ERROR, f"Unknown character: {char}", start_pos, start_line, start_col)


# =============================================================================
# AST Nodes
# =============================================================================


@dataclass
class ASTNode:
    """Base class for AST nodes."""

    pass


@dataclass
class Expression(ASTNode):
    """Base class for expressions."""

    pass


@dataclass
class Statement(ASTNode):
    """Base class for statements."""

    pass


@dataclass
class Literal(Expression):
    """Literal value."""

    value: Any
    type_hint: Optional[str] = None


@dataclass
class Identifier(Expression):
    """Column or table identifier."""

    name: str
    schema: Optional[str] = None
    table: Optional[str] = None

    def full_name(self) -> str:
        """Get fully qualified name."""
        parts = []
        if self.schema:
            parts.append(self.schema)
        if self.table:
            parts.append(self.table)
        parts.append(self.name)
        return ".".join(parts)


@dataclass
class Parameter(Expression):
    """Query parameter placeholder."""

    name: str
    index: Optional[int] = None


@dataclass
class BinaryOp(Expression):
    """Binary operation."""

    operator: str
    left: Expression
    right: Expression


@dataclass
class UnaryOp(Expression):
    """Unary operation."""

    operator: str
    operand: Expression


@dataclass
class FunctionCall(Expression):
    """Function call."""

    name: str
    args: List[Expression] = field(default_factory=list)
    distinct: bool = False
    over: Optional[WindowSpec] = None


@dataclass
class WindowSpec(ASTNode):
    """Window specification for window functions."""

    partition_by: List[Expression] = field(default_factory=list)
    order_by: List[OrderByItem] = field(default_factory=list)
    frame: Optional[WindowFrame] = None


@dataclass
class WindowFrame(ASTNode):
    """Window frame specification."""

    type: str  # ROWS or RANGE
    start: str
    end: Optional[str] = None


@dataclass
class CaseExpression(Expression):
    """CASE expression."""

    operand: Optional[Expression] = None
    when_clauses: List[Tuple[Expression, Expression]] = field(default_factory=list)
    else_clause: Optional[Expression] = None


@dataclass
class SubqueryExpression(Expression):
    """Subquery as expression."""

    query: SelectStatement


@dataclass
class InExpression(Expression):
    """IN expression."""

    operand: Expression
    values: Union[List[Expression], SubqueryExpression]
    negated: bool = False


@dataclass
class BetweenExpression(Expression):
    """BETWEEN expression."""

    operand: Expression
    low: Expression
    high: Expression
    negated: bool = False


@dataclass
class LikeExpression(Expression):
    """LIKE expression."""

    operand: Expression
    pattern: Expression
    escape: Optional[Expression] = None
    negated: bool = False


@dataclass
class ExistsExpression(Expression):
    """EXISTS expression."""

    subquery: SubqueryExpression


@dataclass
class CastExpression(Expression):
    """CAST expression."""

    operand: Expression
    target_type: str


@dataclass
class TableRef(ASTNode):
    """Table reference."""

    name: str
    schema: Optional[str] = None
    alias: Optional[str] = None


@dataclass
class JoinClause(ASTNode):
    """JOIN clause."""

    type: str  # INNER, LEFT, RIGHT, FULL, CROSS
    table: Union[TableRef, SubqueryExpression]
    condition: Optional[Expression] = None
    using: Optional[List[str]] = None


@dataclass
class SelectItem(ASTNode):
    """Item in SELECT list."""

    expression: Expression
    alias: Optional[str] = None


@dataclass
class OrderByItem(ASTNode):
    """ORDER BY item."""

    expression: Expression
    direction: str = "ASC"
    nulls: Optional[str] = None  # FIRST or LAST


@dataclass
class GroupByClause(ASTNode):
    """GROUP BY clause."""

    items: List[Expression]
    having: Optional[Expression] = None


@dataclass
class CTEDefinition(ASTNode):
    """Common Table Expression definition."""

    name: str
    columns: Optional[List[str]] = None
    query: SelectStatement = None
    recursive: bool = False


@dataclass
class SelectStatement(Statement):
    """SELECT statement."""

    columns: List[SelectItem] = field(default_factory=list)
    distinct: bool = False
    from_clause: Optional[TableRef] = None
    joins: List[JoinClause] = field(default_factory=list)
    where: Optional[Expression] = None
    group_by: Optional[GroupByClause] = None
    order_by: List[OrderByItem] = field(default_factory=list)
    limit: Optional[int] = None
    offset: Optional[int] = None
    ctes: List[CTEDefinition] = field(default_factory=list)


@dataclass
class InsertStatement(Statement):
    """INSERT statement."""

    table: TableRef
    columns: Optional[List[str]] = None
    values: Optional[List[List[Expression]]] = None
    query: Optional[SelectStatement] = None
    on_conflict: Optional[str] = None


@dataclass
class UpdateStatement(Statement):
    """UPDATE statement."""

    table: TableRef
    assignments: List[Tuple[str, Expression]] = field(default_factory=list)
    where: Optional[Expression] = None
    from_clause: Optional[TableRef] = None


@dataclass
class DeleteStatement(Statement):
    """DELETE statement."""

    table: TableRef
    where: Optional[Expression] = None
    using: Optional[TableRef] = None


@dataclass
class CreateTableStatement(Statement):
    """CREATE TABLE statement."""

    table: TableRef
    columns: List[ColumnDefinition] = field(default_factory=list)
    constraints: List[TableConstraint] = field(default_factory=list)
    if_not_exists: bool = False


@dataclass
class ColumnDefinition(ASTNode):
    """Column definition."""

    name: str
    type: str
    constraints: List[str] = field(default_factory=list)
    default: Optional[Expression] = None
    nullable: bool = True


@dataclass
class TableConstraint(ASTNode):
    """Table constraint."""

    type: str  # PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK
    columns: List[str] = field(default_factory=list)
    name: Optional[str] = None
    expression: Optional[Expression] = None
    references: Optional[Tuple[str, List[str]]] = None


# =============================================================================
# Parser
# =============================================================================


class ParseError(Exception):
    """SQL parse error."""

    def __init__(self, message: str, token: Token):
        self.token = token
        super().__init__(f"{message} at line {token.line}, column {token.column}")


class Parser:
    """SQL parser."""

    def __init__(self, tokens: List[Token]):
        """Initialize parser.

        Args:
            tokens: List of tokens from lexer
        """
        self.tokens = tokens
        self.pos = 0

    def parse(self) -> Statement:
        """Parse tokens into AST.

        Returns:
            Statement AST node
        """
        # Handle CTEs
        ctes = []
        if self._check(TokenType.WITH):
            ctes = self._parse_ctes()

        # Parse main statement
        if self._check(TokenType.SELECT):
            stmt = self._parse_select()
            stmt.ctes = ctes
            return stmt
        elif self._check(TokenType.INSERT):
            return self._parse_insert()
        elif self._check(TokenType.UPDATE):
            return self._parse_update()
        elif self._check(TokenType.DELETE):
            return self._parse_delete()
        elif self._check(TokenType.CREATE):
            return self._parse_create()
        else:
            raise ParseError(f"Unexpected token: {self._current().type.name}", self._current())

    def _current(self) -> Token:
        """Get current token."""
        return self.tokens[self.pos]

    def _peek(self, offset: int = 1) -> Token:
        """Peek at token at offset."""
        pos = self.pos + offset
        if pos < len(self.tokens):
            return self.tokens[pos]
        return self.tokens[-1]

    def _check(self, *types: TokenType) -> bool:
        """Check if current token is one of types."""
        return self._current().type in types

    def _advance(self) -> Token:
        """Advance to next token and return previous."""
        token = self._current()
        if not self._check(TokenType.EOF):
            self.pos += 1
        return token

    def _expect(self, type: TokenType) -> Token:
        """Expect and consume a token of type."""
        if not self._check(type):
            raise ParseError(f"Expected {type.name}, got {self._current().type.name}", self._current())
        return self._advance()

    def _match(self, *types: TokenType) -> bool:
        """Match and consume if current token is one of types."""
        if self._check(*types):
            self._advance()
            return True
        return False

    # -------------------------------------------------------------------------
    # SELECT parsing
    # -------------------------------------------------------------------------

    def _parse_select(self) -> SelectStatement:
        """Parse SELECT statement."""
        self._expect(TokenType.SELECT)

        stmt = SelectStatement()

        # DISTINCT
        if self._match(TokenType.DISTINCT):
            stmt.distinct = True
        elif self._match(TokenType.ALL):
            pass

        # Select list
        stmt.columns = self._parse_select_list()

        # FROM
        if self._match(TokenType.FROM):
            stmt.from_clause = self._parse_table_ref()

            # JOINs
            while self._check(TokenType.JOIN, TokenType.LEFT, TokenType.RIGHT, TokenType.INNER, TokenType.FULL, TokenType.CROSS, TokenType.NATURAL):
                stmt.joins.append(self._parse_join())

        # WHERE
        if self._match(TokenType.WHERE):
            stmt.where = self._parse_expression()

        # GROUP BY
        if self._match(TokenType.GROUP):
            self._expect(TokenType.BY)
            stmt.group_by = self._parse_group_by()

        # ORDER BY
        if self._match(TokenType.ORDER):
            self._expect(TokenType.BY)
            stmt.order_by = self._parse_order_by()

        # LIMIT
        if self._match(TokenType.LIMIT):
            limit_token = self._expect(TokenType.NUMBER)
            stmt.limit = int(limit_token.value)

        # OFFSET
        if self._match(TokenType.OFFSET):
            offset_token = self._expect(TokenType.NUMBER)
            stmt.offset = int(offset_token.value)

        return stmt

    def _parse_select_list(self) -> List[SelectItem]:
        """Parse SELECT list."""
        items = []

        while True:
            if self._match(TokenType.STAR):
                items.append(SelectItem(expression=Literal(value="*")))
            else:
                expr = self._parse_expression()
                alias = None
                if self._match(TokenType.AS):
                    alias = self._expect(TokenType.IDENTIFIER).value
                elif self._check(TokenType.IDENTIFIER):
                    alias = self._advance().value

                items.append(SelectItem(expression=expr, alias=alias))

            if not self._match(TokenType.COMMA):
                break

        return items

    def _parse_table_ref(self) -> TableRef:
        """Parse table reference."""
        if self._check(TokenType.LPAREN):
            # Subquery
            self._advance()
            query = self._parse_select()
            self._expect(TokenType.RPAREN)

            alias = None
            if self._match(TokenType.AS):
                alias = self._expect(TokenType.IDENTIFIER).value
            elif self._check(TokenType.IDENTIFIER):
                alias = self._advance().value

            return SubqueryExpression(query=query)

        # Table name
        name_token = self._expect(TokenType.IDENTIFIER)
        schema = None
        name = name_token.value

        if self._match(TokenType.DOT):
            schema = name
            name = self._expect(TokenType.IDENTIFIER).value

        alias = None
        if self._match(TokenType.AS):
            alias = self._expect(TokenType.IDENTIFIER).value
        elif self._check(TokenType.IDENTIFIER) and not self._check(TokenType.JOIN, TokenType.LEFT, TokenType.RIGHT, TokenType.WHERE, TokenType.ORDER, TokenType.GROUP, TokenType.LIMIT):
            alias = self._advance().value

        return TableRef(name=name, schema=schema, alias=alias)

    def _parse_join(self) -> JoinClause:
        """Parse JOIN clause."""
        join_type = "INNER"

        if self._match(TokenType.NATURAL):
            join_type = "NATURAL"

        if self._match(TokenType.LEFT):
            join_type = "LEFT"
            self._match(TokenType.OUTER)
        elif self._match(TokenType.RIGHT):
            join_type = "RIGHT"
            self._match(TokenType.OUTER)
        elif self._match(TokenType.FULL):
            join_type = "FULL"
            self._match(TokenType.OUTER)
        elif self._match(TokenType.CROSS):
            join_type = "CROSS"
        elif self._match(TokenType.INNER):
            join_type = "INNER"

        self._expect(TokenType.JOIN)

        table = self._parse_table_ref()
        condition = None
        using = None

        if self._match(TokenType.ON):
            condition = self._parse_expression()
        elif self._match(TokenType.USING):
            self._expect(TokenType.LPAREN)
            using = []
            while True:
                using.append(self._expect(TokenType.IDENTIFIER).value)
                if not self._match(TokenType.COMMA):
                    break
            self._expect(TokenType.RPAREN)

        return JoinClause(type=join_type, table=table, condition=condition, using=using)

    def _parse_group_by(self) -> GroupByClause:
        """Parse GROUP BY clause."""
        items = []
        while True:
            items.append(self._parse_expression())
            if not self._match(TokenType.COMMA):
                break

        having = None
        if self._match(TokenType.HAVING):
            having = self._parse_expression()

        return GroupByClause(items=items, having=having)

    def _parse_order_by(self) -> List[OrderByItem]:
        """Parse ORDER BY clause."""
        items = []
        while True:
            expr = self._parse_expression()
            direction = "ASC"
            nulls = None

            if self._match(TokenType.ASC):
                direction = "ASC"
            elif self._match(TokenType.DESC):
                direction = "DESC"

            if self._match(TokenType.NULLS):
                if self._match(TokenType.FIRST):
                    nulls = "FIRST"
                elif self._match(TokenType.LAST):
                    nulls = "LAST"

            items.append(OrderByItem(expression=expr, direction=direction, nulls=nulls))

            if not self._match(TokenType.COMMA):
                break

        return items

    def _parse_ctes(self) -> List[CTEDefinition]:
        """Parse Common Table Expressions."""
        self._expect(TokenType.WITH)
        ctes = []

        recursive = self._match(TokenType.RECURSIVE)

        while True:
            name = self._expect(TokenType.IDENTIFIER).value
            columns = None

            if self._match(TokenType.LPAREN):
                columns = []
                while True:
                    columns.append(self._expect(TokenType.IDENTIFIER).value)
                    if not self._match(TokenType.COMMA):
                        break
                self._expect(TokenType.RPAREN)

            self._expect(TokenType.AS)
            self._expect(TokenType.LPAREN)
            query = self._parse_select()
            self._expect(TokenType.RPAREN)

            ctes.append(CTEDefinition(name=name, columns=columns, query=query, recursive=recursive))

            if not self._match(TokenType.COMMA):
                break

        return ctes

    # -------------------------------------------------------------------------
    # Expression parsing
    # -------------------------------------------------------------------------

    def _parse_expression(self) -> Expression:
        """Parse expression."""
        return self._parse_or_expression()

    def _parse_or_expression(self) -> Expression:
        """Parse OR expression."""
        left = self._parse_and_expression()

        while self._match(TokenType.OR):
            right = self._parse_and_expression()
            left = BinaryOp(operator="OR", left=left, right=right)

        return left

    def _parse_and_expression(self) -> Expression:
        """Parse AND expression."""
        left = self._parse_not_expression()

        while self._match(TokenType.AND):
            right = self._parse_not_expression()
            left = BinaryOp(operator="AND", left=left, right=right)

        return left

    def _parse_not_expression(self) -> Expression:
        """Parse NOT expression."""
        if self._match(TokenType.NOT):
            operand = self._parse_not_expression()
            return UnaryOp(operator="NOT", operand=operand)

        return self._parse_comparison()

    def _parse_comparison(self) -> Expression:
        """Parse comparison expression."""
        left = self._parse_additive()

        # IS NULL / IS NOT NULL
        if self._match(TokenType.IS):
            negated = self._match(TokenType.NOT)
            self._expect(TokenType.NULL)
            op = "IS NOT NULL" if negated else "IS NULL"
            return UnaryOp(operator=op, operand=left)

        # IN
        if self._check(TokenType.NOT) and self._peek().type == TokenType.IN:
            self._advance()  # NOT
            self._advance()  # IN
            return self._parse_in_expression(left, negated=True)

        if self._match(TokenType.IN):
            return self._parse_in_expression(left, negated=False)

        # BETWEEN
        if self._check(TokenType.NOT) and self._peek().type == TokenType.BETWEEN:
            self._advance()  # NOT
            self._advance()  # BETWEEN
            return self._parse_between_expression(left, negated=True)

        if self._match(TokenType.BETWEEN):
            return self._parse_between_expression(left, negated=False)

        # LIKE
        if self._check(TokenType.NOT) and self._peek().type == TokenType.LIKE:
            self._advance()  # NOT
            self._advance()  # LIKE
            return self._parse_like_expression(left, negated=True)

        if self._match(TokenType.LIKE):
            return self._parse_like_expression(left, negated=False)

        # Comparison operators
        ops = {
            TokenType.EQUAL: "=",
            TokenType.NOT_EQUAL: "!=",
            TokenType.LESS: "<",
            TokenType.LESS_EQUAL: "<=",
            TokenType.GREATER: ">",
            TokenType.GREATER_EQUAL: ">=",
        }

        if self._check(*ops.keys()):
            op_token = self._advance()
            right = self._parse_additive()
            return BinaryOp(operator=ops[op_token.type], left=left, right=right)

        return left

    def _parse_in_expression(self, operand: Expression, negated: bool) -> InExpression:
        """Parse IN expression."""
        self._expect(TokenType.LPAREN)

        if self._check(TokenType.SELECT):
            query = self._parse_select()
            values = SubqueryExpression(query=query)
        else:
            values = []
            while True:
                values.append(self._parse_expression())
                if not self._match(TokenType.COMMA):
                    break

        self._expect(TokenType.RPAREN)
        return InExpression(operand=operand, values=values, negated=negated)

    def _parse_between_expression(self, operand: Expression, negated: bool) -> BetweenExpression:
        """Parse BETWEEN expression."""
        low = self._parse_additive()
        self._expect(TokenType.AND)
        high = self._parse_additive()
        return BetweenExpression(operand=operand, low=low, high=high, negated=negated)

    def _parse_like_expression(self, operand: Expression, negated: bool) -> LikeExpression:
        """Parse LIKE expression."""
        pattern = self._parse_primary()
        escape = None
        # Handle ESCAPE clause if needed
        return LikeExpression(operand=operand, pattern=pattern, escape=escape, negated=negated)

    def _parse_additive(self) -> Expression:
        """Parse additive expression (+, -, ||)."""
        left = self._parse_multiplicative()

        while self._check(TokenType.PLUS, TokenType.MINUS, TokenType.CONCAT):
            op_token = self._advance()
            op = {TokenType.PLUS: "+", TokenType.MINUS: "-", TokenType.CONCAT: "||"}[op_token.type]
            right = self._parse_multiplicative()
            left = BinaryOp(operator=op, left=left, right=right)

        return left

    def _parse_multiplicative(self) -> Expression:
        """Parse multiplicative expression (*, /, %)."""
        left = self._parse_unary()

        while self._check(TokenType.STAR, TokenType.SLASH, TokenType.PERCENT):
            op_token = self._advance()
            op = {TokenType.STAR: "*", TokenType.SLASH: "/", TokenType.PERCENT: "%"}[op_token.type]
            right = self._parse_unary()
            left = BinaryOp(operator=op, left=left, right=right)

        return left

    def _parse_unary(self) -> Expression:
        """Parse unary expression (+, -)."""
        if self._check(TokenType.PLUS, TokenType.MINUS):
            op_token = self._advance()
            op = "+" if op_token.type == TokenType.PLUS else "-"
            operand = self._parse_unary()
            return UnaryOp(operator=op, operand=operand)

        return self._parse_primary()

    def _parse_primary(self) -> Expression:
        """Parse primary expression."""
        token = self._current()

        # Literals
        if self._match(TokenType.NUMBER):
            return Literal(value=token.value)

        if self._match(TokenType.STRING):
            return Literal(value=token.value)

        if self._match(TokenType.TRUE):
            return Literal(value=True)

        if self._match(TokenType.FALSE):
            return Literal(value=False)

        if self._match(TokenType.NULL):
            return Literal(value=None)

        # Parameter
        if self._match(TokenType.PARAMETER):
            return Parameter(name=token.value)

        # CASE expression
        if self._match(TokenType.CASE):
            return self._parse_case()

        # EXISTS
        if self._match(TokenType.EXISTS):
            self._expect(TokenType.LPAREN)
            query = self._parse_select()
            self._expect(TokenType.RPAREN)
            return ExistsExpression(subquery=SubqueryExpression(query=query))

        # CAST
        if self._match(TokenType.CAST):
            self._expect(TokenType.LPAREN)
            operand = self._parse_expression()
            self._expect(TokenType.AS)
            type_name = self._expect(TokenType.IDENTIFIER).value
            self._expect(TokenType.RPAREN)
            return CastExpression(operand=operand, target_type=type_name)

        # Parenthesized expression or subquery
        if self._match(TokenType.LPAREN):
            if self._check(TokenType.SELECT):
                query = self._parse_select()
                self._expect(TokenType.RPAREN)
                return SubqueryExpression(query=query)
            else:
                expr = self._parse_expression()
                self._expect(TokenType.RPAREN)
                return expr

        # Identifier or function call
        if self._match(TokenType.IDENTIFIER):
            name = token.value

            # Check for qualified name
            if self._match(TokenType.DOT):
                table = name
                name = self._expect(TokenType.IDENTIFIER).value
                if self._match(TokenType.DOT):
                    schema = table
                    table = name
                    name = self._expect(TokenType.IDENTIFIER).value
                    return Identifier(name=name, table=table, schema=schema)
                return Identifier(name=name, table=table)

            # Function call
            if self._match(TokenType.LPAREN):
                return self._parse_function_call(name)

            return Identifier(name=name)

        raise ParseError(f"Unexpected token: {token.type.name}", token)

    def _parse_function_call(self, name: str) -> FunctionCall:
        """Parse function call."""
        distinct = self._match(TokenType.DISTINCT)
        args = []

        if not self._check(TokenType.RPAREN):
            if self._match(TokenType.STAR):
                args.append(Literal(value="*"))
            else:
                while True:
                    args.append(self._parse_expression())
                    if not self._match(TokenType.COMMA):
                        break

        self._expect(TokenType.RPAREN)

        # Window function
        over = None
        if self._match(TokenType.OVER):
            over = self._parse_window_spec()

        return FunctionCall(name=name.upper(), args=args, distinct=distinct, over=over)

    def _parse_window_spec(self) -> WindowSpec:
        """Parse window specification."""
        self._expect(TokenType.LPAREN)

        partition_by = []
        order_by = []
        frame = None

        if self._match(TokenType.PARTITION):
            self._expect(TokenType.BY)
            while True:
                partition_by.append(self._parse_expression())
                if not self._match(TokenType.COMMA):
                    break

        if self._match(TokenType.ORDER):
            self._expect(TokenType.BY)
            order_by = self._parse_order_by()

        # Frame clause (simplified)
        if self._check(TokenType.ROWS, TokenType.RANGE):
            frame_type = "ROWS" if self._match(TokenType.ROWS) else "RANGE"
            self._advance()  # Consume ROWS/RANGE
            # Simplified frame parsing
            frame = WindowFrame(type=frame_type, start="UNBOUNDED PRECEDING")

        self._expect(TokenType.RPAREN)

        return WindowSpec(partition_by=partition_by, order_by=order_by, frame=frame)

    def _parse_case(self) -> CaseExpression:
        """Parse CASE expression."""
        operand = None

        # Simple CASE vs searched CASE
        if not self._check(TokenType.WHEN):
            operand = self._parse_expression()

        when_clauses = []
        while self._match(TokenType.WHEN):
            condition = self._parse_expression()
            self._expect(TokenType.THEN)
            result = self._parse_expression()
            when_clauses.append((condition, result))

        else_clause = None
        if self._match(TokenType.ELSE):
            else_clause = self._parse_expression()

        self._expect(TokenType.END)

        return CaseExpression(operand=operand, when_clauses=when_clauses, else_clause=else_clause)

    # -------------------------------------------------------------------------
    # DML parsing
    # -------------------------------------------------------------------------

    def _parse_insert(self) -> InsertStatement:
        """Parse INSERT statement."""
        self._expect(TokenType.INSERT)
        self._expect(TokenType.INTO)

        table = self._parse_table_ref()
        columns = None

        if self._match(TokenType.LPAREN):
            columns = []
            while True:
                columns.append(self._expect(TokenType.IDENTIFIER).value)
                if not self._match(TokenType.COMMA):
                    break
            self._expect(TokenType.RPAREN)

        if self._check(TokenType.SELECT):
            query = self._parse_select()
            return InsertStatement(table=table, columns=columns, query=query)

        self._expect(TokenType.VALUES)
        values = []
        while True:
            self._expect(TokenType.LPAREN)
            row = []
            while True:
                row.append(self._parse_expression())
                if not self._match(TokenType.COMMA):
                    break
            self._expect(TokenType.RPAREN)
            values.append(row)
            if not self._match(TokenType.COMMA):
                break

        return InsertStatement(table=table, columns=columns, values=values)

    def _parse_update(self) -> UpdateStatement:
        """Parse UPDATE statement."""
        self._expect(TokenType.UPDATE)

        table = self._parse_table_ref()

        self._expect(TokenType.SET)
        assignments = []
        while True:
            column = self._expect(TokenType.IDENTIFIER).value
            self._expect(TokenType.EQUAL)
            value = self._parse_expression()
            assignments.append((column, value))
            if not self._match(TokenType.COMMA):
                break

        where = None
        if self._match(TokenType.WHERE):
            where = self._parse_expression()

        return UpdateStatement(table=table, assignments=assignments, where=where)

    def _parse_delete(self) -> DeleteStatement:
        """Parse DELETE statement."""
        self._expect(TokenType.DELETE)
        self._expect(TokenType.FROM)

        table = self._parse_table_ref()

        where = None
        if self._match(TokenType.WHERE):
            where = self._parse_expression()

        return DeleteStatement(table=table, where=where)

    def _parse_create(self) -> Statement:
        """Parse CREATE statement."""
        self._expect(TokenType.CREATE)

        if self._match(TokenType.TABLE):
            return self._parse_create_table()

        raise ParseError(f"Unsupported CREATE type", self._current())

    def _parse_create_table(self) -> CreateTableStatement:
        """Parse CREATE TABLE statement."""
        if_not_exists = False
        # Handle IF NOT EXISTS

        name_token = self._expect(TokenType.IDENTIFIER)
        schema = None
        name = name_token.value

        if self._match(TokenType.DOT):
            schema = name
            name = self._expect(TokenType.IDENTIFIER).value

        table = TableRef(name=name, schema=schema)

        self._expect(TokenType.LPAREN)

        columns = []
        constraints = []

        while True:
            if self._check(TokenType.CONSTRAINT, TokenType.PRIMARY, TokenType.UNIQUE, TokenType.FOREIGN, TokenType.CHECK):
                constraints.append(self._parse_table_constraint())
            else:
                columns.append(self._parse_column_definition())

            if not self._match(TokenType.COMMA):
                break

        self._expect(TokenType.RPAREN)

        return CreateTableStatement(table=table, columns=columns, constraints=constraints, if_not_exists=if_not_exists)

    def _parse_column_definition(self) -> ColumnDefinition:
        """Parse column definition."""
        name = self._expect(TokenType.IDENTIFIER).value
        type_name = self._expect(TokenType.IDENTIFIER).value

        # Handle type with parameters
        if self._match(TokenType.LPAREN):
            type_name += "("
            type_name += str(self._expect(TokenType.NUMBER).value)
            if self._match(TokenType.COMMA):
                type_name += ","
                type_name += str(self._expect(TokenType.NUMBER).value)
            type_name += ")"
            self._expect(TokenType.RPAREN)

        constraints = []
        default = None
        nullable = True

        while True:
            if self._match(TokenType.NOT):
                self._expect(TokenType.NULL)
                constraints.append("NOT NULL")
                nullable = False
            elif self._match(TokenType.NULL):
                nullable = True
            elif self._match(TokenType.PRIMARY):
                self._expect(TokenType.KEY)
                constraints.append("PRIMARY KEY")
            elif self._match(TokenType.UNIQUE):
                constraints.append("UNIQUE")
            elif self._match(TokenType.DEFAULT):
                default = self._parse_primary()
            elif self._match(TokenType.REFERENCES):
                ref_table = self._expect(TokenType.IDENTIFIER).value
                constraints.append(f"REFERENCES {ref_table}")
            else:
                break

        return ColumnDefinition(name=name, type=type_name, constraints=constraints, default=default, nullable=nullable)

    def _parse_table_constraint(self) -> TableConstraint:
        """Parse table constraint."""
        name = None
        if self._match(TokenType.CONSTRAINT):
            name = self._expect(TokenType.IDENTIFIER).value

        if self._match(TokenType.PRIMARY):
            self._expect(TokenType.KEY)
            columns = self._parse_column_list()
            return TableConstraint(type="PRIMARY KEY", columns=columns, name=name)

        if self._match(TokenType.UNIQUE):
            columns = self._parse_column_list()
            return TableConstraint(type="UNIQUE", columns=columns, name=name)

        if self._match(TokenType.FOREIGN):
            self._expect(TokenType.KEY)
            columns = self._parse_column_list()
            self._expect(TokenType.REFERENCES)
            ref_table = self._expect(TokenType.IDENTIFIER).value
            ref_columns = self._parse_column_list()
            return TableConstraint(type="FOREIGN KEY", columns=columns, name=name, references=(ref_table, ref_columns))

        if self._match(TokenType.CHECK):
            self._expect(TokenType.LPAREN)
            expr = self._parse_expression()
            self._expect(TokenType.RPAREN)
            return TableConstraint(type="CHECK", name=name, expression=expr)

        raise ParseError("Unknown constraint type", self._current())

    def _parse_column_list(self) -> List[str]:
        """Parse column list in parentheses."""
        self._expect(TokenType.LPAREN)
        columns = []
        while True:
            columns.append(self._expect(TokenType.IDENTIFIER).value)
            if not self._match(TokenType.COMMA):
                break
        self._expect(TokenType.RPAREN)
        return columns


# =============================================================================
# Query Builder
# =============================================================================


class Query:
    """High-level query builder and parser interface."""

    def __init__(self, sql: Optional[str] = None):
        """Initialize query.

        Args:
            sql: Optional SQL string to parse
        """
        self.sql = sql
        self.ast: Optional[Statement] = None
        self._params: Dict[str, Any] = {}

        if sql:
            self.parse()

    def parse(self) -> Statement:
        """Parse SQL into AST."""
        lexer = Lexer(self.sql)
        tokens = lexer.tokenize()
        parser = Parser(tokens)
        self.ast = parser.parse()
        return self.ast

    def bind(self, **params) -> Query:
        """Bind parameters to query.

        Args:
            **params: Named parameters

        Returns:
            Self for chaining
        """
        self._params.update(params)
        return self

    @classmethod
    def select(cls, *columns: str) -> QueryBuilder:
        """Start building a SELECT query."""
        return QueryBuilder().select(*columns)

    @classmethod
    def insert_into(cls, table: str) -> QueryBuilder:
        """Start building an INSERT query."""
        return QueryBuilder().insert_into(table)

    @classmethod
    def update(cls, table: str) -> QueryBuilder:
        """Start building an UPDATE query."""
        return QueryBuilder().update(table)

    @classmethod
    def delete_from(cls, table: str) -> QueryBuilder:
        """Start building a DELETE query."""
        return QueryBuilder().delete_from(table)


class QueryBuilder:
    """Fluent query builder."""

    def __init__(self):
        self._type: Optional[str] = None
        self._columns: List[str] = []
        self._table: Optional[str] = None
        self._joins: List[Dict] = []
        self._where: List[str] = []
        self._group_by: List[str] = []
        self._having: Optional[str] = None
        self._order_by: List[str] = []
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._values: List[Dict] = []
        self._set: Dict[str, Any] = {}

    def select(self, *columns: str) -> QueryBuilder:
        self._type = "SELECT"
        self._columns = list(columns) if columns else ["*"]
        return self

    def from_(self, table: str) -> QueryBuilder:
        self._table = table
        return self

    def join(self, table: str, on: str, type: str = "INNER") -> QueryBuilder:
        self._joins.append({"table": table, "on": on, "type": type})
        return self

    def left_join(self, table: str, on: str) -> QueryBuilder:
        return self.join(table, on, "LEFT")

    def where(self, condition: str) -> QueryBuilder:
        self._where.append(condition)
        return self

    def and_where(self, condition: str) -> QueryBuilder:
        return self.where(condition)

    def or_where(self, condition: str) -> QueryBuilder:
        if self._where:
            self._where[-1] = f"({self._where[-1]} OR {condition})"
        else:
            self._where.append(condition)
        return self

    def group_by(self, *columns: str) -> QueryBuilder:
        self._group_by.extend(columns)
        return self

    def having(self, condition: str) -> QueryBuilder:
        self._having = condition
        return self

    def order_by(self, column: str, direction: str = "ASC") -> QueryBuilder:
        self._order_by.append(f"{column} {direction}")
        return self

    def limit(self, count: int) -> QueryBuilder:
        self._limit = count
        return self

    def offset(self, count: int) -> QueryBuilder:
        self._offset = count
        return self

    def insert_into(self, table: str) -> QueryBuilder:
        self._type = "INSERT"
        self._table = table
        return self

    def values(self, **data: Any) -> QueryBuilder:
        self._values.append(data)
        return self

    def update(self, table: str) -> QueryBuilder:
        self._type = "UPDATE"
        self._table = table
        return self

    def set(self, **data: Any) -> QueryBuilder:
        self._set.update(data)
        return self

    def delete_from(self, table: str) -> QueryBuilder:
        self._type = "DELETE"
        self._table = table
        return self

    def build(self) -> str:
        """Build SQL string."""
        if self._type == "SELECT":
            return self._build_select()
        elif self._type == "INSERT":
            return self._build_insert()
        elif self._type == "UPDATE":
            return self._build_update()
        elif self._type == "DELETE":
            return self._build_delete()
        else:
            raise ValueError("Query type not set")

    def _build_select(self) -> str:
        parts = ["SELECT", ", ".join(self._columns)]

        if self._table:
            parts.extend(["FROM", self._table])

        for join in self._joins:
            parts.append(f"{join['type']} JOIN {join['table']} ON {join['on']}")

        if self._where:
            parts.extend(["WHERE", " AND ".join(self._where)])

        if self._group_by:
            parts.extend(["GROUP BY", ", ".join(self._group_by)])
            if self._having:
                parts.extend(["HAVING", self._having])

        if self._order_by:
            parts.extend(["ORDER BY", ", ".join(self._order_by)])

        if self._limit is not None:
            parts.extend(["LIMIT", str(self._limit)])

        if self._offset is not None:
            parts.extend(["OFFSET", str(self._offset)])

        return " ".join(parts)

    def _build_insert(self) -> str:
        if not self._values:
            raise ValueError("No values to insert")

        columns = list(self._values[0].keys())
        values_list = []
        for row in self._values:
            vals = [self._quote_value(row.get(c)) for c in columns]
            values_list.append(f"({', '.join(vals)})")

        return f"INSERT INTO {self._table} ({', '.join(columns)}) VALUES {', '.join(values_list)}"

    def _build_update(self) -> str:
        if not self._set:
            raise ValueError("No values to update")

        set_clause = ", ".join(f"{k} = {self._quote_value(v)}" for k, v in self._set.items())
        parts = [f"UPDATE {self._table} SET {set_clause}"]

        if self._where:
            parts.extend(["WHERE", " AND ".join(self._where)])

        return " ".join(parts)

    def _build_delete(self) -> str:
        parts = [f"DELETE FROM {self._table}"]

        if self._where:
            parts.extend(["WHERE", " AND ".join(self._where)])

        return " ".join(parts)

    def _quote_value(self, value: Any) -> str:
        """Quote a value for SQL."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        return f"'{str(value).replace(chr(39), chr(39)+chr(39))}'"


# Convenience alias
QueryParser = Parser


__all__ = [
    # Lexer
    "Lexer",
    "Token",
    "TokenType",
    # Parser
    "Parser",
    "QueryParser",
    "ParseError",
    # AST Nodes
    "ASTNode",
    "Expression",
    "Statement",
    "Literal",
    "Identifier",
    "Parameter",
    "BinaryOp",
    "UnaryOp",
    "FunctionCall",
    "WindowSpec",
    "WindowFrame",
    "CaseExpression",
    "SubqueryExpression",
    "InExpression",
    "BetweenExpression",
    "LikeExpression",
    "ExistsExpression",
    "CastExpression",
    "TableRef",
    "JoinClause",
    "SelectItem",
    "OrderByItem",
    "GroupByClause",
    "CTEDefinition",
    "SelectStatement",
    "InsertStatement",
    "UpdateStatement",
    "DeleteStatement",
    "CreateTableStatement",
    "ColumnDefinition",
    "TableConstraint",
    # Query interface
    "Query",
    "QueryBuilder",
]
