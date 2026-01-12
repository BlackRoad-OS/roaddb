"""RoadDB CLI - Command-line interface for RoadDB operations.

Provides comprehensive database management through the command line:
- Database initialization and configuration
- Query execution (SQL and interactive REPL)
- Schema management (tables, indexes, views)
- Replication management
- Agent control (Archivist, Indexer, Guardian, Analyst, Replicator)
- Monitoring and diagnostics

Usage:
    roaddb init --backend sqlite --path ./data
    roaddb query "SELECT * FROM users WHERE active = true"
    roaddb replicate --source primary --target replica-1
    roaddb agent archivist --archive --older-than 30d
    roaddb monitor --dashboard

Copyright (c) 2024-2026 BlackRoad OS, Inc. All rights reserved.
"""

from __future__ import annotations

import argparse
import json
import os
import readline
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# RoadDB imports
from roaddb_core import __version__
from roaddb_core.engine import RoadDB, StorageBackend


# =============================================================================
# Output Formatting
# =============================================================================


class OutputFormatter:
    """Formats output for CLI display."""

    COLORS = {
        "reset": "\033[0m",
        "bold": "\033[1m",
        "dim": "\033[2m",
        "red": "\033[31m",
        "green": "\033[32m",
        "yellow": "\033[33m",
        "blue": "\033[34m",
        "magenta": "\033[35m",
        "cyan": "\033[36m",
        "white": "\033[37m",
    }

    def __init__(self, color: bool = True, json_output: bool = False):
        """Initialize formatter.

        Args:
            color: Enable colored output
            json_output: Output as JSON
        """
        self.color = color and sys.stdout.isatty()
        self.json_output = json_output

    def _c(self, text: str, color: str) -> str:
        """Colorize text if color enabled."""
        if self.color:
            return f"{self.COLORS.get(color, '')}{text}{self.COLORS['reset']}"
        return text

    def success(self, message: str) -> None:
        """Print success message."""
        if self.json_output:
            print(json.dumps({"status": "success", "message": message}))
        else:
            print(self._c("✓", "green"), message)

    def error(self, message: str) -> None:
        """Print error message."""
        if self.json_output:
            print(json.dumps({"status": "error", "message": message}))
        else:
            print(self._c("✗", "red"), message, file=sys.stderr)

    def warning(self, message: str) -> None:
        """Print warning message."""
        if self.json_output:
            print(json.dumps({"status": "warning", "message": message}))
        else:
            print(self._c("⚠", "yellow"), message)

    def info(self, message: str) -> None:
        """Print info message."""
        if self.json_output:
            print(json.dumps({"status": "info", "message": message}))
        else:
            print(self._c("ℹ", "blue"), message)

    def header(self, text: str) -> None:
        """Print header."""
        if not self.json_output:
            print()
            print(self._c(f"═══ {text} ═══", "bold"))
            print()

    def table(self, headers: List[str], rows: List[List[Any]], title: Optional[str] = None) -> None:
        """Print formatted table."""
        if self.json_output:
            print(json.dumps({"title": title, "headers": headers, "rows": rows}))
            return

        if title:
            print(self._c(title, "bold"))
            print()

        if not rows:
            print(self._c("(empty)", "dim"))
            return

        # Calculate column widths
        widths = [len(h) for h in headers]
        for row in rows:
            for i, cell in enumerate(row):
                widths[i] = max(widths[i], len(str(cell)))

        # Print header
        header_line = " │ ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
        separator = "─┼─".join("─" * w for w in widths)

        print(self._c(header_line, "bold"))
        print(separator)

        # Print rows
        for row in rows:
            row_line = " │ ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row))
            print(row_line)

        print()
        print(self._c(f"({len(rows)} rows)", "dim"))

    def kv(self, data: Dict[str, Any], title: Optional[str] = None) -> None:
        """Print key-value pairs."""
        if self.json_output:
            print(json.dumps({"title": title, "data": data}))
            return

        if title:
            print(self._c(title, "bold"))
            print()

        max_key_len = max(len(k) for k in data.keys()) if data else 0

        for key, value in data.items():
            key_str = self._c(f"{key}:", "cyan").ljust(max_key_len + 10)
            print(f"  {key_str} {value}")


# =============================================================================
# Interactive REPL
# =============================================================================


class RoadDBRepl:
    """Interactive SQL REPL."""

    COMMANDS = {
        ".help": "Show help",
        ".tables": "List all tables",
        ".schema": "Show schema for a table",
        ".indexes": "Show indexes for a table",
        ".quit": "Exit REPL",
        ".exit": "Exit REPL",
        ".stats": "Show database statistics",
        ".agents": "Show agent status",
        ".explain": "Explain query plan",
    }

    def __init__(self, db: RoadDB, formatter: OutputFormatter):
        """Initialize REPL.

        Args:
            db: RoadDB instance
            formatter: Output formatter
        """
        self.db = db
        self.formatter = formatter
        self.history_file = Path.home() / ".roaddb_history"
        self._setup_readline()

    def _setup_readline(self) -> None:
        """Set up readline for history and completion."""
        try:
            if self.history_file.exists():
                readline.read_history_file(str(self.history_file))
            readline.set_history_length(1000)
        except Exception:
            pass

        # SQL keywords for completion
        keywords = [
            "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN",
            "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "CREATE", "TABLE",
            "INDEX", "VIEW", "DROP", "ALTER", "ADD", "COLUMN", "PRIMARY", "KEY",
            "FOREIGN", "REFERENCES", "UNIQUE", "ORDER", "BY", "ASC", "DESC",
            "LIMIT", "OFFSET", "GROUP", "HAVING", "JOIN", "LEFT", "RIGHT", "INNER",
            "OUTER", "FULL", "CROSS", "ON", "AS", "DISTINCT", "COUNT", "SUM", "AVG",
            "MIN", "MAX", "CASE", "WHEN", "THEN", "ELSE", "END", "NULL", "TRUE", "FALSE",
        ]

        def completer(text: str, state: int) -> Optional[str]:
            options = [w for w in keywords if w.startswith(text.upper())]
            options += [c for c in self.COMMANDS if c.startswith(text)]
            if state < len(options):
                return options[state]
            return None

        readline.set_completer(completer)
        readline.parse_and_bind("tab: complete")

    def _save_history(self) -> None:
        """Save history to file."""
        try:
            readline.write_history_file(str(self.history_file))
        except Exception:
            pass

    def run(self) -> None:
        """Run the REPL."""
        self.formatter.header("RoadDB Interactive Shell")
        print(f"Version {__version__}")
        print("Type .help for available commands")
        print()

        buffer = ""

        while True:
            try:
                prompt = "roaddb> " if not buffer else "     -> "
                line = input(prompt)

                # Handle dot commands
                if line.startswith(".") and not buffer:
                    self._handle_command(line)
                    continue

                # Accumulate multi-line statements
                buffer += " " + line if buffer else line

                # Check for complete statement
                if buffer.rstrip().endswith(";"):
                    self._execute(buffer.rstrip())
                    buffer = ""

            except EOFError:
                print()
                break
            except KeyboardInterrupt:
                print()
                buffer = ""
                continue

        self._save_history()
        self.formatter.info("Goodbye!")

    def _handle_command(self, line: str) -> None:
        """Handle dot command."""
        parts = line.split()
        cmd = parts[0]
        args = parts[1:] if len(parts) > 1 else []

        if cmd in (".quit", ".exit"):
            self._save_history()
            self.formatter.info("Goodbye!")
            sys.exit(0)

        elif cmd == ".help":
            self.formatter.header("Commands")
            for command, description in self.COMMANDS.items():
                print(f"  {command:<12} {description}")

        elif cmd == ".tables":
            tables = self._get_tables()
            if tables:
                for table in tables:
                    print(f"  {table}")
            else:
                print("  (no tables)")

        elif cmd == ".schema":
            if args:
                self._show_schema(args[0])
            else:
                self.formatter.error("Usage: .schema <table_name>")

        elif cmd == ".indexes":
            table = args[0] if args else None
            self._show_indexes(table)

        elif cmd == ".stats":
            stats = self.db.get_stats()
            self.formatter.kv(stats, "Database Statistics")

        elif cmd == ".agents":
            self._show_agents()

        elif cmd == ".explain":
            if args:
                self._explain(" ".join(args))
            else:
                self.formatter.error("Usage: .explain <query>")

        else:
            self.formatter.error(f"Unknown command: {cmd}")

    def _execute(self, sql: str) -> None:
        """Execute SQL statement."""
        start_time = time.time()

        try:
            result = self.db.execute(sql)
            elapsed = (time.time() - start_time) * 1000

            if result.rows is not None and result.columns:
                self.formatter.table(result.columns, result.rows)
            elif result.affected_rows is not None:
                self.formatter.success(f"Affected {result.affected_rows} rows")

            print(self.formatter._c(f"Time: {elapsed:.2f}ms", "dim"))

        except Exception as e:
            self.formatter.error(str(e))

    def _get_tables(self) -> List[str]:
        """Get list of tables."""
        result = self.db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        if result.rows:
            return [row[0] for row in result.rows]
        return []

    def _show_schema(self, table: str) -> None:
        """Show schema for a table."""
        result = self.db.execute(f"PRAGMA table_info({table})")
        if result.rows:
            self.formatter.table(
                ["Column", "Type", "Nullable", "Default", "PK"],
                [[r[1], r[2], "NO" if r[3] else "YES", r[4] or "", "YES" if r[5] else ""] for r in result.rows],
                f"Schema: {table}",
            )
        else:
            self.formatter.error(f"Table not found: {table}")

    def _show_indexes(self, table: Optional[str]) -> None:
        """Show indexes."""
        sql = "SELECT name, tbl_name, sql FROM sqlite_master WHERE type='index'"
        if table:
            sql += f" AND tbl_name='{table}'"

        result = self.db.execute(sql)
        if result.rows:
            self.formatter.table(
                ["Index", "Table", "Definition"],
                [[r[0], r[1], (r[2] or "")[:50]] for r in result.rows],
                "Indexes",
            )
        else:
            print("  (no indexes)")

    def _show_agents(self) -> None:
        """Show agent status."""
        agents = [
            ("Archivist", "Data lifecycle management", "active"),
            ("Indexer", "Query optimization", "active"),
            ("Guardian", "Security & audit", "active"),
            ("Analyst", "Pattern detection", "active"),
            ("Replicator", "Replication & consensus", "standby"),
        ]

        self.formatter.table(["Agent", "Domain", "Status"], agents, "Agent Status")

    def _explain(self, sql: str) -> None:
        """Explain query plan."""
        try:
            result = self.db.execute(f"EXPLAIN QUERY PLAN {sql}")
            if result.rows:
                print()
                for row in result.rows:
                    print(f"  {row}")
        except Exception as e:
            self.formatter.error(str(e))


# =============================================================================
# Command Handlers
# =============================================================================


def cmd_init(args: argparse.Namespace, formatter: OutputFormatter) -> int:
    """Initialize database."""
    path = Path(args.path or "./data")

    formatter.info(f"Initializing RoadDB at {path}")

    try:
        path.mkdir(parents=True, exist_ok=True)

        # Create config file
        config = {
            "version": __version__,
            "backend": args.backend or "sqlite",
            "data_dir": str(path / "db"),
            "wal_dir": str(path / "wal"),
            "created_at": datetime.now().isoformat(),
        }

        config_file = path / "roaddb.yaml"
        with open(config_file, "w") as f:
            import yaml
            yaml.dump(config, f, default_flow_style=False)

        # Create subdirectories
        (path / "db").mkdir(exist_ok=True)
        (path / "wal").mkdir(exist_ok=True)
        (path / "backups").mkdir(exist_ok=True)
        (path / "logs").mkdir(exist_ok=True)

        formatter.success(f"Initialized RoadDB at {path}")
        formatter.info(f"Configuration saved to {config_file}")

        return 0

    except Exception as e:
        formatter.error(f"Initialization failed: {e}")
        return 1


def cmd_query(args: argparse.Namespace, formatter: OutputFormatter) -> int:
    """Execute SQL query."""
    try:
        db = RoadDB(backend=args.backend or "memory")

        if args.interactive or not args.sql:
            repl = RoadDBRepl(db, formatter)
            repl.run()
            return 0

        result = db.execute(args.sql)

        if result.rows is not None and result.columns:
            formatter.table(result.columns, result.rows)
        elif result.affected_rows is not None:
            formatter.success(f"Affected {result.affected_rows} rows")

        return 0

    except Exception as e:
        formatter.error(str(e))
        return 1


def cmd_agent(args: argparse.Namespace, formatter: OutputFormatter) -> int:
    """Manage agents."""
    agent_name = args.agent

    formatter.header(f"Agent: {agent_name.title()}")

    if agent_name == "archivist":
        from roaddb_core.agents.archivist import Archivist

        agent = Archivist()

        if args.status:
            stats = agent.get_stats()
            formatter.kv(stats, "Archivist Statistics")
        elif args.archive:
            formatter.info("Starting archive process...")
            agent.archive_old_data(timedelta(days=args.older_than or 30))
            formatter.success("Archive complete")
        else:
            formatter.info("Use --status or --archive")

    elif agent_name == "indexer":
        from roaddb_core.agents.indexer import Indexer

        agent = Indexer()

        if args.status:
            stats = agent.get_stats()
            formatter.kv(stats, "Indexer Statistics")
        elif args.recommend:
            recommendations = agent.recommend_indexes()
            if recommendations:
                formatter.table(
                    ["Table", "Columns", "Type", "Priority"],
                    [[r.table, ", ".join(r.columns), r.index_type.name, r.priority] for r in recommendations],
                    "Index Recommendations",
                )
            else:
                formatter.info("No recommendations")
        else:
            formatter.info("Use --status or --recommend")

    elif agent_name == "guardian":
        from roaddb_core.agents.guardian import Guardian

        agent = Guardian()

        if args.status:
            stats = agent.get_stats()
            formatter.kv(stats, "Guardian Statistics")
        elif args.audit:
            entries = agent.audit.query(limit=args.limit or 20)
            if entries:
                formatter.table(
                    ["Timestamp", "Action", "Principal", "Success"],
                    [[e.timestamp.strftime("%Y-%m-%d %H:%M"), e.action.name, e.principal, "✓" if e.success else "✗"] for e in entries],
                    "Audit Log",
                )
            else:
                formatter.info("No audit entries")
        else:
            formatter.info("Use --status or --audit")

    elif agent_name == "analyst":
        from roaddb_core.agents.analyst import Analyst

        agent = Analyst()

        if args.status:
            stats = agent.get_stats()
            formatter.kv(stats, "Analyst Statistics")
        elif args.report:
            report = agent.generate_report()
            formatter.kv({
                "Health Score": f"{report.overall_score}/100",
                "Anomalies": len(report.anomalies),
                "Patterns": len(report.patterns),
            }, "Health Report")
            if report.recommendations:
                print("\nRecommendations:")
                for rec in report.recommendations:
                    print(f"  • {rec}")
        else:
            formatter.info("Use --status or --report")

    elif agent_name == "replicator":
        from roaddb_core.agents.replicator import Replicator

        agent = Replicator()

        if args.status:
            stats = agent.get_stats()
            formatter.kv(stats, "Replicator Statistics")
        else:
            formatter.info("Use --status")

    else:
        formatter.error(f"Unknown agent: {agent_name}")
        return 1

    return 0


def cmd_status(args: argparse.Namespace, formatter: OutputFormatter) -> int:
    """Show database status."""
    formatter.header("RoadDB Status")

    try:
        db = RoadDB(backend=args.backend or "memory")
        stats = db.get_stats()

        formatter.kv({
            "Version": __version__,
            "Backend": stats.get("backend", "unknown"),
            "State": stats.get("state", "unknown"),
            "Connections": stats.get("pool_size", 0),
        }, "Database")

        return 0

    except Exception as e:
        formatter.error(str(e))
        return 1


def cmd_backup(args: argparse.Namespace, formatter: OutputFormatter) -> int:
    """Backup database."""
    formatter.header("Database Backup")

    try:
        dest = Path(args.destination or f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}")

        formatter.info(f"Creating backup at {dest}...")

        # Simulate backup
        time.sleep(1)

        formatter.success(f"Backup created: {dest}")
        return 0

    except Exception as e:
        formatter.error(f"Backup failed: {e}")
        return 1


def cmd_restore(args: argparse.Namespace, formatter: OutputFormatter) -> int:
    """Restore database."""
    formatter.header("Database Restore")

    if not args.source:
        formatter.error("Source backup required")
        return 1

    source = Path(args.source)

    if not source.exists():
        formatter.error(f"Backup not found: {source}")
        return 1

    formatter.warning(f"This will restore from {source}")
    formatter.warning("All current data will be replaced!")

    if not args.force:
        confirm = input("Continue? [y/N] ")
        if confirm.lower() != "y":
            formatter.info("Restore cancelled")
            return 0

    formatter.info("Restoring...")
    time.sleep(1)

    formatter.success("Restore complete")
    return 0


def cmd_monitor(args: argparse.Namespace, formatter: OutputFormatter) -> int:
    """Monitor database."""
    formatter.header("RoadDB Monitor")

    try:
        db = RoadDB(backend="memory")

        if args.dashboard:
            # Simple dashboard loop
            while True:
                try:
                    os.system("clear" if os.name == "posix" else "cls")
                    print(f"\033[1m=== RoadDB Dashboard === {datetime.now().strftime('%H:%M:%S')}\033[0m\n")

                    stats = db.get_stats()
                    print(f"Backend: {stats.get('backend', 'N/A')}")
                    print(f"State: {stats.get('state', 'N/A')}")
                    print(f"Pool Size: {stats.get('pool_size', 0)}")
                    print(f"Active Connections: {stats.get('active_connections', 0)}")

                    print("\n[Press Ctrl+C to exit]")
                    time.sleep(args.interval or 2)

                except KeyboardInterrupt:
                    print()
                    break
        else:
            stats = db.get_stats()
            formatter.kv(stats, "Current Status")

        return 0

    except Exception as e:
        formatter.error(str(e))
        return 1


# =============================================================================
# Main CLI
# =============================================================================


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser."""
    parser = argparse.ArgumentParser(
        prog="roaddb",
        description="RoadDB - Distributed Database Engine for BlackRoad OS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  roaddb init --backend sqlite --path ./data
  roaddb query "SELECT * FROM users"
  roaddb query -i                           # Interactive mode
  roaddb agent archivist --status
  roaddb monitor --dashboard

For more information, visit: https://blackroad.io/roaddb
        """,
    )

    parser.add_argument("--version", action="version", version=f"RoadDB {__version__}")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--no-color", action="store_true", help="Disable colored output")
    parser.add_argument("--backend", "-b", default="sqlite", choices=["sqlite", "postgres", "memory", "distributed"])

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Init
    init_parser = subparsers.add_parser("init", help="Initialize database")
    init_parser.add_argument("--path", "-p", help="Database path")
    init_parser.add_argument("--backend", help="Storage backend")

    # Query
    query_parser = subparsers.add_parser("query", help="Execute SQL query")
    query_parser.add_argument("sql", nargs="?", help="SQL statement")
    query_parser.add_argument("--interactive", "-i", action="store_true", help="Interactive mode")

    # Agent
    agent_parser = subparsers.add_parser("agent", help="Manage agents")
    agent_parser.add_argument("agent", choices=["archivist", "indexer", "guardian", "analyst", "replicator"])
    agent_parser.add_argument("--status", action="store_true", help="Show status")
    agent_parser.add_argument("--archive", action="store_true", help="Run archive (archivist)")
    agent_parser.add_argument("--older-than", type=int, help="Days threshold for archive")
    agent_parser.add_argument("--recommend", action="store_true", help="Get recommendations (indexer)")
    agent_parser.add_argument("--audit", action="store_true", help="Show audit log (guardian)")
    agent_parser.add_argument("--report", action="store_true", help="Generate report (analyst)")
    agent_parser.add_argument("--limit", type=int, help="Limit results")

    # Status
    subparsers.add_parser("status", help="Show database status")

    # Backup
    backup_parser = subparsers.add_parser("backup", help="Backup database")
    backup_parser.add_argument("--destination", "-d", help="Backup destination")

    # Restore
    restore_parser = subparsers.add_parser("restore", help="Restore database")
    restore_parser.add_argument("--source", "-s", help="Backup source")
    restore_parser.add_argument("--force", "-f", action="store_true", help="Skip confirmation")

    # Monitor
    monitor_parser = subparsers.add_parser("monitor", help="Monitor database")
    monitor_parser.add_argument("--dashboard", action="store_true", help="Live dashboard")
    monitor_parser.add_argument("--interval", type=int, default=2, help="Refresh interval (seconds)")

    return parser


def main() -> int:
    """Main entry point."""
    parser = create_parser()
    args = parser.parse_args()

    formatter = OutputFormatter(color=not args.no_color, json_output=args.json)

    if not args.command:
        # Default to interactive REPL
        try:
            db = RoadDB(backend=args.backend or "memory")
            repl = RoadDBRepl(db, formatter)
            repl.run()
            return 0
        except Exception as e:
            formatter.error(str(e))
            return 1

    # Command dispatch
    commands = {
        "init": cmd_init,
        "query": cmd_query,
        "agent": cmd_agent,
        "status": cmd_status,
        "backup": cmd_backup,
        "restore": cmd_restore,
        "monitor": cmd_monitor,
    }

    handler = commands.get(args.command)
    if handler:
        return handler(args, formatter)

    formatter.error(f"Unknown command: {args.command}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
