"""RoadDB Agents - Intelligent data management agents.

Like Lucidia Core's specialized reasoning engines, RoadDB agents are
domain-expert systems that manage different aspects of the database:

| Agent       | Domain          | Responsibilities                          |
|-------------|-----------------|-------------------------------------------|
| Archivist   | Data Lifecycle  | Storage optimization, archival, retention |
| Indexer     | Query Speed     | Index management, query optimization      |
| Guardian    | Security        | Access control, encryption, audit         |
| Analyst     | Intelligence    | Pattern detection, anomaly detection      |
| Replicator  | Consistency     | Replication, consensus, failover          |

Each agent follows the same charter structure as Lucidia agents:
- Seed configuration (YAML codex)
- Behavioral loop
- Input/output channels
- Moral constant and core principle
"""

from roaddb_core.agents.archivist import Archivist
from roaddb_core.agents.indexer import Indexer
from roaddb_core.agents.guardian import Guardian
from roaddb_core.agents.analyst import Analyst
from roaddb_core.agents.replicator import Replicator

__all__ = [
    "Archivist",
    "Indexer",
    "Guardian",
    "Analyst",
    "Replicator",
]
