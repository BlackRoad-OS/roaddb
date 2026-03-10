# RoadDB

Fully managed PostgreSQL, MongoDB, and Redis databases.

## Part of [BlackRoad OS](https://blackroad.io)

RoadDB provides managed database infrastructure with auto-scaling, automated backups, and zero-downtime deployments.

### Supported Databases

| Database   | Versions      | Features                                    |
|-----------|---------------|---------------------------------------------|
| PostgreSQL | 16, 17       | PostGIS, pgvector, connection pooling, PITR |
| MongoDB    | 7.x, 8.x    | Replica sets, sharding, change streams      |
| Redis      | 7.x         | Cluster mode, Pub/Sub, Streams, persistence |

### Quick Start

```python
# PostgreSQL
import psycopg2
conn = psycopg2.connect("postgresql://user:pass@your-db.roaddb.blackroad.io:5432/myapp")

# MongoDB
from pymongo import MongoClient
client = MongoClient("mongodb+srv://user:pass@your-db.roaddb.blackroad.io/myapp")

# Redis
import redis
r = redis.Redis.from_url("rediss://user:pass@your-db.roaddb.blackroad.io:6379")
```

### Links

- [Product Page](https://blackroad.io/roaddb)
- [Documentation](https://docs.blackroad.io/roaddb)
- [API Reference](https://api.blackroad.io/roaddb)
- [System Status](https://status.blackroad.io)
- [Support](https://support.blackroad.io)

### License

Copyright 2024-2026 BlackRoad OS, Inc. All rights reserved. See [LICENSE](LICENSE).
