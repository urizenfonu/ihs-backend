import aiosqlite
import json
from typing import List, Optional
from datetime import datetime, timedelta
from models import CompositeRule, Alarm
from pathlib import Path

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    async def init_schema(self):
        """Initialize database schema"""
        async with aiosqlite.connect(self.db_path) as db:
            # Create composite_rules table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS composite_rules (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    severity TEXT NOT NULL,
                    category TEXT NOT NULL,
                    rule_type TEXT NOT NULL,
                    enabled BOOLEAN DEFAULT 1,
                    conditions TEXT NOT NULL,
                    logical_operator TEXT,
                    time_window_minutes INTEGER,
                    aggregation_type TEXT,
                    applies_to TEXT DEFAULT 'all',
                    region_id TEXT,
                    cluster_id TEXT,
                    site_id TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indexes
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_composite_rules_enabled
                ON composite_rules(enabled, category)
            """)

            # Extend alarms table if it exists
            try:
                await db.execute("""
                    ALTER TABLE alarms ADD COLUMN composite_rule_id TEXT
                """)
            except:
                pass

            try:
                await db.execute("""
                    ALTER TABLE alarms ADD COLUMN conditions_met INTEGER
                """)
            except:
                pass

            try:
                await db.execute("""
                    ALTER TABLE alarms ADD COLUMN total_conditions INTEGER
                """)
            except:
                pass

            await db.commit()

    async def insert_rule(self, rule: dict):
        """Insert a composite rule"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT OR REPLACE INTO composite_rules
                (id, name, description, severity, category, rule_type, enabled,
                 conditions, logical_operator, time_window_minutes, aggregation_type, applies_to)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                rule['id'],
                rule['name'],
                rule.get('description'),
                rule['severity'],
                rule['category'],
                rule['rule_type'],
                rule.get('enabled', True),
                json.dumps(rule['conditions']),
                rule.get('logical_operator'),
                rule.get('time_window_minutes'),
                rule.get('aggregation_type'),
                rule.get('applies_to', 'all')
            ))
            await db.commit()

    async def get_rules(self, category: Optional[str] = None, enabled: bool = True) -> List[CompositeRule]:
        """Get all rules or filter by category"""
        async with aiosqlite.connect(self.db_path) as db:
            if category:
                cursor = await db.execute("""
                    SELECT id, name, description, severity, category, rule_type, enabled,
                           conditions, logical_operator, time_window_minutes, aggregation_type,
                           applies_to, region_id, cluster_id, site_id
                    FROM composite_rules
                    WHERE category = ? AND enabled = ?
                """, (category, enabled))
            else:
                cursor = await db.execute("""
                    SELECT id, name, description, severity, category, rule_type, enabled,
                           conditions, logical_operator, time_window_minutes, aggregation_type,
                           applies_to, region_id, cluster_id, site_id
                    FROM composite_rules
                    WHERE enabled = ?
                """, (enabled,))

            rows = await cursor.fetchall()
            rules = []
            for row in rows:
                rules.append(CompositeRule(
                    id=row[0],
                    name=row[1],
                    description=row[2],
                    severity=row[3],
                    category=row[4],
                    rule_type=row[5],
                    enabled=bool(row[6]),
                    conditions=json.loads(row[7]),
                    logical_operator=row[8],
                    time_window_minutes=row[9],
                    aggregation_type=row[10],
                    applies_to=row[11],
                    region_id=row[12],
                    cluster_id=row[13],
                    site_id=row[14]
                ))
            return rules

    async def get_rules_for_asset(self, asset_id: int) -> List[CompositeRule]:
        """Get applicable rules for an asset"""
        # For now, return all enabled rules
        # TODO: Filter by site/region/cluster if needed
        return await self.get_rules(enabled=True)

    async def count_rules(self) -> int:
        """Count total rules"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM composite_rules")
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_readings_window(self, asset_id: int, minutes: int) -> List[dict]:
        """Get readings for an asset within time window"""
        async with aiosqlite.connect(self.db_path) as db:
            cutoff_time = datetime.now() - timedelta(minutes=minutes)
            cursor = await db.execute("""
                SELECT data, timestamp FROM readings
                WHERE asset_id = ? AND timestamp >= ?
                ORDER BY timestamp DESC
            """, (asset_id, cutoff_time.isoformat()))

            rows = await cursor.fetchall()
            readings = []
            for row in rows:
                try:
                    data = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                    readings.append({"data": data, "timestamp": row[1]})
                except:
                    continue
            return readings

    async def get_previous_reading(self, asset_id: int):
        """Get the most recent reading for an asset"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT data, timestamp FROM readings
                WHERE asset_id = ?
                ORDER BY timestamp DESC
                LIMIT 2
            """, (asset_id,))

            rows = await cursor.fetchall()
            if len(rows) >= 2:
                data = json.loads(rows[1][0]) if isinstance(rows[1][0], str) else rows[1][0]
                return {"data": data, "timestamp": rows[1][1]}
            return None

    async def has_active_composite_alarm(self, asset_id: int, composite_rule_id: str, severity: str) -> bool:
        """Check for existing active/acknowledged composite alarm for an asset."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                SELECT 1 FROM alarms
                WHERE asset_id = ?
                  AND composite_rule_id = ?
                  AND severity = ?
                  AND status IN ('active', 'acknowledged')
                LIMIT 1
                """,
                (asset_id, composite_rule_id, severity)
            )
            return await cursor.fetchone() is not None

    async def create_alarm(self, alarm: Alarm):
        """Create a new alarm"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO alarms
                (id, timestamp, site, region, severity, category, message, status,
                 composite_rule_id, asset_id, conditions_met, total_conditions)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                alarm.id,
                alarm.timestamp.isoformat(),
                alarm.site,
                alarm.region,
                alarm.severity,
                alarm.category,
                alarm.message,
                alarm.status,
                alarm.composite_rule_id,
                alarm.asset_id,
                alarm.conditions_met,
                alarm.total_conditions
            ))
            await db.commit()

    async def get_alarms(
        self,
        status: str = "active",
        severity: Optional[str] = None,
        site: Optional[str] = None
    ) -> List[dict]:
        """Get alarms with filters"""
        async with aiosqlite.connect(self.db_path) as db:
            query = "SELECT * FROM alarms WHERE status = ?"
            params = [status]

            if severity:
                query += " AND severity = ?"
                params.append(severity)

            if site:
                query += " AND site = ?"
                params.append(site)

            query += " ORDER BY timestamp DESC LIMIT 1000"

            cursor = await db.execute(query, params)
            rows = await cursor.fetchall()

            # Get column names
            columns = [description[0] for description in cursor.description]

            alarms = []
            for row in rows:
                alarm = dict(zip(columns, row))
                alarms.append(alarm)

            return alarms
