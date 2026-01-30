#!/usr/bin/env python3
"""Regression test for /power-flow site scoping.

Run: ./venv/bin/python test_power_flow.py

This specifically covers the fallback path in routers/power_flow._resolve_site_ids()
when there are no readings yet; historically this raised:
    sqlite3.OperationalError: no such column: s.region
"""

import os
import tempfile


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test_ihs.db")
        os.environ["DATABASE_PATH"] = db_path

        from db.client import close_database, get_database  # import after env var set
        from routers import power_flow

        db = get_database()

        # Ensure schema exists (get_database initializes on first connect).
        # Populate only the sites table; leave assets/readings empty to force fallback.
        db.execute(
            "INSERT INTO sites (name, region, zone, state) VALUES (?, ?, ?, ?)",
            ("Site A", "South", "South", "Rivers"),
        )
        db.execute(
            "INSERT INTO sites (name, region, zone, state) VALUES (?, ?, ?, ?)",
            ("Site B", "Lagos", "Lagos", "Lagos"),
        )
        db.commit()

        site_ids, state_label, label = power_flow._resolve_site_ids(
            region="South", state=None, site=None, sample_size=10
        )
        assert site_ids, "expected at least one site id"
        assert label == "South"
        assert state_label is None

        site_ids2, state_label2, label2 = power_flow._resolve_site_ids(
            region="South", state="Rivers", site=None, sample_size=10
        )
        assert site_ids2 == site_ids
        assert label2 == "South"
        assert state_label2 == "Rivers"

        close_database()

    print("✅ power_flow scope regression test passed")


if __name__ == "__main__":
    main()
