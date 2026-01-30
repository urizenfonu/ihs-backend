#!/usr/bin/env python3
"""
Initialize IHS System

This script:
1. Initializes the database schema
2. Creates Lagos site if not exists
3. Seeds default thresholds from Excel data
"""

import sys
import os

# Enable unbuffered output for deployment logging
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.client import get_database
from db.repositories.site_repository import SiteRepository

def init_system():
    print("=" * 60, flush=True)
    print("INIT_SYSTEM.PY STARTING", flush=True)
    print(f"Current working directory: {os.getcwd()}", flush=True)
    print(f"DATABASE_PATH: {os.getenv('DATABASE_PATH', '../data/ihs.db')}", flush=True)
    print("=" * 60, flush=True)

    print("🚀 Initializing IHS System...\n", flush=True)

    # Step 1: Initialize database
    print("📊 Step 1: Initializing database schema...", flush=True)
    try:
        db = get_database()
        print("✅ Database initialized\n", flush=True)
    except Exception as e:
        print(f"❌ Failed to initialize database: {e}", flush=True)
        import traceback
        traceback.print_exc()
        return False

    # Step 2: Create Lagos site
    print("🏢 Step 2: Creating Lagos site...")
    try:
        site_repo = SiteRepository()
        lagos_id = site_repo.create_lagos_if_not_exists()
        print(f"✅ Lagos site ready (ID: {lagos_id})\n")
    except Exception as e:
        print(f"❌ Failed to create Lagos site: {e}")
        return False

    # Step 3: Populate composite rules
    print("⚙️  Step 3: Populating composite rules...", flush=True)
    try:
        from scripts.populate_composite_rules import populate_composite_rules
        populate_composite_rules()
    except Exception as e:
        print(f"❌ Failed to populate composite rules: {e}", flush=True)
        return False

    print("✨ System initialization complete!")

    # Count actual resources in DB
    try:
        from db.repositories.threshold_repository import ThresholdRepository
        threshold_repo = ThresholdRepository()
        actual_thresholds = threshold_repo.get_all()
        threshold_count = len(actual_thresholds)
    except:
        threshold_count = "unknown"

    try:
        import sqlite3
        conn = sqlite3.connect(os.getenv('DATABASE_PATH', '../data/ihs.db'))
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM composite_rules")
        composite_count = cursor.fetchone()[0]
        conn.close()
    except:
        composite_count = "unknown"

    print(f"\nSeeded resources:")
    print(f"- Sites: 1 (Lagos)")
    print(f"- Composite Rules: {composite_count} (source of truth)")
    print(f"- Thresholds: {threshold_count} (synced from composite_rules)")
    print("\nNext steps:")
    print("- Run backend: python3 main.py or uvicorn main:app --reload")
    print("- Sync thresholds: python3 scripts/sync_composite_to_thresholds.py")
    print("- API docs: http://localhost:3001/docs")

    return True

if __name__ == "__main__":
    success = init_system()
    sys.exit(0 if success else 1)
