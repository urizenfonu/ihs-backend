import sqlite3
import os
import threading
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

_thread_local = threading.local()
_db_path = None
_initialized = False

def get_database() -> sqlite3.Connection:
    global _db_path, _initialized

    # Get thread-local connection
    if hasattr(_thread_local, 'connection') and _thread_local.connection:
        return _thread_local.connection

    # Determine database path (only once)
    if _db_path is None:
        db_path_str = os.getenv('DATABASE_PATH')
        if db_path_str:
            _db_path = Path(db_path_str)
            _db_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            data_dir = Path(__file__).parent.parent / 'data'
            data_dir.mkdir(parents=True, exist_ok=True)
            _db_path = data_dir / 'ihs.db'

    # Create thread-local connection
    _thread_local.connection = sqlite3.connect(str(_db_path), check_same_thread=True)
    _thread_local.connection.row_factory = sqlite3.Row

    readonly = False
    try:
        # Enable WAL mode (may fail in read-only environments, e.g. sandboxed CI).
        _thread_local.connection.execute('PRAGMA journal_mode = WAL')
    except sqlite3.OperationalError:
        readonly = True

    try:
        _thread_local.connection.execute('PRAGMA foreign_keys = ON')
    except sqlite3.OperationalError:
        pass

    # Initialize schema only once
    if not _initialized and not readonly:
        try:
            initialize_schema()
        except sqlite3.OperationalError:
            # Best-effort; avoid crashing in read-only deployments.
            pass
        _initialized = True

    return _thread_local.connection

def ensure_column(db, table: str, column: str, ddl: str):
    cursor = db.execute(f"PRAGMA table_info({table})")
    columns = {row[1] for row in cursor.fetchall()}
    if column not in columns:
        db.execute(ddl)
        db.commit()

def run_migrations():
    db = get_database()
    migrations_dir = Path(__file__).parent / 'migrations'
    if not migrations_dir.exists():
        return

    # Create migrations tracking table
    db.execute('''
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    db.commit()

    # Get applied migrations
    cursor = db.execute('SELECT version FROM schema_migrations')
    applied = {row[0] for row in cursor.fetchall()}

    # Apply new migrations
    for migration_file in sorted(migrations_dir.glob('*.sql')):
        version = migration_file.stem
        if version not in applied:
            print(f"[Migration] Applying {version}...")
            with open(migration_file) as f:
                try:
                    db.executescript(f.read())
                except sqlite3.OperationalError as exc:
                    if (
                        version == '005_add_assets_tenant_channels'
                        and 'duplicate column name: tenant_channels' in str(exc)
                    ) or (
                        version == '006_add_alarms_composite_rule_id'
                        and 'duplicate column name: composite_rule_id' in str(exc)
                    ):
                        print(f"[Migration] Skipping {version}; column already exists")
                    elif 'duplicate column name' in str(exc):
                        print(f"[Migration] Skipping {version}; column already exists")
                    else:
                        raise
            db.row_factory = sqlite3.Row
            db.execute('INSERT INTO schema_migrations (version) VALUES (?)', (version,))
            db.commit()
            print(f"[Migration] Applied {version}")

def initialize_schema():
    db = get_database()
    schema_path = Path(__file__).parent / 'schema.sql'
    if schema_path.exists():
        with open(schema_path, 'r') as f:
            schema = f.read()
        db.executescript(schema)
        db.row_factory = sqlite3.Row
        db.commit()

    # Ensure expected columns exist before migrations
    ensure_column(db, 'sites', 'external_id', 'ALTER TABLE sites ADD COLUMN external_id INTEGER')
    ensure_column(db, 'sites', 'state', 'ALTER TABLE sites ADD COLUMN state TEXT')
    ensure_column(db, 'sites', 'cluster_code', 'ALTER TABLE sites ADD COLUMN cluster_code TEXT')
    ensure_column(db, 'sites', 'zone_external_id', 'ALTER TABLE sites ADD COLUMN zone_external_id INTEGER')

    # Run migrations after schema initialization
    run_migrations()

def close_database():
    if hasattr(_thread_local, 'connection') and _thread_local.connection:
        _thread_local.connection.close()
        _thread_local.connection = None
