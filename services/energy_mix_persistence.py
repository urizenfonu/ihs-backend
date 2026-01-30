from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
from db.client import get_database
import logging

logger = logging.getLogger(__name__)

def initialize_energy_mix_table():
    """Initialize the energy_mix_history table if it doesn't exist."""
    db = get_database()
    
    db.execute("""
        CREATE TABLE IF NOT EXISTS energy_mix_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hour_key TEXT NOT NULL,
            grid REAL DEFAULT 0,
            generator REAL DEFAULT 0,
            solar REAL DEFAULT 0,
            battery REAL DEFAULT 0,
            total_sites INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(hour_key)
        )
    """)
    
    # Create index for faster queries
    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_energy_mix_hour ON energy_mix_history (hour_key)
    """)
    
    db.commit()


def store_energy_mix_snapshot(hour_key: str, energy_mix: Dict[str, float], total_sites: int = 0):
    """Store an energy mix snapshot in the persistent table."""
    db = get_database()
    
    try:
        db.execute("""
            INSERT OR REPLACE INTO energy_mix_history 
            (hour_key, grid, generator, solar, battery, total_sites, created_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            hour_key,
            energy_mix.get('grid', 0),
            energy_mix.get('generator', 0),
            energy_mix.get('solar', 0),
            energy_mix.get('battery', 0),
            total_sites
        ))
        db.commit()
        logger.info(f"Stored energy mix snapshot for {hour_key}")
    except Exception as e:
        logger.error(f"Failed to store energy mix snapshot: {e}")
        db.rollback()


def get_historical_energy_mix(hours_back: int = 24) -> List[Dict]:
    """Retrieve historical energy mix data for the specified number of hours."""
    db = get_database()
    
    # Generate the expected hour keys for the past N hours
    now = datetime.now()
    expected_hours = []
    hour_objs = []
    for i in range(hours_back - 1, -1, -1):  # Going backwards from now
        hour = now - timedelta(hours=i)
        hour_key = hour.strftime("%Y-%m-%d %H:00")
        expected_hours.append(hour_key)
        hour_objs.append(hour)
    
    # Query for existing data
    placeholders = ','.join(['?' for _ in expected_hours])
    query = f"""
        SELECT hour_key, grid, generator, solar, battery, created_at
        FROM energy_mix_history 
        WHERE hour_key IN ({placeholders})
        ORDER BY hour_key
    """
    
    cursor = db.execute(query, expected_hours)
    results = cursor.fetchall()
    
    # Convert to list of dicts
    stored_data = {row['hour_key']: {
        'grid': row['grid'],
        'generator': row['generator'],
        'solar': row['solar'],
        'battery': row['battery']
    } for row in results}

    # Fill in missing hours with zeros
    result_list = []
    for i, hour_key in enumerate(expected_hours):
        hour_obj = hour_objs[i]
        if hour_key in stored_data:
            result_list.append({
                'time': hour_obj.strftime("%H:00"),
                **stored_data[hour_key]
            })
        else:
            result_list.append({
                'time': hour_obj.strftime("%H:00"),
                'grid': 0.0,
                'generator': 0.0,
                'solar': 0.0,
                'battery': 0.0
            })
    
    return result_list


def cleanup_old_records(days_to_keep: int = 30):
    """Remove records older than the specified number of days."""
    db = get_database()
    
    cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime('%Y-%m-%d')
    
    try:
        cursor = db.execute(
            "DELETE FROM energy_mix_history WHERE created_at < ?",
            (cutoff_date,)
        )
        deleted_count = cursor.rowcount
        db.commit()
        
        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} old energy mix records")
        
        return deleted_count
    except Exception as e:
        logger.error(f"Failed to clean up old energy mix records: {e}")
        return 0


def get_energy_mix_summary(start_date: str, end_date: str) -> Dict:
    """Get summary statistics for energy mix over a date range."""
    db = get_database()
    
    query = """
        SELECT 
            AVG(grid) as avg_grid,
            AVG(generator) as avg_generator,
            AVG(solar) as avg_solar,
            AVG(battery) as avg_battery,
            SUM(grid) as total_grid,
            SUM(generator) as total_generator,
            SUM(solar) as total_solar,
            SUM(battery) as total_battery,
            COUNT(*) as record_count
        FROM energy_mix_history
        WHERE created_at BETWEEN ? AND ?
    """
    
    cursor = db.execute(query, (start_date, end_date))
    result = cursor.fetchone()
    
    return {
        'avg_grid': result['avg_grid'] or 0,
        'avg_generator': result['avg_generator'] or 0,
        'avg_solar': result['avg_solar'] or 0,
        'avg_battery': result['avg_battery'] or 0,
        'total_grid': result['total_grid'] or 0,
        'total_generator': result['total_generator'] or 0,
        'total_solar': result['total_solar'] or 0,
        'total_battery': result['total_battery'] or 0,
        'record_count': result['record_count'] or 0
    }