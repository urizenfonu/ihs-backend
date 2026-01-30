"""
One-time cleanup script to remove corrupted energy_mix_history data.
This script clears old data with incorrect key format and runs only once.
"""
import logging
from db.client import get_database

logger = logging.getLogger(__name__)

CLEANUP_FLAG_KEY = "energy_mix_cleanup_v1_completed"

def cleanup_energy_mix_data():
    """Clean up corrupted energy mix data from database (runs once)."""
    db = get_database()

    try:
        # Create system_flags table if it doesn't exist
        db.execute("""
            CREATE TABLE IF NOT EXISTS system_flags (
                key TEXT PRIMARY KEY,
                value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db.commit()

        # Check if cleanup already ran
        cursor = db.execute(
            "SELECT value FROM system_flags WHERE key = ?",
            (CLEANUP_FLAG_KEY,)
        )
        result = cursor.fetchone()

        if result and result['value'] == '1':
            logger.info("Energy mix cleanup already completed, skipping")
            return True

        logger.info("Starting energy mix data cleanup...")

        # Delete all old data (will be repopulated by scheduler)
        cursor = db.execute("DELETE FROM energy_mix_history")
        deleted_count = cursor.rowcount
        db.commit()

        logger.info(f"Deleted {deleted_count} old energy mix records")

        # Set cleanup flag
        db.execute(
            "INSERT OR REPLACE INTO system_flags (key, value) VALUES (?, ?)",
            (CLEANUP_FLAG_KEY, '1')
        )
        db.commit()

        logger.info("Energy mix cleanup completed successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to cleanup energy mix data: {e}")
        db.rollback()
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cleanup_energy_mix_data()
