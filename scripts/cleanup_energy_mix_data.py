"""
One-time cleanup script to remove corrupted energy_mix_history data.
This script clears old data with incorrect key format and runs only once.
"""
import logging
from db.client import get_database

logger = logging.getLogger(__name__)

def cleanup_energy_mix_data():
    """Remove only corrupted records with JSON objects in numeric columns."""
    db = get_database()

    try:
        cursor = db.execute("""
            DELETE FROM energy_mix_history
            WHERE CAST(grid AS TEXT) LIKE '{%'
               OR CAST(generator AS TEXT) LIKE '{%'
               OR CAST(solar AS TEXT) LIKE '{%'
               OR CAST(battery AS TEXT) LIKE '{%'
        """)
        deleted = cursor.rowcount
        db.commit()

        if deleted > 0:
            logger.info(f"Deleted {deleted} corrupted energy mix records")
        else:
            logger.info("No corrupted energy mix records found")

        return deleted
    except Exception as e:
        logger.error(f"Failed to cleanup energy mix data: {e}")
        db.rollback()
        return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cleanup_energy_mix_data()
