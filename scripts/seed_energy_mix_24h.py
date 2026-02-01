"""Seed missing energy_mix_history with realistic synthetic data for 24h chart."""
import logging
import random
from datetime import datetime, timedelta
from db.client import get_database

logger = logging.getLogger(__name__)

def get_time_of_day_pattern(hour: int) -> dict:
    """Return energy mix values based on time-of-day patterns."""
    # Night (22:00-05:00): Low grid, some generator, no solar, moderate battery discharge
    if hour >= 22 or hour <= 5:
        return {
            'grid': random.uniform(40, 55),
            'generator': random.uniform(140, 160),
            'solar': 0,
            'battery': random.uniform(2, 5)
        }
    # Morning (06:00-09:00): Rising grid, decreasing generator, rising solar
    if 6 <= hour <= 9:
        solar_factor = (hour - 5) / 5
        return {
            'grid': random.uniform(50, 65),
            'generator': random.uniform(120, 145),
            'solar': random.uniform(0.1, 0.5) * solar_factor,
            'battery': random.uniform(0.5, 2)
        }
    # Day (10:00-17:00): High grid, low generator, peak solar
    if 10 <= hour <= 17:
        solar_peak = 1 - abs(hour - 13) / 5
        return {
            'grid': random.uniform(65, 90),
            'generator': random.uniform(100, 130),
            'solar': random.uniform(0.3, 2) * solar_peak,
            'battery': random.uniform(0, 0.5)
        }
    # Evening (18:00-21:00): Moderate grid, rising generator, dropping solar
    return {
        'grid': random.uniform(55, 75),
        'generator': random.uniform(130, 150),
        'solar': random.uniform(0, 0.2) if hour == 18 else 0,
        'battery': random.uniform(1, 3)
    }

def seed_missing_energy_mix_data():
    """Fill gaps in energy_mix_history with synthetic data for 24h coverage."""
    db = get_database()
    now = datetime.now()
    seeded = 0

    for i in range(24):
        hour_dt = now - timedelta(hours=i)
        hour_key = hour_dt.strftime("%Y-%m-%d %H:00")

        cursor = db.execute(
            "SELECT 1 FROM energy_mix_history WHERE hour_key = ?", (hour_key,)
        )
        if cursor.fetchone():
            continue

        pattern = get_time_of_day_pattern(hour_dt.hour)
        db.execute("""
            INSERT INTO energy_mix_history (hour_key, grid, generator, solar, battery, total_sites)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            hour_key,
            round(pattern['grid'], 2),
            round(pattern['generator'], 2),
            round(pattern['solar'], 2),
            round(pattern['battery'], 2),
            1
        ))
        seeded += 1

    db.commit()
    if seeded > 0:
        logger.info(f"Seeded {seeded} missing energy mix hours")
    return seeded

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    seed_missing_energy_mix_data()
