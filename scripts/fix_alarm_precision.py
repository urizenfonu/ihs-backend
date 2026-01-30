#!/usr/bin/env python3
"""Fix floating point precision and frequency scaling in existing alarm messages and details."""
import sys
import re
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.client import get_database

MIGRATION_VERSION = 'fix_alarm_precision_002'


def fix_float_precision(text: str) -> str:
    """Replace floats with many decimals to 2 decimal places."""
    return re.sub(
        r'(\d+\.\d{3,})',
        lambda m: f"{float(m.group(1)):.2f}",
        text
    )


def fix_frequency_scaling(text: str) -> str:
    """Fix frequency values > 100Hz by dividing by 10."""
    def scale_hz(m):
        val = float(m.group(1))
        unit = m.group(2)
        if val > 100:
            val = val / 10
        return f"{val:.1f}{unit}"
    return re.sub(r'(\d+\.?\d*)(Hz|HZ)', scale_hz, text)


def fix_alarm_precision() -> int:
    """Fix precision and frequency scaling in alarms. Returns count of updated records, or -1 if already applied."""
    db = get_database()

    cursor = db.execute('SELECT version FROM schema_migrations WHERE version = ?', (MIGRATION_VERSION,))
    if cursor.fetchone():
        return -1

    cursor = db.execute('SELECT id, message, details FROM alarms')
    alarms = cursor.fetchall()

    updated = 0
    for alarm in alarms:
        alarm_id = alarm['id']
        message = alarm['message'] or ''
        details = alarm['details'] or '{}'

        new_message = fix_frequency_scaling(fix_float_precision(message))

        try:
            details_dict = json.loads(details)
            if 'currentValue' in details_dict:
                details_dict['currentValue'] = fix_frequency_scaling(fix_float_precision(details_dict['currentValue']))
            new_details = json.dumps(details_dict)
        except json.JSONDecodeError:
            new_details = details

        if new_message != message or new_details != details:
            db.execute(
                'UPDATE alarms SET message = ?, details = ? WHERE id = ?',
                (new_message, new_details, alarm_id)
            )
            updated += 1

    db.execute('INSERT INTO schema_migrations (version) VALUES (?)', (MIGRATION_VERSION,))
    db.commit()
    return updated


if __name__ == '__main__':
    result = fix_alarm_precision()
    if result == -1:
        print("Migration already applied")
    else:
        print(f"Updated {result} alarms")
