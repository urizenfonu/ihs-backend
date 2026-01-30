# IHS Backend API

Python FastAPI backend for IHS monitoring system.

## Local Development

```bash
# Install dependencies
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Initialize database
python3 scripts/init_system.py

# Load Excel payloads (assets + readings)
python3 scripts/load_excel_data.py

# Run development server
uvicorn main:app --reload --port 3001

# Or
python3 main.py
```

## API Docs

- Swagger UI: http://localhost:3001/docs
- ReDoc: http://localhost:3001/redoc
- Health: http://localhost:3001/health

## Endpoints

### Alarms
- `GET /api/alarms` - List alarms (filter: status, severity, category, site)
- `PUT /api/alarms/{id}` - Update alarm status
- `DELETE /api/alarms/{id}` - Delete alarm
- `POST /api/alarms/clear?action=archive|delete` - Clear alarms (does not change thresholds)

By default, alarms are cleared on every server restart (archived) so the UI starts with a clean list.
To preserve alarms across restarts, set `ALARMS_CLEAR_ON_STARTUP=off`. To permanently delete instead of archive, set `ALARMS_CLEAR_ON_STARTUP=delete`.

### Thresholds
- `GET /api/thresholds` - List thresholds
- `POST /api/thresholds` - Create threshold
- `PUT /api/thresholds/{id}` - Update threshold
- `DELETE /api/thresholds/{id}` - Delete threshold

### Dashboard
- `GET /api/power-flow` - Current power flow data
- `GET /api/energy-mix` - 24hr energy mix chart data

## Deployment

### Railway
```bash
railway login
railway init
railway up
```

### Fly.io
```bash
fly launch
fly deploy
```

## Database

SQLite database located at `data/ihs.db`. Schema auto-initialized on first run.
After initializing the schema, run `python3 scripts/load_excel_data.py` to seed
the assets/readings tables with the latest measurements extracted from the Excel
files in `IHS Payload/`. This ensures the `/api/energy-mix`, `/api/power-flow`,
and `/api/alarms` endpoints serve realistic values derived directly from the
provided payloads.
