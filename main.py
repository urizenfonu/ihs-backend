from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from routers import alarms, thresholds, threshold_options, power_flow, energy_mix, composite_alarms, sync, sites, assets, tenants, regional, energy_sources, debug, reports
from apscheduler.schedulers.background import BackgroundScheduler
from services.alarm_monitor import get_alarm_monitor
from services.ihs_sync_service import get_ihs_sync_service
from services.energy_mix_scheduler import update_energy_mix_history, update_energy_mix_history_hourly, run_initial_backfill
from datetime import datetime, timedelta
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s [%(name)s] %(message)s'
)

# Scheduler for background jobs
scheduler = BackgroundScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager - handles startup and shutdown"""

    # STARTUP
    print("=" * 60, flush=True)
    print("[Lifespan] Starting application initialization", flush=True)
    print("=" * 60, flush=True)

    # Run init_system.py
    try:
        print("[Lifespan] Running init_system.py...", flush=True)
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from scripts.init_system import init_system

        success = init_system()
        if not success:
            print("[Lifespan] ⚠️  init_system.py completed with errors", flush=True)
        else:
            print("[Lifespan] ✅ init_system.py completed successfully", flush=True)
    except Exception as e:
        print(f"[Lifespan] ❌ Failed to run init_system.py: {e}", flush=True)
        import traceback
        traceback.print_exc()

    # Sync composite rules to thresholds
    try:
        print("[Lifespan] Syncing composite rules to thresholds...", flush=True)
        from scripts.sync_composite_to_thresholds import sync_rules_to_thresholds
        sync_rules_to_thresholds()
        print("[Lifespan] ✅ Thresholds synced from composite rules", flush=True)
    except Exception as e:
        print(f"[Lifespan] ⚠️  Failed to sync thresholds: {e}", flush=True)

    # Recalculate trigger counts
    try:
        from scripts.recalculate_trigger_counts import recalculate_trigger_counts
        recalculate_trigger_counts()
        print("[Lifespan] ✅ Trigger counts recalculated", flush=True)
    except Exception as e:
        print(f"[Lifespan] ⚠️  Failed to recalculate trigger counts: {e}", flush=True)

    # Fix alarm float precision (one-time migration)
    try:
        from scripts.fix_alarm_precision import fix_alarm_precision
        result = fix_alarm_precision()
        if result >= 0:
            print(f"[Lifespan] ✅ Fixed alarm precision ({result} updated)", flush=True)
    except Exception as e:
        print(f"[Lifespan] ⚠️  Failed to fix alarm precision: {e}", flush=True)

    # Run one-time cleanup of corrupted energy mix data
    try:
        print("[Lifespan] Running energy mix data cleanup...", flush=True)
        from scripts.cleanup_energy_mix_data import cleanup_energy_mix_data
        cleanup_energy_mix_data()
        print("[Lifespan] ✅ Energy mix cleanup completed", flush=True)
    except Exception as e:
        print(f"[Lifespan] ⚠️  Failed to cleanup energy mix data: {e}", flush=True)

    # Run initial backfill of energy mix data
    try:
        print("[Lifespan] Running initial energy mix data backfill...", flush=True)
        run_initial_backfill()
        print("[Lifespan] ✅ Energy mix data backfill completed", flush=True)
    except Exception as e:
        print(f"[Lifespan] ⚠️  Failed to run energy mix backfill: {e}", flush=True)

    # Start schedulers
    print("[Lifespan] Initializing schedulers...", flush=True)

    alarm_monitor = get_alarm_monitor()
    sync_service = get_ihs_sync_service()

    # IHS sync every 30 minutes
    scheduler.add_job(
        sync_service.sync_all,
        'interval',
        minutes=30,
        id='ihs_sync',
        replace_existing=True
    )

    # IHS sync on startup (after 10 seconds)
    scheduler.add_job(
        sync_service.sync_all,
        'date',
        run_date=datetime.now() + timedelta(seconds=10),
        id='ihs_sync_startup'
    )

    # Energy mix update every 30 minutes (current snapshot)
    scheduler.add_job(
        update_energy_mix_history,
        'interval',
        minutes=30,
        id='energy_mix_update',
        replace_existing=True
    )

    # Energy mix update for previous hour (more complete data for the hour)
    scheduler.add_job(
        update_energy_mix_history_hourly,
        'cron',
        minute=5,  # Run 5 minutes after each hour to ensure data is available
        id='energy_mix_hourly_update',
        replace_existing=True
    )

    # Alarm evaluation every 2 minutes
    scheduler.add_job(
        alarm_monitor.evaluate_all_assets,
        'interval',
        minutes=2,
        id='alarm_evaluation',
        replace_existing=True
    )

    scheduler.start()
    print("[Lifespan] Schedulers started (IHS sync: 30min, Alarms: 2min)", flush=True)

    yield  # Application runs here

    # SHUTDOWN
    print("[Lifespan] Stopping schedulers...", flush=True)
    scheduler.shutdown(wait=False)
    print("[Lifespan] Shutdown complete", flush=True)

app = FastAPI(title="IHS Backend API", lifespan=lifespan)

CORS_ORIGINS = [origin.strip() for origin in os.getenv("CORS_ORIGINS", "").split(",") if origin.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS or ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(alarms.router, prefix="/api/alarms", tags=["alarms"])
app.include_router(thresholds.router, prefix="/api/thresholds", tags=["thresholds"])
app.include_router(threshold_options.router, prefix="/api", tags=["threshold-options"])
app.include_router(power_flow.router, prefix="/api", tags=["dashboard"])
app.include_router(energy_mix.router, prefix="/api", tags=["dashboard"])
app.include_router(composite_alarms.router, prefix="/api/alarms/composite", tags=["composite-alarms"])
app.include_router(sync.router, prefix="/api", tags=["sync"])
app.include_router(sites.router, prefix="/api", tags=["sites"])
app.include_router(assets.router, prefix="/api", tags=["assets"])
app.include_router(tenants.router, prefix="/api", tags=["tenants"])
app.include_router(regional.router, prefix="/api", tags=["regional"])
app.include_router(energy_sources.router, prefix="/api", tags=["energy-sources"])
app.include_router(debug.router, prefix="/api", tags=["debug"])
app.include_router(reports.router, prefix="/api/reports", tags=["reports"])

@app.get("/health")
def health():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3001)
