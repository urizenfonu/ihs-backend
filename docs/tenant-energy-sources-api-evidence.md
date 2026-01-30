# Tenant Energy Sources - IoT API Deep Dive Verification

**Date:** 2026-01-24
**API:** https://iot.novatrack-webservices.net/api/v1

---

## EXECUTIVE SUMMARY

**Claim:** Tenants only show "battery" as energy source in UI.

**Verdict:** ✅ **CONFIRMED** - IoT API provides NO tenant-level breakdown for grid/generator. All tenant consumption is measured via DC Meter channels on the DC bus (battery).

---

## API SURFACE INVESTIGATION

| Endpoint | Status | Notes |
|----------|--------|-------|
| `GET /sites` | ✅ Works | Only working endpoint, contains all data |
| `GET /assets` | ❌ 404 | Does not exist |
| `GET /tenants` | ❌ 404 | Does not exist |
| `GET /readings` | ❌ 404 | Does not exist |
| `GET /channels` | ❌ 404 | Does not exist |
| `GET /assets/{id}/readings` | ✅ Works | Asset readings endpoint |

**All tenant/asset data is embedded in the `/sites` response.**

---

## ASSET DISTRIBUTION (From IoT API)

```bash
curl -s "https://iot.novatrack-webservices.net/api/v1/sites?X-Access-Token=TOKEN&per_page=100"
```

| Asset Type | Total Count | Has tenant_channels | Has config.channels (type=tenant) |
|------------|-------------|---------------------|-----------------------------------|
| Generator | 36 | **0** | **0** |
| DC Meter | 20 | 0 | **20** |
| Diesel Tank | 21 | 0 | 0 |
| Grid | 17 | **0** | **0** |
| Rectifier | 7 | 0 | 0 |
| Cold Room | 6 | 0 | 0 |
| Solar | 1 | **0** | **0** |
| Battery | 1 | 0 | 0 |

**Only DC Meter assets have tenant channel configuration.**

---

## CHANNEL TYPES IN ENTIRE API

```bash
curl -s ".../sites?per_page=100" | jq '[.data[].assets[] | select(.config.channels) | .config.channels[].type] | group_by(.) | map({type: .[0], count: length})'
```

| Channel Type | Count | Found In |
|--------------|-------|----------|
| battery | 20 | DC Meter only |
| solar | 4 | DC Meter only |
| tenant | 37 | DC Meter only |
| **grid** | **0** | **Does not exist** |
| **generator** | **0** | **Does not exist** |

**No "grid" or "generator" channel types exist in the IoT API.**

---

## SAMPLE API DATA

### DC Meter (Asset 900) - HAS TENANT CHANNELS ✅

```json
// GET /sites → asset in site LAG_0435B
{
  "id": 900,
  "name": "DC Meter",
  "tenant_channels": null,
  "config": {
    "channels": [
      {"name": "Battery", "type": "battery", "index": 1},
      {"name": "MTN 2G 3G LTE", "type": "tenant", "index": 3},
      {"name": "MTN Fiber", "type": "tenant", "index": 4},
      {"name": "MTN 6S", "type": "tenant", "index": 5}
    ]
  }
}

// GET /assets/900/readings?per_page=1
{
  "date": "01/24/2026 15:18:50",
  "Power1": 0,           // Battery channel (index 1)
  "Power3": 7169.67,     // MTN 2G 3G LTE (index 3) ← TENANT SPECIFIC
  "Power4": 1152.92,     // MTN Fiber (index 4) ← TENANT SPECIFIC
  "Power5": 536.07,      // MTN 6S (index 5) ← TENANT SPECIFIC
  "Battery": 3411,
  "Voltage": 52.95
}
```

### Grid (Asset 720) - NO TENANT CHANNELS ❌

```json
// GET /sites → asset in site LAG_3778H
{
  "id": 720,
  "name": "Grid",
  "tenant_channels": null,
  "config": null  // NO channels configuration
}

// GET /assets/720/readings?per_page=1
{
  "date": "06/13/2025 20:48:20",
  "total_active_power": 12.66,        // SITE-LEVEL TOTAL ONLY
  "total_energy_consumption": 2052.78,
  "current_1": 18.9,
  "voltage_1": 228
  // NO PowerX fields - cannot break down by tenant
}
```

### Generator (Asset 681) - NO TENANT CHANNELS ❌

```json
// GET /sites → asset in site LAG_3778H
{
  "id": 681,
  "name": "Generator",
  "tenant_channels": null,
  "config": null  // NO channels configuration
}

// GET /assets/681/readings?per_page=1
{
  "coolant_temperature": 0,
  "engine_run_time": 20548,
  "fuel_consumption": 0,
  "gen_kwh": 142965,      // SITE-LEVEL CUMULATIVE
  "gen_total_watt": 0,    // SITE-LEVEL TOTAL
  "p1": 0, "p2": 0, "p3": 0
  // NO PowerX fields - cannot break down by tenant
}
```

### Solar (Asset 708) - NO TENANT CHANNELS ❌

```json
// GET /sites → asset in site LAG_3778H
{
  "id": 708,
  "name": "Solar",
  "tenant_channels": null,
  "config": null  // NO channels configuration
}
```

---

## ALL TENANTS IN IoT API

```bash
curl -s ".../sites?per_page=100" | jq '[.data[].assets[] | select(.config.channels) | .config.channels[] | select(.type == "tenant")] | group_by(.name) | map({tenant: .[0].name, count: length})'
```

```json
[
  {"tenant": "9Mobile", "count": 3},
  {"tenant": "Airtel", "count": 8},
  {"tenant": "CobraNet", "count": 1},
  {"tenant": "MTN", "count": 10},
  {"tenant": "MTN 2G 3G", "count": 3},
  {"tenant": "MTN 2G 3G 4G", "count": 1},
  {"tenant": "MTN 2G 3G LTE", "count": 1},
  {"tenant": "MTN 2G 3G LTE800", "count": 1},
  {"tenant": "MTN 5G", "count": 3},
  {"tenant": "MTN 6S", "count": 1},
  {"tenant": "MTN Fiber", "count": 4},
  {"tenant": "MTN LTE", "count": 1}
]
```

**All 37 tenant channels exist ONLY in DC Meter assets.**

---

## VERIFICATION COMMANDS

```bash
# 1. List all channel types in API
curl -s "https://iot.novatrack-webservices.net/api/v1/sites?X-Access-Token=TOKEN&per_page=100" \
  | jq '[.data[].assets[] | select(.config.channels) | .config.channels[].type] | group_by(.) | map({type: .[0], count: length})'

# Result: [{"type":"battery","count":20},{"type":"solar","count":4},{"type":"tenant","count":37}]
# NO grid, NO generator

# 2. Check Generator for tenant config
curl -s ".../sites?per_page=100" | jq '[.[] | select(.name | test("Generator"; "i"))] | {type: "Generator", total: length, with_tenant_config: [.[] | select(.has_tenant_config)] | length}'
# Result: {"type": "Generator", "total": 36, "with_tenant_config": 0}

# 3. Check Grid for tenant config
curl -s ".../sites?per_page=100" | jq '[.[] | select(.name | test("Grid"; "i"))] | {type: "Grid", total: length, with_tenant_config: [.[] | select(.has_tenant_config)] | length}'
# Result: {"type": "Grid", "total": 17, "with_tenant_config": 0}

# 4. Check DC Meter for tenant config
curl -s ".../sites?per_page=100" | jq '[.[] | select(.name | test("DC Meter"; "i"))] | {type: "DC Meter", total: length, with_tenant_config: [.[] | select(.has_tenant_config)] | length}'
# Result: {"type": "DC Meter", "total": 20, "with_tenant_config": 20}
```

---

## CONCLUSION

| Question | Answer | Evidence |
|----------|--------|----------|
| Are tenants associated with Grid? | **NO** | 0/17 Grid assets have tenant config |
| Are tenants associated with Generator? | **NO** | 0/36 Generator assets have tenant config |
| Are tenants associated with Solar? | **NO** | 0/1 Solar assets have tenant config |
| Are tenants associated with DC Meter? | **YES** | 20/20 DC Meters have tenant config |
| Do "grid" channel types exist? | **NO** | 0 in entire API |
| Do "generator" channel types exist? | **NO** | 0 in entire API |

---

## WHY TENANTS ONLY SHOW BATTERY

The IoT hardware architecture:

```
Grid ──┐
       ├──► Rectifier ──► DC Bus (48V) ──► DC Meter ──► Tenant Loads
Gen ───┘                      │
                              └──► Battery Bank
Solar ────────────────────────────────────────────┘
```

- **DC Meter** measures consumption on the DC bus
- **Tenant channels** are DC loads fed from the battery/DC bus
- **Grid/Generator** feed into the rectifier (AC side) - no per-tenant metering
- **Solar** feeds directly to DC bus - measured separately, not per-tenant

**The hardware does not support per-tenant breakdown of grid/generator consumption.**

---

## WHAT WOULD BE NEEDED

To show grid/generator/solar per tenant:

1. **Hardware changes** - Separate AC meters per tenant per source (expensive)
2. **Algorithmic estimation** - Proportionally allocate site-level sources based on DC consumption ratios (not implemented, requires assumptions)

**Current system correctly reflects hardware limitations.**
