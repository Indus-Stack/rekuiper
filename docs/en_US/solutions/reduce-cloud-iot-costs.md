---
title: Reduce Cloud IoT Costs by 95% at the Edge
description: Cut AWS IoT Core and Azure IoT Hub bills by 95% using edge SQL downsampling, anomaly filters, and Snappy-compressed Apache Parquet archiving.
keywords: Reduce AWS IoT Core bill, Azure IoT Hub cost optimization, edge MQTT downsampling, MQTT to Parquet edge
---

# Reduce Cloud IoT Ingestion Costs by 95% at the Edge

Industrial IoT systems generate massive telemetry streams. Connecting sensors directly to cloud brokers creates exponential data transfer bills. `rekuiper` sits on the edge gateway, downsampling and filtering streams locally so you send only meaningful data to the cloud.

---

## The Cloud Ingestion Arithmetic: The Raw Telemetry Problem

Consider a standard industrial manufacturing deployment:

* **Fleet Size:** 100 industrial machines.
* **Tag Density:** 50 sensor tags per machine (temperatures, vibrations, motor currents).
* **Sampling Rate:** 10 Hz (10 samples per second per tag).

```
Calculations:
• Messages per second: 100 machines × 50 tags × 10 Hz = 50,000 messages/sec
• Messages per day:    50,000 msg/s × 86,400 seconds   = 4.32 billion messages/day
• Messages per month:  4.32B × 30 days                 = 129.6 billion messages/month
```

### The Financial Impact on Cloud Brokers
Cloud providers meter ingestion and routing per message block (typically 5 KB blocks):
* **AWS IoT Core Pricing:** ~$1.00 to $1.50 per million messages.
* **Monthly Ingestion Cost:** 129.6 billion messages × $1.00/million = **$129,600 per month**.

Over 90% of those messages contain static or repetitive values where motor temperatures and pressures remain unchanged.

---

## The `rekuiper` 3-Step Edge Solution

Deploying `rekuiper` on your local edge gateways reduces message volume by 95% to 98% while preserving sub-millisecond local safety alerts.

```
[ 50,000 msg/s Raw Telemetry ]
               │
               ▼
   ┌───────────────────────┐
   │  rekuiper Edge Engine │
   │  • Tumbling Windows   │
   │  • Change Detection   │
   │  • Parquet Archiving  │
   └───────────────────────┘
         │           │
         │ (98% drop)│ (< 1ms alert)
         ▼           ▼
   [ Cloud MQTT ] [ Local PLC Actuation ]
   (100 msg/s)    (Immediate shutdown)
```

---

### Step 1: Tumbling Time Windows (Downsampling)

Aggregate 60 seconds of high-frequency sensor readings into a single statistical summary row:

```sql
SELECT 
  deviceId,
  avg(temperature) AS avg_temp,
  max(temperature) AS max_temp,
  min(temperature) AS min_temp,
  stddev(vibration) AS stddev_vib,
  count(*) AS sample_count
FROM machine_stream
GROUP BY 
  deviceId, 
  TUMBLINGWINDOW(ss, 60)
```

**Result:** Replaces 600 raw messages per minute with 1 summary row per minute.  
**Volume Reduction:** **99.83% drop in message count.**

---

### Step 2: Local Safety Alerts with Zero Cloud Latency

Do not route telemetry to the cloud to decide if a motor is overheating. Evaluate safety thresholds on the gateway:

```sql
SELECT 
  deviceId, 
  temperature, 
  vibration,
  meta(topic) AS source_topic
FROM machine_stream
WHERE temperature > 85.0 OR vibration > 4.5
```

Configure this rule to publish directly to a local MQTT alert topic or call a PLC Modbus actuator sink:

```json
{
  "id": "critical_overheat_alert",
  "sql": "SELECT deviceId, temperature FROM machine_stream WHERE temperature > 85.0",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "alerts/critical"
      }
    },
    {
      "modbus": {
        "address": "192.168.1.50:502",
        "register": 40001,
        "value": 1
      }
    }
  ]
}
```

**Result:** Emergency shutdowns trigger in sub-milliseconds without depending on cellular or WAN links.

---

### Step 3: Columnar Apache Parquet Archiving

High-resolution raw data is valuable for model training and historical audits, but does not require instant cloud transmission.

Write raw telemetry directly to Snappy-compressed **Apache Parquet files** on local gateway storage:

```json
{
  "id": "archive_raw_parquet",
  "sql": "SELECT * FROM machine_stream",
  "actions": [
    {
      "file": {
        "path": "/data/archives/telemetry.parquet",
        "format": "parquet",
        "rollingCount": 50000,
        "rollingInterval": 3600
      }
    }
  ]
}
```

* **Arrow Schema Inference:** `rekuiper` infers the Arrow schema automatically from your JSON/Protobuf streams.
* **Snappy Compression:** Reduces disk footprint by 75–85% compared to raw JSON Lines.
* **Batched Egress:** Upload one consolidated Parquet file per hour via batch HTTPS object storage, eliminating per-message broker fees.

---

## Financial Comparison: Before and After `rekuiper`

| Parameter | Without `rekuiper` (Direct Cloud Stream) | With `rekuiper` (Edge SQL + Parquet) |
| :--- | :--- | :--- |
| **Ingested Messages to Cloud** | 129.6 billion / month | **1.29 billion / month** (Downsampled) |
| **Monthly Cloud Broker Fee** | ~$129,600 / month | **~$1,290 / month** |
| **Egress Bandwidth Usage** | ~1.8 TB / month (Raw JSON) | **~180 GB / month** (Snappy Parquet) |
| **Local Alert Latency** | 250–1,200 ms (Cloud round-trip) | **< 1 ms** (Evaluated in-memory) |
| **Offline Resilience** | Data lost during link drop | Buffered in disk sink cache |
| **Net Monthly Savings** | Baseline ($0) | **$128,310 / month (99% Cost Cut)** |

---

## Next Steps

* [Explore Windowing SQL Functions](../concepts/streaming/windowing.md)
* [Configure Apache Parquet Sinks](../guide/sinks/overview.md)
* [Review Offline Sink Caching & Resilience](../guide/rules/error_handling_dlq.md)
