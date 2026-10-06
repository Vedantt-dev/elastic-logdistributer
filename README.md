# QuickBite Telemetry Pipeline — BDA Mini-Project

A laptop-scale copy of the telemetry backbone used by hyper-scale consumer platforms (Swiggy, Netflix, Amazon). A multi-threaded simulator runs five independent microservices that emit structured JSON logs for realistic food-delivery journeys. The logs flow through **Kafka → Logstash → Elasticsearch → Kibana**, where a live command-center dashboard shows outages as they happen.

```
 ┌──────────────────────── producer.py (multi-threaded) ────────────────────────┐
 │                                                                              │
 │  traffic-gen ─► auth-vault ─► menu-catalog ─► payment-gateway ─► driver-     │
 │  (Poisson)      (login/OTP)   (search, menu,   (UPI / card /      dispatcher │
 │                               checkout)        wallet / COD)     (allocate,  │
 │                                    │                 │            pickup,    │
 │   ScenarioController               ▼                 ▼            pings,     │
 │   (flash sale, UPI spike,   notification-service ◄───┴─────────── delivered) │
 │    driver shortage, bots)   (push → SMS fallback)                            │
 │                                                                              │
 │   bounded queues per service = in-process backpressure + 503 load shedding   │
 └───────────────────────────────┬──────────────────────────────────────────────┘
                                 │ key = correlation_id, acks=all, idempotent, lz4
                                 ▼
            ┌─────────────────────────────────────────────┐
            │ Kafka (KRaft)  topic platform.telemetry.v1  │   kafka-ui :8080
            │ P0  P1  P2  P3  P4  P5   (6 partitions)     │
            └──────────────────────┬──────────────────────┘
                                   │ consumer group logstash-telemetry-indexer (3 threads)
                                   ▼
            ┌─────────────────────────────────────────────┐
            │ Logstash  pipeline.conf    (persisted queue)│   monitoring :9600
            │  parse JSON → event-time → 2xx/4xx/5xx tag  │
            │  zone→geo_point, service→SLO, latency band  │
            │  HMAC + mask UPI/card/phone/IP, scrub text  │
            │  poison pills → deadletter-telemetry-*      │
            └──────────────────────┬──────────────────────┘
                                   │ _id = event_id (idempotent upsert)
                                   ▼
            ┌─────────────────────────────────────────────┐
            │ Elasticsearch   telemetry-YYYY.MM.dd        │   :9200
            │ index template + ILM (delete after 3 days)  │
            └──────────────────────┬──────────────────────┘
                                   ▼
            ┌─────────────────────────────────────────────┐
            │ Kibana  "QuickBite Command Center"          │   :5601
            └─────────────────────────────────────────────┘
```

## Repository layout

| Path | Purpose |
|---|---|
| `docker-compose.yml` | Starts the whole stack with one command, with CPU and memory limits on every container |
| `producer/producer.py` | Multi-service traffic simulator that injects scenarios |
| `producer/Dockerfile`, `producer/requirements.txt` | Container image for the simulator |
| `logstash/pipeline/pipeline.conf` | Ingest pipeline: Kafka input, classification, enrichment, PII masking, Elasticsearch output |
| `logstash/config/logstash.yml`, `pipelines.yml` | Logstash node settings (persistent queue, DLQ, workers) |
| `setup/bootstrap.py` | Creates the ILM policy, index templates, Kibana data views, visualisations and dashboard |
| `scripts/verify.sh` | End-to-end health check from Kafka through to Kibana |
| `VIVA.md` | Viva / defense questions with model answers |

## Resource budget

| Container | CPU limit | RAM limit | JVM heap |
|---|---|---|---|
| elasticsearch | 1.5 | 2 GB | 1 GB |
| kibana | 1.0 | 1.25 GB | Node 900 MB |
| logstash | 1.0 | 1 GB | 512 MB |
| kafka | 1.0 | 1 GB | 512 MB |
| kafka-ui | 0.5 | 448 MB | 256 MB |
| producer | 1.0 | 256 MB | — |
| one-shot setup jobs | 0.25–0.5 | 128–384 MB | — |

The long-running containers need about 6 GB in total. In **Docker Desktop → Settings → Resources**, give Docker at least **6 GB RAM and 4 CPUs** (8 GB is more comfortable).

---

## Step-by-step execution and verification guide

### Step 0 — Prerequisites

1. Install Docker Desktop (macOS/Windows) or Docker Engine with the Compose v2 plugin (Linux).
2. Check that both are available:

```bash
docker --version
```

```bash
docker compose version
```

3. **Linux only.** Elasticsearch needs a higher mmap limit:

```bash
sudo sysctl -w vm.max_map_count=262144
```

### Step 1 — Build and start everything

From the project root:

```bash
docker compose up -d --build
```

Start-up order is enforced with `depends_on` conditions:

1. `kafka` becomes healthy, then `kafka-init` creates the 6-partition topic.
2. `elasticsearch` becomes healthy, then `es-setup` installs the ILM policy and index templates.
3. `logstash` starts only after the topic **and** the templates exist, so the first index is created with the correct mappings.
4. `kibana` becomes healthy, then `kibana-setup` imports the dashboard.
5. `producer` starts once the topic exists.

The first run downloads about 2.5 GB of images. Expect 2–4 minutes before Kibana is ready.

### Step 2 — Watch the stack come up

```bash
docker compose ps
```

The one-shot jobs (`kafka-init`, `es-setup`, `kibana-setup`) should show `Exited (0)`. Every other container should show `running (healthy)`.

```bash
docker compose logs -f producer
```

Every 10 seconds the producer prints a stats line. Watch the `phase=` value change as the scenarios rotate:

```
phase=upi_timeout_spike  eps= 212.4 events=48211 journeys=5120 2xx=46010 4xx=1350 5xx=851 ... | queues: auth=0 menu=0 payment=37 ...
```

Check the one-shot jobs:

```bash
docker compose logs kafka-init es-setup kibana-setup
```

### Step 3 — Inspect the Kafka topic queue

Describe the topic. This shows 6 partitions, their leader, replicas and ISR:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic platform.telemetry.v1
```

Show the end offset of each partition, i.e. how many messages each partition holds:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic platform.telemetry.v1 --time latest
```

Read 5 live messages with their key, partition and offset. Notice that all events with the same `correlation_id` key land on the same partition:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic platform.telemetry.v1 --max-messages 5 --property print.key=true --property print.partition=true --property print.offset=true
```

Read only partition 2 from the beginning:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic platform.telemetry.v1 --partition 2 --offset earliest --max-messages 5 --property print.key=true
```

Check consumer-group lag. The `LAG` column is the backlog Logstash has not yet read:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group logstash-telemetry-indexer
```

For a GUI view, open **Kafka UI** at <http://localhost:8080> → Topics → `platform.telemetry.v1` → Messages / Consumers.

### Step 4 — Inspect Logstash

Per-plugin event counts and timings:

```bash
curl -s http://localhost:9600/_node/stats/pipelines/telemetry-ingest?pretty
```

Pipeline logs (startup, plugin errors, Elasticsearch connectivity):

```bash
docker compose logs -f logstash
```

### Step 5 — Verify Elasticsearch

List the daily indices and their document counts:

```bash
curl -s "http://localhost:9200/_cat/indices/telemetry-*,deadletter-telemetry-*?v&s=index"
```

Traffic split by status class:

```bash
curl -s -H 'Content-Type: application/json' "http://localhost:9200/telemetry-*/_search?size=0&pretty" -d '{"aggs":{"by_class":{"terms":{"field":"traffic_class"}}}}'
```

p95 latency per service:

```bash
curl -s -H 'Content-Type: application/json' "http://localhost:9200/telemetry-*/_search?size=0&pretty" -d '{"aggs":{"svc":{"terms":{"field":"service"},"aggs":{"p95":{"percentiles":{"field":"latency_ms","percents":[95]}}}}}}'
```

Confirm PII masking. `upi_id` should look like `pr****@okhdfcbank` and `upi_fingerprint` should be a 64-character HMAC:

```bash
curl -s -H 'Content-Type: application/json' "http://localhost:9200/telemetry-*/_search?pretty" -d '{"size":2,"_source":["payment","user.phone","client_ip","message"],"query":{"exists":{"field":"payment.upi_id"}}}'
```

Confirm that leaked card PANs in free text were scrubbed. The query searches for `pan=` debug lines:

```bash
curl -s -H 'Content-Type: application/json' "http://localhost:9200/telemetry-*/_search?pretty" -d '{"size":2,"_source":["message"],"query":{"match_phrase":{"message":"acquirer request"}}}'
```

Rebuild one complete user journey from its correlation ID. Replace `JRN-...` with a real `correlation_id` taken from any document:

```bash
curl -s -H 'Content-Type: application/json' "http://localhost:9200/telemetry-*/_search?pretty" -d '{"size":50,"_source":["@timestamp","service","operation","status_code","latency_ms","kafka.partition"],"sort":[{"@timestamp":"asc"}],"query":{"term":{"correlation_id":"JRN-REPLACE-ME"}}}'
```

Check the dead-letter index (malformed poison-pill records):

```bash
curl -s "http://localhost:9200/deadletter-telemetry-*/_search?size=3&pretty"
```

Or run every check at once:

```bash
./scripts/verify.sh
```

### Step 6 — Open the Command Center dashboard

Open <http://localhost:5601/app/dashboards#/view/quickbite-command-center>. It auto-refreshes every 10 seconds over the last 15 minutes.

| Panel | What to look for |
|---|---|
| Events ingested / Server errors (5xx) / p95 latency / SLO breaches | KPI tiles |
| Traffic by status class | The red 5xx band grows during `upi_timeout_spike` and `driver_shortage` |
| p95 latency by service | `payment-gateway` jumps towards 9–10 s during the UPI spike |
| Errors by city zone | Bengaluru dominates during `driver_shortage` |
| Payment outcomes by method | The UPI slice turns red during the UPI spike |
| Events per Kafka partition | Roughly even spread, which shows the correlation-ID partition key is well balanced |
| Injected scenario timeline | Ground-truth labels to compare each anomaly against |
| Pipeline ingest lag | Rises when Logstash or Elasticsearch fall behind (backpressure) |
| Top error codes / Live error stream | Drill down to individual log lines |

If the import ever fails, re-run it on its own:

```bash
docker compose run --rm kibana-setup
```

### Step 7 — Build the visualisations yourself (for the demo / viva)

All of these use the **QuickBite Telemetry** data view (`telemetry-*`, time field `@timestamp`).

**A. 5xx outage timeline (Lens, stacked bar)**
1. Open Kibana → ☰ → **Visualize Library** → **Create visualization** → **Lens**.
2. Drag `@timestamp` onto the horizontal axis.
3. Set the vertical axis to **Count of records**.
4. Drag `traffic_class` onto **Breakdown**.
5. Change the chart type to **Bar vertical stacked**.
6. Save as "Outage timeline" and add it to the dashboard.

**B. Latency heat map per service**
1. In Lens, pick the **Heat map** chart type.
2. Horizontal axis: `@timestamp`. Vertical axis: `service`.
3. Cell value: **Percentile** of `latency_ms`, with percentile 95.

**C. UPI failure rate (Lens formula)**
1. In Lens, pick the **Metric** chart type.
2. Click **Primary metric** → **Formula** and enter:
   `count(kql='payment.method : "UPI" and status_code >= 500') / count(kql='payment.method : "UPI" and operation : "payment.authorize"')`
3. Set **Value format** to Percent.

**D. Driver map (Maps app)**
1. Open Kibana → ☰ → **Maps** → **Create map** → **Add layer** → **Documents**.
2. Choose data view `telemetry-*` and geospatial field `driver.location`.
3. Add a filter: `operation : "tracking.location_ping"`.
4. Add a second **Clusters** layer on `zone_location`, filtered to `status_code >= 500`. This shows which city is failing.

**E. Find one journey in Discover**
1. Open Discover and search `correlation_id : "JRN-..."`.
2. Sort ascending by `@timestamp` to read the trace hop by hop: auth → menu → checkout → payment → dispatch → notifications.

### Step 8 — Drive specific scenarios on demand

Pin one scenario instead of cycling through them. Edit `PIN_SCENARIO` in `docker-compose.yml` (for example `upi_timeout_spike`), then recreate only the producer:

```bash
docker compose up -d --no-deps --force-recreate producer
```

Available scenarios: `normal`, `flash_sale`, `upi_timeout_spike`, `driver_shortage`, `auth_credential_stuffing`, `notification_outage`.

To demonstrate backpressure, pause Elasticsearch so Logstash cannot write. Then watch the consumer-group `LAG` grow while the producer keeps running:

```bash
docker compose pause elasticsearch
```

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group logstash-telemetry-indexer
```

Resume Elasticsearch. The lag drains and the *Pipeline ingest lag* panel shows the spike:

```bash
docker compose unpause elasticsearch
```

To demonstrate at-least-once delivery and idempotent writes, restart Logstash. Uncommitted offsets are replayed, but `_id = event_id` turns the duplicates into overwrites, so document counts do not double:

```bash
docker compose restart logstash
```

### Step 9 — Run the producer outside Docker (optional)

```bash
python3 -m pip install -r producer/requirements.txt
```

```bash
python3 producer/producer.py --bootstrap localhost:9092 --rate 15 --scenario flash_sale
```

Dry run without Kafka (prints JSON to stdout):

```bash
python3 producer/producer.py --dry-run --duration 10 --rate 5
```

### Step 10 — Stop or reset

Stop and keep all data:

```bash
docker compose stop
```

Remove containers but keep the Elasticsearch volume:

```bash
docker compose down
```

Full reset, which deletes indices and the Logstash queue:

```bash
docker compose down -v
```

## Restarting for a demo

1. Open Docker Desktop and wait for **Engine running**.
2. Start the stack. No rebuild is needed after the first run:

```bash
docker compose up -d
```

3. Check every stage:

```bash
./scripts/verify.sh
```

4. Start the stack **10–15 minutes before presenting**. The dashboard shows the last 15 minutes, and one full cycle of scenarios takes about 9 minutes.
5. When you're done, stop it and keep the data:

```bash
docker compose stop
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `elasticsearch` exits with code 137 | Docker has too little memory; raise it to 6–8 GB. |
| `max virtual memory areas vm.max_map_count [65530] is too low` | Linux only: `sudo sysctl -w vm.max_map_count=262144` |
| Dashboard panels show "No results" | Widen the time picker to *Last 1 hour*, then check `docker compose logs logstash` and `./scripts/verify.sh`. |
| `kibana-setup` failed | Run `docker compose run --rm kibana-setup` once Kibana is healthy. |
| Producer logs `delivery failed` | Kafka is not healthy; check `docker compose logs kafka`. |
| Port already in use | Change the left-hand side of the `ports:` mapping (e.g. `"5602:5601"`). |
| Apple Silicon | Every image used has a native arm64 build; no emulation is needed. |

## Log schema (one event)

```json
{
  "event_id": "01ac299b-9777-42c6-a681-ea9a7dacbe02",
  "event_time": "2026-10-06T12:30:33.561Z",
  "service": "payment-gateway",
  "host": "payment-gateway-ff46a-0",
  "level": "INFO",
  "operation": "payment.authorize",
  "http_method": "POST",
  "endpoint": "/v2/payments/upi/collect",
  "status_code": 200,
  "latency_ms": 1122,
  "trace_id": "a29302a504cff0e572c2850bd126e378",
  "span_id": "b05a3d44d42b207c",
  "parent_span_id": "45f65ba12f1e36e6",
  "correlation_id": "JRN-26243dd902194de8",
  "order_id": "ORD-20261006-61143738",
  "geo_zone": "Bengaluru",
  "geo_area": "Indiranagar",
  "scenario": "normal",
  "message": "UPI collect approved by priya.gupta64@okhdfcbank for INR 787.55",
  "payment": {"method": "UPI", "upi_id": "priya.gupta64@okhdfcbank", "amount_inr": 787.55, "attempt": 1}
}
```

After Logstash processes it, the same event gains `traffic_class`, `latency_band`, `slo_threshold_ms`, `slo_breach`, `ingest_lag_ms`, `zone_location`, `kafka.{topic,partition,offset,key}`, `payment.upi_fingerprint` and the `pii_scrubbed` tag. The VPA, phone, client IP and message are masked.
