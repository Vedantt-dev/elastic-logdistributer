# Viva / Defense — Questions and Model Answers

Each answer is tied back to a concrete artifact in this project, so you can point at the code or the dashboard while you answer.

---

## Section A — Big Data fundamentals

### Q1. What are the 5 V's of Big Data, and how does this project show each one?

| V | Meaning | Where it appears in this project |
|---|---|---|
| **Volume** | Data too large for one machine to store or process | About 200 events/s at baseline and 800+ during a flash sale. That is roughly 17–70 million events/day. At ~800 bytes each, that is 14–55 GB/day for a single small city cluster. Elasticsearch spreads this across daily indices and shards. |
| **Velocity** | Speed at which data arrives and must be processed | Events must appear on the dashboard within seconds. Kafka absorbs bursts, Logstash processes them in micro-batches of 250, and Kibana refreshes every 10 s. `ingest_lag_ms` measures end-to-end velocity. |
| **Variety** | Different formats and shapes of data | Five services emit different sub-documents (`payment`, `driver`, `notification`, `search`). The mix includes numbers, keywords, free text, geo-points and IPs, plus malformed non-JSON poison pills. |
| **Veracity** | Trustworthiness and quality of data | Malformed records are routed to `deadletter-telemetry-*`. Event-time beats ingest-time for correctness. Idempotent `_id`s prevent duplicates. A ground-truth `scenario` label lets us verify that detected anomalies are real. |
| **Value** | Business insight extracted | Detecting a UPI outage within seconds, seeing which city has a driver shortage, measuring SLO breaches, spotting credential-stuffing bots. Each of these protects revenue or users. |

### Q2. Why is log data considered Big Data and not just "a lot of files"?
It is unbounded (it never ends) and append-only. It is semi-structured and arrives from thousands of independent sources at variable rates. It must be queried both by full-text search and by aggregation. No single-node database can ingest, index and query it at the same time at this scale. That requires horizontal partitioning (Kafka partitions, Elasticsearch shards) and parallel consumers.

### Q3. Is this batch processing or stream processing?
It is stream processing. Every event is processed individually as it arrives. Logstash uses micro-batches (250 events or 50 ms) only to amortise network cost, not as a time-based batch window. A batch system (e.g. Hadoop or a nightly Spark job) would only show the UPI outage hours later.

### Q4. Which architecture pattern is this — Lambda or Kappa?
**Kappa**. There is one streaming path, and Kafka is the replayable source of truth. To reprocess history, we reset the consumer-group offsets and replay the topic through a new pipeline version. There is no separate batch layer.

---

## Section B — Kafka and distributed messaging

### Q5. Why put Kafka between the producers and Logstash? Why not send directly to Elasticsearch?
1. **Decoupling.** Producers do not know or care who consumes. We could add a Spark or Flink fraud detector as a second consumer group without touching the services.
2. **Buffering and backpressure.** If Elasticsearch is slow or down, events wait durably on Kafka's disk (a retention window of 6 h here) instead of being dropped or blocking the payment service.
3. **Replay.** Offsets can be rewound to reprocess data after a bug fix.
4. **Horizontal scale.** Partitions let many consumers read in parallel.

### Q6. What is a partition, and why 6 partitions?
A partition is an ordered, append-only, immutable log. It is the unit of parallelism and ordering in Kafka. Each partition is consumed by exactly one consumer within a consumer group. Six partitions allow up to six parallel Logstash consumer threads (we use 3, so each thread owns 2). Six divides evenly by 1, 2, 3 and 6, so rebalances stay even as you scale consumers.

### Q7. What is a partition key? Why did you use `correlation_id`?
The producer computes `hash(key) % num_partitions` (murmur2 hashing). Every event with the same key always goes to the same partition, and Kafka guarantees ordering only *within* a partition. With `correlation_id` (one per user journey) as the key, all events of a journey (login → checkout → payment → dispatch → delivered) stay in order. The key also has high cardinality, so the load spreads evenly. The *Events per Kafka partition* panel proves this.

### Q8. What would go wrong with a bad partition key such as `geo_zone` or `service`?
- **Hot partitions (skew).** Delhi, Mumbai and Bengaluru carry 66% of traffic. With `geo_zone` as the key, 3 partitions would be overloaded and others idle. During `driver_shortage`, one partition would carry the whole Bengaluru storm.
- **Limited parallelism.** Only 6 distinct zones means at most 6 useful partitions forever.
- `service` as the key would put all `driver-dispatcher` pings (the largest volume) into one partition.

### Q9. What are `acks=all` and `enable.idempotence=true` in the producer?
- `acks=all`: the leader replies only after all in-sync replicas have written the record. No acknowledged message is lost if the leader dies (with replication factor > 1).
- Idempotence: the producer attaches a producer-id and a sequence number to each batch. If a retry re-sends a batch the broker already wrote, the broker discards the duplicate. That gives **exactly-once per partition from producer to broker**, and ordering is preserved even with retries.

### Q10. What is a consumer group, and what is consumer lag?
A consumer group is a set of consumers that share the partitions of a topic. Each partition is assigned to one member. The group commits **offsets** (its position in each partition). **Lag** is `log-end-offset − committed-offset`, i.e. how far behind the consumer is. You can see it with `kafka-consumer-groups.sh --describe`. It is the single most important health metric of a streaming pipeline.

### Q11. What is KRaft? Why no ZooKeeper?
KRaft (Kafka Raft) puts cluster metadata (topics, partitions, leaders, ISR) in an internal Raft-replicated log managed by controller nodes inside Kafka itself. It removes the separate ZooKeeper ensemble, so there is one system to deploy and secure. It also gives faster controller failover and supports millions of partitions. ZooKeeper mode was removed in Kafka 4.0. Here one node acts as both `broker` and `controller` (`KAFKA_PROCESS_ROLES: broker,controller`).

### Q12. What are replication factor, ISR and `min.insync.replicas`?
- **Replication factor (RF):** copies of each partition across brokers. It is 1 here because there is one broker. Production uses 3.
- **ISR (in-sync replicas):** replicas fully caught up with the leader.
- **`min.insync.replicas`:** with `acks=all`, the minimum ISR size for a write to succeed. Production uses RF=3 with `min.insync.replicas=2`, which tolerates one broker failure without losing acknowledged data and without becoming unavailable.

---

## Section C — Delivery semantics and reliability

### Q13. Explain at-most-once, at-least-once and exactly-once delivery.
| Semantics | How | Risk |
|---|---|---|
| At-most-once | Commit the offset **before** processing | Crash → message lost |
| At-least-once | Commit the offset **after** processing | Crash → message reprocessed (duplicate) |
| Exactly-once | Transactions (Kafka EOS) or at-least-once **plus idempotent sink** | Higher complexity / latency |

### Q14. Which semantics does this pipeline provide, end to end?
**At-least-once delivery with effectively-once results.**
- Producer → Kafka: idempotent producer, so no duplicates from retries.
- Kafka → Logstash: offsets are auto-committed every 5 s, after events have entered Logstash's **persistent (on-disk) queue**. If Logstash crashes, uncommitted events are re-read, so duplicates are possible but nothing is lost.
- Logstash → Elasticsearch: `document_id => "%{event_id}"`. A replayed event overwrites the same `_id` instead of creating a second document. The **idempotent sink** turns at-least-once delivery into an exactly-once *result*.

Demo: `docker compose restart logstash`. Document counts do not double.

### Q15. Why not use exactly-once transactions everywhere?
Kafka transactions guarantee exactly-once only for **Kafka-to-Kafka** read-process-write. Elasticsearch is an external system that does not take part in Kafka transactions. The standard industry pattern for external sinks is therefore at-least-once plus idempotent writes (deterministic IDs or upserts). It is also cheaper.

### Q16. What happens to a malformed (poison-pill) message?
The producer deliberately emits about 0.1% truncated non-JSON records. The JSON codec tags them `_jsonparsefailure`, and the pipeline routes them to `deadletter-telemetry-*` with the Kafka partition, offset and key for forensics. They do not crash the pipeline, block the partition, or pollute the main index. Separately, Logstash's built-in `dead_letter_queue` captures events that Elasticsearch rejects (e.g. mapping conflicts).

---

## Section D — Backpressure and flow control

### Q17. What is backpressure?
Backpressure is a signal that flows *upstream* from a slow consumer to a fast producer, saying "slow down or buffer". Without it, a fast producer overwhelms the slow stage, causing memory exhaustion, crashes and data loss.

### Q18. Where does backpressure appear in this project? (Name all layers.)
1. **Inside the simulator.** Each microservice has a **bounded** `queue.Queue(maxsize=1000)`. When `payment-gateway` slows down (UPI 9 s timeouts), its queue fills. Upstream `submit()` cannot enqueue within its timeout, so the request is **load-shed with a 503 `UPSTREAM_SATURATED`**, as a real service mesh would do.
2. **Kafka client.** When librdkafka's local buffer is full, `produce()` raises `BufferError`. The producer then polls (drains delivery reports) and retries instead of dropping (`producer_buffer_full` counter).
3. **Kafka itself.** Kafka is a pull-based system, so consumers fetch at their own pace. A slow consumer does not slow producers. The backlog is stored on disk as **lag**.
4. **Logstash.** If Elasticsearch returns HTTP 429 (bulk queue full) or is down, the output retries with exponential backoff. Workers block, the **persistent queue** fills (256 MB cap), and then the Kafka input stops polling. The backlog stays safely in Kafka.
5. **Visible in Kibana.** The *Pipeline ingest lag* panel (`now − event_time`) rises.

Demo: `docker compose pause elasticsearch`, watch the lag grow, then `unpause` and watch it drain.

### Q19. Backpressure vs load shedding vs buffering — what is the difference?
- **Buffering:** absorb the burst and process it later (Kafka, persistent queue). This works when bursts are temporary.
- **Backpressure:** make the sender slow down (blocking `put`, Logstash stops polling).
- **Load shedding:** deliberately reject excess work quickly (HTTP 429/503) to protect latency for the requests that are accepted. This happens in `menu-catalog` during the flash sale and in the saturated inboxes.

### Q20. Why does the producer use exponential backoff with jitter for UPI retries?
Without it, thousands of clients retry at the same moment and hammer an already failing dependency (a **retry storm** / thundering herd). Backoff `0.5 s × 2^(attempt-1) + random(0, 0.3 s)` spreads the retries out. A cap of 3 attempts bounds the work, after which the payment is finalised as failed (`UPI_RETRIES_EXHAUSTED`).

---

## Section E — Elasticsearch storage and indexing

### Q21. Full-text (inverted) indexing vs columnar storage — explain both.
- **Inverted index** (Lucene, used for `text` and `keyword` search): maps each *term* to the list of documents containing it, e.g. `"timeout" → [doc 7, doc 91, doc 4410]`. Finding "all logs mentioning NPCI" is a fast lookup instead of a scan of every document. `message` is mapped as `text` (analysed into tokens), so it is searchable by words.
- **Columnar storage** (Elasticsearch *doc values*, also Parquet and ClickHouse): stores each field's values contiguously per segment, e.g. all `latency_ms` values together. Aggregations (avg, percentiles, terms, date histograms) read only the columns they need, sequentially and compressed. The dashboards rely on this: *p95 latency by service* reads only the `latency_ms`, `service` and `@timestamp` columns.
- **Elasticsearch uses both**: inverted indexes to *find* documents, doc values to *aggregate* them. This is why `message` also has a `message.raw` keyword sub-field, and why identifiers like `trace_id` are `keyword` (exact match plus aggregatable) rather than `text`.

### Q22. Why `keyword` vs `text`? Give examples from your mapping.
- `text` is analysed (tokenised, lower-cased) for relevance search. Use it for `message`.
- `keyword` is stored as one exact term. It can be aggregated and sorted. Use it for `service`, `status class`, `trace_id`, `geo_zone` and `error.code`.

Mapping `trace_id` as `text` would split it into tokens and break exact lookups and aggregations. A dynamic template maps any unknown string as `keyword` to prevent mapping explosion.

### Q23. What is time-series indexing? Why daily indices?
Logs are time-ordered, append-only, and queried by recent time ranges. Writing to one index per day (`telemetry-2026.10.06`) gives these benefits:
- **Cheap retention.** ILM deletes whole indices after 3 days, which is far cheaper than `delete_by_query`.
- **Query pruning.** A "last 15 min" query only touches today's index.
- **Hot/warm/cold tiers.** Older indices can move to cheaper nodes or be force-merged and made read-only.
- **Index sorting** on `@timestamp desc` (set in our template) speeds up "latest N" queries through early termination.

The modern equivalent is **data streams**, which use rollover by size or age plus hidden backing indices. The concept is the same.

### Q24. What are shards and replicas? Why 1 shard and 0 replicas here?
A shard is a Lucene index. An Elasticsearch index is split into primary shards that are spread across nodes for parallelism. Replicas are copies for high availability and read throughput. On a single node a replica cannot be placed anywhere (it would stay unassigned and the cluster would be *yellow*), so `number_of_replicas: 0`. One small daily index does not need multiple shards. A common rule of thumb is to target 10–50 GB per shard.

### Q25. What is `refresh_interval: 5s`? What is near-real-time search?
Newly indexed documents sit in an in-memory buffer. A *refresh* writes them into a new searchable segment. The default of 1 s gives very fresh results but creates many tiny segments and costs CPU. 5 s trades a little freshness for better ingest throughput. Elasticsearch is "near real-time" because of this refresh delay.

### Q26. Why set `_id = event_id` instead of letting Elasticsearch generate IDs?
To get idempotent writes (Q14). The trade-off is that Elasticsearch must check whether that `_id` already exists, which is slightly slower than auto-generated IDs. For correctness under at-least-once delivery this is worth it.

---

## Section F — Logstash pipeline design

### Q27. Walk through `pipeline.conf`.
1. **Input:** Kafka consumer group, 3 threads, cooperative-sticky rebalancing, `decorate_events` to expose topic, partition and offset.
2. **Dead-letter routing** of non-JSON records.
3. **`date` filter:** `@timestamp ← event_time`. We index by *event time*, not *processing time*, so late or replayed data lands in the correct time bucket.
4. **Kafka lineage fields** (`kafka.partition` and `kafka.offset`).
5. **Classification:** `traffic_class` = 2xx / 4xx / 5xx, plus tags `outage_signal`, `throttled`, `bot_traffic`, `retried_request`.
6. **Enrichment:** `translate` zone → geo_point and service → SLO threshold. A Ruby block computes `latency_band`, `slo_breach` and `ingest_lag_ms`.
7. **PII protection:** HMAC fingerprint, field masking, free-text scrubbing.
8. **Output:** daily index, idempotent `_id`, separate dead-letter index.

### Q28. How is sensitive data protected? Why fingerprint *and* mask?
- **Masking** (`pr****@okhdfcbank`, `tok_0123****…4567`, `+91 98******10`, IP → `/24`) keeps the value human-recognisable for support staff while hiding identity.
- **Keyed HMAC-SHA256 fingerprint** of the UPI ID is **pseudonymisation**. Analysts can still count *distinct payers* or follow one payer's repeated failures without ever seeing the VPA. A key (pepper) is used because UPI IDs have low entropy: an unkeyed SHA-256 could be brute-forced from a dictionary of common names and bank handles.
- **Free-text scrubbing** catches PII that developers accidentally log. The simulator deliberately leaks a raw test card PAN in a DEBUG line, and the regex turns it into `4111********1111`.
- Why this matters: PCI-DSS forbids storing full PANs in logs, and India's DPDP Act 2023 requires data minimisation. Masking **before** indexing means the PII never reaches disk in Elasticsearch, snapshots or backups.

### Q29. Logstash vs Vector vs Fluent Bit — why Logstash?
Logstash has a mature plugin ecosystem (Kafka input, translate, fingerprint, Ruby) and native Elasticsearch integration, so it is ideal for teaching. Vector (Rust) and Fluent Bit (C) use 5–10× less memory and are preferred as edge or sidecar agents at scale. A common production topology is Fluent Bit or Vector on each node → Kafka → Logstash, Flink or Vector aggregators → Elasticsearch.

### Q30. Why the persistent queue in Logstash?
The default memory queue loses in-flight events if Logstash crashes, and it cannot absorb bursts larger than memory. The persistent queue writes events to disk after the input stage, before they are acknowledged. This lets Logstash commit Kafka offsets safely and gives a 256 MB shock absorber.

---

## Section G — Observability concepts

### Q31. What are `trace_id`, `span_id` and `correlation_id`? How do they differ?
- **trace_id:** one end-to-end request tree (here, one user journey) in W3C Trace Context / OpenTelemetry format (32 hex chars).
- **span_id / parent_span_id:** one unit of work (one service call) and the call that caused it. Together they rebuild the causal chain and per-hop latency.
- **correlation_id:** a business-level identifier propagated across services and async boundaries. Here it is also the **Kafka key**. `order_id` is another business correlation key that appears only after checkout.

A query on `correlation_id` in Discover rebuilds a full journey across all 5 services.

### Q32. Logs vs metrics vs traces — the three pillars of observability.
- **Logs:** discrete, detailed events (this project).
- **Metrics:** aggregated numbers over time (we *derive* metrics such as p95 and error rate from logs through aggregations).
- **Traces:** causal request trees across services (our trace and span IDs).

At scale, metrics are cheaper for alerting, while logs and traces are used for diagnosis.

### Q33. Why p95/p99 latency instead of the average?
Latency distributions are long-tailed (the simulator uses a log-normal distribution). The average hides the slow tail. If 5% of UPI payments take 9 s, the average may still look fine while thousands of users stare at a spinner. SLOs are therefore defined on percentiles, e.g. "p95 < 3 s for payment-gateway", which is `slo_threshold_ms`.

### Q34. How would you alert on the UPI outage automatically?
Use a Kibana alerting rule (Elasticsearch query or threshold rule) every 1 minute:
`count(payment.method:UPI and status_code:504) / count(payment.method:UPI) > 10% over 5m` → notify Slack or PagerDuty.

A more robust option is multi-window burn-rate alerting on the SLO error budget. An ML anomaly-detection job on `latency_ms` partitioned by `service` would also work.

---

## Section H — Design trade-offs and scaling

### Q35. How would this scale to Swiggy or Netflix production volumes?
- **Kafka:** 3+ brokers across availability zones, RF=3, `min.insync.replicas=2`, hundreds of partitions, and tiered storage to S3 for long retention.
- **Ingestion:** many Logstash or Vector instances in the same consumer group, so partitions are shared out automatically (horizontal scaling up to the partition count).
- **Elasticsearch:** dedicated master, hot, warm and cold nodes. Data streams with ILM rollover at ~50 GB/shard. Replicas ≥ 1. Searchable snapshots on object storage.
- **Cost:** sample successful 2xx logs (keep 100% of errors), drop verbose fields, and move high-cardinality analytics to a columnar OLAP store (ClickHouse or Druid).

### Q36. What is the CAP trade-off here?
- Kafka with `acks=all` and `min.insync.replicas` chooses **consistency over availability** for writes. If too few replicas are in sync, it rejects writes rather than risk losing them.
- Elasticsearch is **eventually consistent** for search (refresh interval) and favours availability for reads from replicas.
- For telemetry, a few seconds of staleness is acceptable, but losing payment-failure evidence is not.

### Q37. What is the single point of failure in this lab setup, and how would you fix it?
Every component is a single instance: 1 Kafka broker (RF=1), 1 Elasticsearch node (0 replicas), 1 Logstash. To fix it: 3 Kafka KRaft controllers plus brokers with RF=3; 3 Elasticsearch master-eligible nodes with replicas = 1; ≥ 2 Logstash instances in the same consumer group. If one Logstash dies, Kafka rebalances its partitions to the survivor within `session.timeout.ms`.

### Q38. What happens during a consumer-group rebalance? Why `cooperative_sticky`?
When a consumer joins or leaves, partitions are reassigned. The classic *eager* protocol revokes **all** partitions from all consumers ("stop-the-world"). **Cooperative sticky** moves only the partitions that must move and keeps the rest assigned, so there is no global pause.

### Q39. Event time vs processing time — why does it matter?
Event time is when the payment actually failed. Processing time is when Logstash saw it. During backpressure (Elasticsearch paused for 2 minutes), the events are processed late. Indexing by processing time would make the outage appear 2 minutes later and compressed into a spike. Indexing by event time keeps the timeline truthful. `ingest_lag_ms` is exactly the difference between the two.

### Q40. How is this simulator realistic, and what are its limits?
**Realistic:** independent services with bounded inboxes and worker pools; log-normal latency; Poisson arrivals; causal journeys with trace propagation; retries with backoff; fallbacks (push → SMS); refunds when dispatch fails; zone-scoped incidents; bot traffic; real-world PII leaks; poison pills; ground-truth labels.

**Limits:** it runs in a single process (real services run on separate hosts and talk over a network); delivery time is compressed (20–50 s instead of 30 min); there is no clock skew between hosts; traffic has no daily rush-hour pattern beyond a gentle sine wave.

---

## Quick-fire round

| Question | Answer |
|---|---|
| Default Kafka partitioner? | murmur2 hash of the key modulo the number of partitions (sticky partitioning for null keys) |
| Where are consumer offsets stored? | The internal `__consumer_offsets` topic |
| What makes a Kafka write durable? | Replication to the ISR plus `acks=all`. Kafka relies on the OS page cache plus replication rather than fsync on every write |
| Why is Kafka fast? | Sequential disk I/O, zero-copy `sendfile`, batching, compression, page cache |
| Elasticsearch index vs shard vs segment? | Logical index → N primary shards (Lucene indices) → immutable segments, merged in the background |
| What does ILM stand for? | Index Lifecycle Management |
| What is a mapping explosion? | Unbounded dynamic field creation exhausting cluster-state memory, prevented by explicit mappings and `total_fields.limit` |
| HTTP 429 vs 503? | 429 = *this client* is over its quota; 503 = *the service* is unable to handle the request right now |
| HTTP 502 vs 504? | 502 = the upstream returned an invalid response; 504 = the upstream did not respond in time (our UPI timeout) |
| Why `lz4` compression on the producer? | Very fast compression and decompression with a good ratio on repetitive JSON, saving network and disk |
