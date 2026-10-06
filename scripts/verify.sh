#!/usr/bin/env bash
# End-to-end health check for the telemetry pipeline.
# Usage: ./scripts/verify.sh
set -uo pipefail

ES="http://localhost:9200"
KIBANA="http://localhost:5601"
LOGSTASH="http://localhost:9600"
TOPIC="platform.telemetry.v1"
GROUP="logstash-telemetry-indexer"

green() { printf '\033[32m%s\033[0m\n' "$*"; }
red()   { printf '\033[31m%s\033[0m\n' "$*"; }
hdr()   { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }

hdr "1. Containers"
docker compose ps --format 'table {{.Name}}\t{{.State}}\t{{.Status}}'

hdr "2. Kafka topic layout"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --describe --topic "$TOPIC" || red "topic describe failed"

hdr "3. Messages per partition (latest offsets)"
docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:29092 --topic "$TOPIC" --time latest || red "offset lookup failed"

hdr "4. Logstash consumer-group lag"
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 --describe --group "$GROUP" || red "consumer group not found yet"

hdr "5. Logstash pipeline throughput"
curl -fs "$LOGSTASH/_node/stats/pipelines/telemetry-ingest" \
  | python3 -c "import json,sys; p=json.load(sys.stdin)['pipelines']['telemetry-ingest']; e=p['events']; q=p['queue']; print('  in=%s filtered=%s out=%s queued=%s' % (e['in'], e['filtered'], e['out'], q.get('events_count', q.get('events'))))" \
  || red "Logstash API not reachable"

hdr "6. Elasticsearch indices"
curl -fs "$ES/_cat/indices/telemetry-*,deadletter-telemetry-*?v&s=index&h=index,health,docs.count,store.size" || red "ES not reachable"

hdr "7. Traffic class split (last 15 minutes)"
curl -fs -H 'Content-Type: application/json' "$ES/telemetry-*/_search?size=0" -d '{
  "query": {"range": {"@timestamp": {"gte": "now-15m"}}},
  "aggs": {"by_class": {"terms": {"field": "traffic_class"}}}
}' | python3 -c "import json,sys; [print('  %-20s %s' % (b['key'], b['doc_count'])) for b in json.load(sys.stdin)['aggregations']['by_class']['buckets']]" \
  || red "aggregation failed"

hdr "8. PII masking check (should show masked VPA + HMAC fingerprint)"
curl -fs -H 'Content-Type: application/json' "$ES/telemetry-*/_search" -d '{
  "size": 1, "_source": ["payment.upi_id", "payment.upi_fingerprint", "user.phone", "client_ip", "message"],
  "query": {"exists": {"field": "payment.upi_id"}},
  "sort": [{"@timestamp": "desc"}]
}' | python3 -c 'import json,sys; h=json.load(sys.stdin)["hits"]["hits"]; print(json.dumps(h[0]["_source"], indent=2) if h else "  no UPI events yet")' \
  || red "search failed"

hdr "9. Kibana"
if curl -fs "$KIBANA/api/status" | grep -q '"level":"available"'; then
  green "Kibana available -> $KIBANA/app/dashboards#/view/quickbite-command-center"
else
  red "Kibana not ready yet"
fi
