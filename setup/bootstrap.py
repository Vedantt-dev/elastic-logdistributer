#!/usr/bin/env python3
"""
One-shot bootstrap for the telemetry stack (standard library only).

    python bootstrap.py elasticsearch   # ILM policy + index templates (run before Logstash starts)
    python bootstrap.py kibana          # data views, Lens visualisations, saved search, dashboard
    python bootstrap.py all             # both, in order

Idempotent: safe to re-run; existing objects are overwritten.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from typing import Any, Dict, List, Optional, Tuple

ES_URL = os.environ.get("ES_URL", "http://localhost:9200").rstrip("/")
KIBANA_URL = os.environ.get("KIBANA_URL", "http://localhost:5601").rstrip("/")
RETENTION = os.environ.get("TELEMETRY_RETENTION", "3d")
KIBANA_VERSION = os.environ.get("KIBANA_VERSION", "8.15.3")

DATA_VIEW_ID = "telemetry-dv"
DEADLETTER_VIEW_ID = "deadletter-dv"
DASHBOARD_ID = "quickbite-command-center"


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def http(method: str, url: str, body: Optional[Any] = None, headers: Optional[Dict[str, str]] = None,
         raw: Optional[bytes] = None, timeout: float = 30.0) -> Tuple[int, str]:
    data = raw if raw is not None else (json.dumps(body).encode("utf-8") if body is not None else None)
    req = urllib.request.Request(url, data=data, method=method)
    if raw is None and body is not None:
        req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8")
    except (urllib.error.URLError, ConnectionError, TimeoutError, OSError) as exc:
        return 0, str(exc)


def wait_for(name: str, url: str, ok: Any, attempts: int = 120, delay: float = 5.0) -> None:
    for i in range(1, attempts + 1):
        status, text = http("GET", url, timeout=10)
        if status == 200 and ok(text):
            print(f"[bootstrap] {name} is ready")
            return
        print(f"[bootstrap] waiting for {name} ({i}/{attempts}) status={status}")
        time.sleep(delay)
    raise SystemExit(f"[bootstrap] {name} did not become ready at {url}")


def expect(status: int, text: str, what: str) -> None:
    if status not in (200, 201):
        raise SystemExit(f"[bootstrap] {what} failed: HTTP {status}: {text[:2000]}")
    print(f"[bootstrap] {what}: OK")


# ---------------------------------------------------------------------------
# Elasticsearch: lifecycle policy + index templates
# ---------------------------------------------------------------------------

ILM_POLICY = {
    "policy": {
        "_meta": {"description": "Daily telemetry indices: hot while written, deleted after retention"},
        "phases": {
            "hot": {"min_age": "0ms", "actions": {"set_priority": {"priority": 100}}},
            "delete": {"min_age": RETENTION, "actions": {"delete": {}}},
        },
    }
}

KEYWORD = {"type": "keyword"}

TELEMETRY_TEMPLATE = {
    "index_patterns": ["telemetry-*"],
    "priority": 500,
    "_meta": {"description": "QuickBite microservice telemetry (time-series, one index per day)"},
    "template": {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "5s",
            "index.lifecycle.name": "telemetry-retention",
            "index.mapping.total_fields.limit": 2000,
            "index.sort.field": ["@timestamp"],
            "index.sort.order": ["desc"],
        },
        "mappings": {
            "dynamic_templates": [
                {"strings_as_keyword": {"match_mapping_type": "string",
                                        "mapping": {"type": "keyword", "ignore_above": 512}}}
            ],
            "properties": {
                "@timestamp": {"type": "date"},
                "event_time": {"type": "date"},
                "event_id": KEYWORD,
                "schema_version": KEYWORD,
                "platform": KEYWORD,
                "environment": KEYWORD,
                "service": KEYWORD,
                "service_version": KEYWORD,
                "host": KEYWORD,
                "level": KEYWORD,
                "operation": KEYWORD,
                "http_method": KEYWORD,
                "endpoint": KEYWORD,
                "status_code": {"type": "short"},
                "traffic_class": KEYWORD,
                "latency_ms": {"type": "integer"},
                "latency_band": KEYWORD,
                "slo_threshold_ms": {"type": "integer"},
                "slo_breach": {"type": "boolean"},
                "ingest_lag_ms": {"type": "long"},
                "trace_id": KEYWORD,
                "span_id": KEYWORD,
                "parent_span_id": KEYWORD,
                "correlation_id": KEYWORD,
                "order_id": KEYWORD,
                "user_id": KEYWORD,
                "session_id": KEYWORD,
                "geo_zone": KEYWORD,
                "geo_area": KEYWORD,
                "zone_location": {"type": "geo_point"},
                "client_ip": {"type": "ip"},
                "client": {"properties": {"platform": KEYWORD, "app_version": KEYWORD}},
                "user": {"properties": {"phone": KEYWORD, "is_bot": {"type": "boolean"}}},
                "retry_count": {"type": "short"},
                "scenario": KEYWORD,
                "message": {"type": "text", "fields": {"raw": {"type": "keyword", "ignore_above": 1024}}},
                "error": {"properties": {"code": KEYWORD, "type": KEYWORD, "retryable": {"type": "boolean"}}},
                "search": {"properties": {"query": KEYWORD, "results_count": {"type": "integer"}}},
                "order": {"properties": {
                    "restaurant_id": KEYWORD, "restaurant_name": KEYWORD, "cuisine": KEYWORD,
                    "items_count": {"type": "short"},
                    "amount_inr": {"type": "scaled_float", "scaling_factor": 100},
                }},
                "payment": {"properties": {
                    "method": KEYWORD, "provider": KEYWORD, "psp": KEYWORD, "currency": KEYWORD,
                    "upi_id": KEYWORD, "upi_fingerprint": KEYWORD,
                    "card_token": KEYWORD, "card_network": KEYWORD, "card_last4": KEYWORD,
                    "amount_inr": {"type": "scaled_float", "scaling_factor": 100},
                    "attempt": {"type": "short"},
                }},
                "driver": {"properties": {
                    "driver_id": KEYWORD, "vehicle": KEYWORD, "delivery_state": KEYWORD,
                    "eta_min": {"type": "short"}, "distance_km": {"type": "float"},
                    "location": {"type": "geo_point"},
                }},
                "notification": {"properties": {"channel": KEYWORD, "template": KEYWORD, "provider": KEYWORD}},
                "kafka": {"properties": {"topic": KEYWORD, "partition": {"type": "integer"},
                                         "offset": {"type": "long"}, "key": KEYWORD}},
                "tags": KEYWORD,
            },
        },
    },
}

DEADLETTER_TEMPLATE = {
    "index_patterns": ["deadletter-telemetry-*"],
    "priority": 500,
    "template": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0,
                     "index.lifecycle.name": "telemetry-retention"},
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
                "message": {"type": "text"},
                "tags": KEYWORD,
                "deadletter": {"properties": {"reason": KEYWORD, "kafka_partition": KEYWORD,
                                              "kafka_offset": KEYWORD, "kafka_key": KEYWORD}},
            }
        },
    },
}


def bootstrap_elasticsearch() -> None:
    wait_for("Elasticsearch", f"{ES_URL}/_cluster/health",
             lambda t: json.loads(t).get("status") in ("yellow", "green"))
    expect(*http("PUT", f"{ES_URL}/_ilm/policy/telemetry-retention", ILM_POLICY), "ILM policy telemetry-retention")
    expect(*http("PUT", f"{ES_URL}/_index_template/telemetry", TELEMETRY_TEMPLATE), "index template telemetry")
    expect(*http("PUT", f"{ES_URL}/_index_template/deadletter-telemetry", DEADLETTER_TEMPLATE),
           "index template deadletter-telemetry")


# ---------------------------------------------------------------------------
# Kibana: Lens builders
# ---------------------------------------------------------------------------

LAYER = "layer1"
EMPTY_QUERY = {"query": "", "language": "kuery"}


def col_count(label: str, kql_filter: Optional[str] = None) -> Dict[str, Any]:
    col: Dict[str, Any] = {"label": label, "customLabel": True, "dataType": "number", "operationType": "count",
                           "isBucketed": False, "scale": "ratio", "sourceField": "___records___",
                           "params": {"emptyAsNull": True}}
    if kql_filter:
        col["filter"] = {"query": kql_filter, "language": "kuery"}
    return col


def col_metric(op: str, field: str, label: str, percentile: Optional[int] = None) -> Dict[str, Any]:
    col: Dict[str, Any] = {"label": label, "customLabel": True, "dataType": "number", "operationType": op,
                           "sourceField": field, "isBucketed": False, "scale": "ratio",
                           "params": {"emptyAsNull": True}}
    if op == "percentile":
        col["params"] = {"percentile": percentile or 95}
    return col


def col_date(field: str = "@timestamp") -> Dict[str, Any]:
    return {"label": field, "dataType": "date", "operationType": "date_histogram", "sourceField": field,
            "isBucketed": True, "scale": "interval",
            "params": {"interval": "auto", "includeEmptyRows": True, "dropPartials": False}}


def col_terms(field: str, order_by: str, size: int = 10, label: Optional[str] = None,
              data_type: str = "string") -> Dict[str, Any]:
    return {"label": label or f"Top values of {field}", "customLabel": label is not None,
            "dataType": data_type, "operationType": "terms", "sourceField": field,
            "isBucketed": True, "scale": "ordinal",
            "params": {"size": size, "orderBy": {"type": "column", "columnId": order_by},
                       "orderDirection": "desc", "otherBucket": True, "missingBucket": False,
                       "parentFormat": {"id": "terms"}, "include": [], "exclude": [],
                       "includeIsRegex": False, "excludeIsRegex": False}}


def lens(obj_id: str, title: str, vis_type: str, columns: Dict[str, Dict[str, Any]], order: List[str],
         visualization: Dict[str, Any], query: str = "") -> Dict[str, Any]:
    return {
        "type": "lens",
        "id": obj_id,
        "attributes": {
            "title": title,
            "description": "",
            "visualizationType": vis_type,
            "state": {
                "datasourceStates": {
                    "formBased": {
                        "layers": {
                            LAYER: {"columns": columns, "columnOrder": order,
                                    "incompleteColumns": {}, "sampling": 1}
                        }
                    }
                },
                "visualization": visualization,
                "query": {"query": query, "language": "kuery"},
                "filters": [],
                "internalReferences": [],
                "adHocDataViews": {},
            },
        },
        "references": [
            {"type": "index-pattern", "id": DATA_VIEW_ID, "name": "indexpattern-datasource-layer-layer1"},
        ],
        "coreMigrationVersion": "8.8.0",
        "typeMigrationVersion": "8.9.0",
    }


def metric_panel(obj_id: str, title: str, column: Dict[str, Any]) -> Dict[str, Any]:
    return lens(obj_id, title, "lnsMetric", {"m": column}, ["m"],
                {"layerId": LAYER, "layerType": "data", "metricAccessor": "m"})


def xy_panel(obj_id: str, title: str, series_type: str, x: Dict[str, Any], y: Dict[str, Any],
             split: Optional[Dict[str, Any]] = None, query: str = "") -> Dict[str, Any]:
    columns = {"x": x, "y": y}
    order = ["x", "y"]
    layer: Dict[str, Any] = {"layerId": LAYER, "layerType": "data", "seriesType": series_type,
                             "xAccessor": "x", "accessors": ["y"]}
    if split is not None:
        columns["s"] = split
        order = ["x", "s", "y"]
        layer["splitAccessor"] = "s"
    vis = {"legend": {"isVisible": True, "position": "right"}, "valueLabels": "hide",
           "fittingFunction": "None", "preferredSeriesType": series_type, "layers": [layer]}
    return lens(obj_id, title, "lnsXY", columns, order, vis, query)


def build_kibana_objects() -> List[Dict[str, Any]]:
    objects: List[Dict[str, Any]] = []

    # -- data views ------------------------------------------------------------
    objects.append({
        "type": "index-pattern", "id": DATA_VIEW_ID,
        "attributes": {"title": "telemetry-*", "name": "QuickBite Telemetry", "timeFieldName": "@timestamp"},
        "references": [], "coreMigrationVersion": "8.8.0", "typeMigrationVersion": "8.0.0",
    })
    objects.append({
        "type": "index-pattern", "id": DEADLETTER_VIEW_ID,
        "attributes": {"title": "deadletter-telemetry-*", "name": "Telemetry Dead Letters",
                       "timeFieldName": "@timestamp"},
        "references": [], "coreMigrationVersion": "8.8.0", "typeMigrationVersion": "8.0.0",
    })

    # -- KPI tiles ---------------------------------------------------------------
    objects.append(metric_panel("cc-kpi-events", "Events ingested", col_count("Events")))
    objects.append(metric_panel("cc-kpi-5xx", "Server errors (5xx)",
                                col_count("5xx responses", "status_code >= 500")))
    objects.append(metric_panel("cc-kpi-p95", "p95 latency (ms)",
                                col_metric("percentile", "latency_ms", "p95 latency (ms)", 95)))
    objects.append(metric_panel("cc-kpi-slo", "SLO breaches",
                                col_count("Requests over SLO", "slo_breach : true")))

    # -- time series ---------------------------------------------------------------
    objects.append(xy_panel("cc-traffic-class", "Traffic by status class", "bar_stacked",
                            col_date(), col_count("Events"),
                            col_terms("traffic_class", "y", 5, "Traffic class")))
    objects.append(xy_panel("cc-latency-service", "p95 latency by service", "line",
                            col_date(), col_metric("percentile", "latency_ms", "p95 latency (ms)", 95),
                            col_terms("service", "y", 5, "Service")))
    objects.append(xy_panel("cc-scenario-timeline", "Injected scenario timeline (ground truth)", "bar_stacked",
                            col_date(), col_count("Events"),
                            col_terms("scenario", "y", 6, "Scenario")))
    objects.append(xy_panel("cc-ingest-lag", "Pipeline ingest lag (backpressure indicator)", "line",
                            col_date(), col_metric("max", "ingest_lag_ms", "Max ingest lag (ms)")))

    # -- breakdowns ---------------------------------------------------------------
    objects.append(xy_panel("cc-errors-zone", "Errors by city zone", "bar_horizontal_stacked",
                            col_terms("geo_zone", "y", 6, "Zone"), col_count("Errors"),
                            col_terms("traffic_class", "y", 3, "Traffic class"),
                            query="status_code >= 400"))
    objects.append(xy_panel("cc-kafka-partitions", "Events per Kafka partition (key = correlation_id)",
                            "bar", col_terms("kafka.partition", "y", 12, "Partition", "number"),
                            col_count("Events")))

    objects.append(lens(
        "cc-payment-outcomes", "Payment outcomes by method", "lnsPie",
        {"g1": col_terms("payment.method", "m", 5, "Method"),
         "g2": col_terms("traffic_class", "m", 3, "Outcome"),
         "m": col_count("Authorisations")},
        ["g1", "g2", "m"],
        {"shape": "donut", "layers": [{
            "layerId": LAYER, "layerType": "data", "primaryGroups": ["g1", "g2"], "metrics": ["m"],
            "numberDisplay": "percent", "categoryDisplay": "default", "legendDisplay": "default",
            "nestedLegend": False, "emptySizeRatio": 0.3}]},
        query='service : "payment-gateway" and operation : "payment.authorize"'))

    objects.append(lens(
        "cc-top-errors", "Top error codes", "lnsDatatable",
        {"c1": col_terms("service", "c3", 5, "Service"),
         "c2": col_terms("error.code", "c3", 15, "Error code"),
         "c3": col_count("Count"),
         "c4": col_metric("average", "latency_ms", "Avg latency (ms)")},
        ["c1", "c2", "c3", "c4"],
        {"layerId": LAYER, "layerType": "data",
         "columns": [{"columnId": "c1"}, {"columnId": "c2"}, {"columnId": "c3"}, {"columnId": "c4"}]},
        query="status_code >= 400"))

    # -- saved search: live error stream ------------------------------------------
    objects.append({
        "type": "search", "id": "cc-error-stream",
        "attributes": {
            "title": "Live error stream (4xx/5xx)",
            "description": "",
            "columns": ["service", "operation", "status_code", "error.code", "geo_zone",
                        "latency_ms", "correlation_id", "message"],
            "sort": [["@timestamp", "desc"]],
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({
                "query": {"query": "status_code >= 400", "language": "kuery"},
                "filter": [],
                "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
            })},
        },
        "references": [{"name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                        "type": "index-pattern", "id": DATA_VIEW_ID}],
        "coreMigrationVersion": "8.8.0", "typeMigrationVersion": "8.0.0",
    })

    # -- dashboard ---------------------------------------------------------------------
    layout = [
        # (object id, type, x, y, w, h)
        ("cc-kpi-events", "lens", 0, 0, 12, 7),
        ("cc-kpi-5xx", "lens", 12, 0, 12, 7),
        ("cc-kpi-p95", "lens", 24, 0, 12, 7),
        ("cc-kpi-slo", "lens", 36, 0, 12, 7),
        ("cc-traffic-class", "lens", 0, 7, 24, 13),
        ("cc-latency-service", "lens", 24, 7, 24, 13),
        ("cc-errors-zone", "lens", 0, 20, 16, 13),
        ("cc-payment-outcomes", "lens", 16, 20, 16, 13),
        ("cc-kafka-partitions", "lens", 32, 20, 16, 13),
        ("cc-scenario-timeline", "lens", 0, 33, 24, 12),
        ("cc-ingest-lag", "lens", 24, 33, 24, 12),
        ("cc-top-errors", "lens", 0, 45, 20, 16),
        ("cc-error-stream", "search", 20, 45, 28, 16),
    ]
    panels, refs = [], []
    for idx, (obj_id, obj_type, x, y, w, h) in enumerate(layout, start=1):
        panel_key = f"p{idx}"
        panels.append({"version": KIBANA_VERSION, "type": obj_type,
                       "gridData": {"x": x, "y": y, "w": w, "h": h, "i": panel_key},
                       "panelIndex": panel_key, "embeddableConfig": {"enhancements": {}},
                       "panelRefName": f"panel_{panel_key}"})
        refs.append({"name": f"panel_{panel_key}", "type": obj_type, "id": obj_id})

    objects.append({
        "type": "dashboard", "id": DASHBOARD_ID,
        "attributes": {
            "title": "QuickBite Command Center",
            "description": "Real-time health of auth, catalogue, payments, dispatch and notifications",
            "panelsJSON": json.dumps(panels),
            "optionsJSON": json.dumps({"useMargins": True, "syncColors": True, "syncCursor": True,
                                       "syncTooltips": False, "hidePanelTitles": False}),
            "timeRestore": True,
            "timeFrom": "now-15m",
            "timeTo": "now",
            "refreshInterval": {"pause": False, "value": 10000},
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": EMPTY_QUERY, "filter": []})},
        },
        "references": refs,
        "coreMigrationVersion": "8.8.0", "typeMigrationVersion": "8.9.0",
    })
    return objects


def import_saved_objects(objects: List[Dict[str, Any]]) -> None:
    ndjson = "\n".join(json.dumps(o, separators=(",", ":")) for o in objects) + "\n"
    boundary = f"----bda{uuid.uuid4().hex}"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="command-center.ndjson"\r\n'
        f"Content-Type: application/ndjson\r\n\r\n"
        f"{ndjson}\r\n"
        f"--{boundary}--\r\n"
    ).encode("utf-8")
    status, text = http("POST", f"{KIBANA_URL}/api/saved_objects/_import?overwrite=true", raw=body,
                        headers={"kbn-xsrf": "true", "Content-Type": f"multipart/form-data; boundary={boundary}"},
                        timeout=120)
    expect(status, text, "Kibana saved-object import")
    result = json.loads(text)
    if not result.get("success", False):
        print(json.dumps(result.get("errors", []), indent=2))
        raise SystemExit("[bootstrap] some saved objects failed to import (see errors above)")
    print(f"[bootstrap] imported {result.get('successCount')} saved objects")


def bootstrap_kibana() -> None:
    def kibana_ready(text: str) -> bool:
        try:
            return json.loads(text)["status"]["overall"]["level"] == "available"
        except (KeyError, ValueError, TypeError):
            return False

    wait_for("Kibana", f"{KIBANA_URL}/api/status", kibana_ready)
    import_saved_objects(build_kibana_objects())
    expect(*http("POST", f"{KIBANA_URL}/api/data_views/default",
                 {"data_view_id": DATA_VIEW_ID, "force": True}, headers={"kbn-xsrf": "true"}),
           "default data view")
    print(f"[bootstrap] dashboard: {os.environ.get('KIBANA_PUBLIC_URL', 'http://localhost:5601')}"
          f"/app/dashboards#/view/{DASHBOARD_ID}")


def main() -> int:
    target = sys.argv[1] if len(sys.argv) > 1 else "all"
    if target in ("elasticsearch", "all"):
        bootstrap_elasticsearch()
    if target in ("kibana", "all"):
        bootstrap_kibana()
    if target not in ("elasticsearch", "kibana", "all"):
        raise SystemExit("usage: bootstrap.py [elasticsearch|kibana|all]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
