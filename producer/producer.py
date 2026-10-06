#!/usr/bin/env python3
"""
QuickBite Telemetry Simulator
=============================

A multi-threaded simulator of a hyper-scale food-delivery platform
(Swiggy / Zomato / Uber Eats style). Independent "microservices" run as
worker pools connected by bounded in-memory queues and emit structured
JSON logs into Apache Kafka.

User journey modelled (one trace per journey):

    auth-vault  ->  menu-catalog (search, menu, checkout)  ->  payment-gateway
        ->  driver-dispatcher (allocate, pickup, location pings, delivered)
        ->  notification-service (push / SMS / WhatsApp fan-out)

Operational scenarios are injected by a ScenarioController that cycles
through phases (or can be pinned with --scenario):

    normal                    steady-state traffic
    flash_sale                4x traffic, menu latency, 429 throttling, stock-outs
    upi_timeout_spike         NPCI switch degradation -> UPI 504s + retries
    driver_shortage           heavy rain in one zone -> 503 NO_DRIVER_AVAILABLE
    auth_credential_stuffing  bot traffic -> 401 / 429 storm on auth-vault
    notification_outage       push provider down -> 502s + SMS fallback

Every record carries trace_id / span_id / parent_span_id, a journey-level
correlation_id (also the Kafka partition key, so all events of one journey
land on one partition in order), order_id once checkout happens, HTTP status
code, latency in ms, geo zone and the ground-truth scenario label.

Some records deliberately contain PII (UPI VPAs, card tokens, phone numbers,
client IPs and - rarely - a raw test card PAN inside free text) so that the
Logstash pipeline can demonstrate masking.

Usage:
    python producer.py                                   # Kafka on localhost:9092
    python producer.py --bootstrap kafka:29092 --rate 10
    python producer.py --scenario upi_timeout_spike
    python producer.py --dry-run --duration 10           # print JSON to stdout, no Kafka
"""

from __future__ import annotations

import argparse
import heapq
import itertools
import json
import logging
import math
import os
import queue
import random
import signal
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

LOG = logging.getLogger("simulator")

PLATFORM = "quickbite"
ENVIRONMENT = "prod-sim"
SCHEMA_VERSION = "1.2"

# --------------------------------------------------------------------------
# Reference data
# --------------------------------------------------------------------------

ZONES: Dict[str, Dict[str, Any]] = {
    "Delhi": {
        "weight": 0.22,
        "centroid": (28.6139, 77.2090),
        "areas": ["Connaught Place", "Hauz Khas", "Saket", "Dwarka", "Karol Bagh", "Lajpat Nagar"],
    },
    "Mumbai": {
        "weight": 0.22,
        "centroid": (19.0760, 72.8777),
        "areas": ["Andheri West", "Bandra", "Powai", "Lower Parel", "Colaba", "Thane"],
    },
    "Bengaluru": {
        "weight": 0.22,
        "centroid": (12.9716, 77.5946),
        "areas": ["Koramangala", "Indiranagar", "HSR Layout", "Whitefield", "Jayanagar", "Electronic City"],
    },
    "Hyderabad": {
        "weight": 0.12,
        "centroid": (17.3850, 78.4867),
        "areas": ["Gachibowli", "Banjara Hills", "Madhapur", "Kukatpally", "Secunderabad"],
    },
    "Chennai": {
        "weight": 0.10,
        "centroid": (13.0827, 80.2707),
        "areas": ["T. Nagar", "Adyar", "Velachery", "Anna Nagar", "OMR"],
    },
    "Pune": {
        "weight": 0.12,
        "centroid": (18.5204, 73.8567),
        "areas": ["Koregaon Park", "Hinjewadi", "Kothrud", "Viman Nagar", "Baner"],
    },
}
ZONE_NAMES = list(ZONES.keys())
ZONE_WEIGHTS = [ZONES[z]["weight"] for z in ZONE_NAMES]

CUISINES = ["North Indian", "South Indian", "Biryani", "Chinese", "Pizza", "Burgers",
            "Desserts", "Healthy", "Street Food", "Kerala", "Bengali", "Continental"]
SEARCH_TERMS = ["biryani", "pizza", "dosa", "paneer tikka", "momos", "burger", "thali",
                "rolls", "ice cream", "shawarma", "idli", "pav bhaji", "chole bhature", "noodles"]
RESTAURANT_PREFIXES = ["Royal", "Spice", "Urban", "Desi", "Tandoori", "Coastal", "Green",
                       "Mumbai", "Delhi", "Chennai", "Hyderabadi", "Punjabi", "Udupi", "Golden"]
RESTAURANT_SUFFIXES = ["Kitchen", "Darbar", "Bhavan", "Express", "House", "Grill", "Cafe",
                       "Dhaba", "Bistro", "Point", "Corner", "Junction"]

FIRST_NAMES = ["rahul", "priya", "amit", "sneha", "arjun", "kavya", "rohan", "ananya",
               "vikram", "isha", "aditya", "meera", "karan", "neha", "siddharth", "pooja"]
LAST_NAMES = ["sharma", "verma", "iyer", "reddy", "patel", "nair", "gupta", "mehta",
              "rao", "das", "kulkarni", "singh", "menon", "joshi", "bose", "pillai"]
UPI_HANDLES = ["okhdfcbank", "okicici", "oksbi", "okaxis", "ybl", "paytm", "ibl"]

# Published card-network TEST numbers only (never real cards).
TEST_CARD_PANS = [("VISA", "4111111111111111"), ("MASTERCARD", "5555555555554444"),
                  ("VISA", "4012888888881881"), ("MASTERCARD", "5105105105105100")]

CLIENT_PLATFORMS = [("android", ["7.3.1", "7.3.0", "7.2.4"], 0.62),
                    ("ios", ["7.3.0", "7.2.9"], 0.28),
                    ("web", ["2026.10.1"], 0.10)]
INDIAN_IP_PREFIXES = ["49.36", "106.51", "117.96", "157.48", "182.69", "223.185", "103.21"]
BOT_IP_PREFIX = "185.220.101"
VEHICLES = ["bike", "bike", "bike", "e-bike", "scooter", "bicycle"]

SERVICE_VERSIONS = {
    "auth-vault": "3.8.2",
    "menu-catalog": "5.21.0",
    "payment-gateway": "2.14.3",
    "driver-dispatcher": "4.2.7",
    "notification-service": "1.9.12",
}


def build_restaurants(count_per_zone: int = 40) -> Dict[str, List[Dict[str, Any]]]:
    rng = random.Random(42)  # deterministic catalogue across runs
    catalogue: Dict[str, List[Dict[str, Any]]] = {}
    serial = 1000
    for zone, meta in ZONES.items():
        rows = []
        for _ in range(count_per_zone):
            serial += 1
            rows.append({
                "restaurant_id": f"RST{serial}",
                "restaurant_name": f"{rng.choice(RESTAURANT_PREFIXES)} {rng.choice(RESTAURANT_SUFFIXES)}",
                "cuisine": rng.choice(CUISINES),
                "area": rng.choice(meta["areas"]),
                "avg_item_price": rng.choice([89, 129, 149, 199, 249, 299, 349]),
            })
        catalogue[zone] = rows
    return catalogue


RESTAURANTS = build_restaurants()

# --------------------------------------------------------------------------
# Scenario model
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ScenarioProfile:
    name: str
    description: str
    traffic_multiplier: float = 1.0
    bot_share: float = 0.01
    auth_failure_rate: float = 0.015
    menu_latency_multiplier: float = 1.0
    menu_throttle_rate: float = 0.002
    stockout_rate: float = 0.02
    payment_latency_multiplier: float = 1.0
    upi_timeout_rate: float = 0.01
    upi_latency_multiplier: float = 1.0
    card_decline_rate: float = 0.03
    driver_unavailable_rate: float = 0.04
    affected_zone: Optional[str] = None
    push_failure_rate: float = 0.01


SCENARIOS: Dict[str, ScenarioProfile] = {
    "normal": ScenarioProfile(
        name="normal",
        description="Steady-state traffic with background error rates",
    ),
    "flash_sale": ScenarioProfile(
        name="flash_sale",
        description="Flash sale: 4x traffic, catalogue latency, 429 throttling, stock-outs",
        traffic_multiplier=4.0,
        menu_latency_multiplier=2.8,
        menu_throttle_rate=0.07,
        stockout_rate=0.12,
        payment_latency_multiplier=1.6,
        driver_unavailable_rate=0.10,
    ),
    "upi_timeout_spike": ScenarioProfile(
        name="upi_timeout_spike",
        description="NPCI switch degraded: UPI collect requests time out (504) and are retried",
        traffic_multiplier=1.2,
        upi_timeout_rate=0.38,
        upi_latency_multiplier=6.0,
        payment_latency_multiplier=1.4,
    ),
    "driver_shortage": ScenarioProfile(
        name="driver_shortage",
        description="Heavy rain in Bengaluru: driver pool exhausted, 503 NO_DRIVER_AVAILABLE",
        traffic_multiplier=1.4,
        driver_unavailable_rate=0.55,
        affected_zone="Bengaluru",
    ),
    "auth_credential_stuffing": ScenarioProfile(
        name="auth_credential_stuffing",
        description="Credential-stuffing botnet: 401/429 storm on auth-vault",
        traffic_multiplier=2.0,
        bot_share=0.55,
    ),
    "notification_outage": ScenarioProfile(
        name="notification_outage",
        description="Push provider degraded: 502 PROVIDER_ERROR with SMS fallback",
        push_failure_rate=0.6,
    ),
}

DEFAULT_SCHEDULE: List[Tuple[str, int]] = [
    ("normal", 90),
    ("flash_sale", 45),
    ("normal", 60),
    ("upi_timeout_spike", 45),
    ("normal", 60),
    ("driver_shortage", 45),
    ("normal", 60),
    ("auth_credential_stuffing", 30),
    ("normal", 60),
    ("notification_outage", 30),
]


class ScenarioController(threading.Thread):
    """Cycles through scenario phases, or stays pinned to one scenario."""

    def __init__(self, stop: threading.Event, pinned: Optional[str], phase_scale: float):
        super().__init__(name="scenario-controller", daemon=True)
        self._stop = stop
        self._pinned = pinned
        self._phase_scale = phase_scale
        self._current = SCENARIOS[pinned or "normal"]
        self._phase_started = time.monotonic()
        self._lock = threading.Lock()

    @property
    def current(self) -> ScenarioProfile:
        with self._lock:
            return self._current

    def _switch(self, name: str) -> None:
        with self._lock:
            self._current = SCENARIOS[name]
            self._phase_started = time.monotonic()
        LOG.warning(">>> scenario phase -> %s : %s", name, SCENARIOS[name].description)

    def run(self) -> None:
        if self._pinned:
            LOG.warning(">>> scenario pinned -> %s : %s", self._pinned, SCENARIOS[self._pinned].description)
            self._stop.wait()
            return
        for name, seconds in itertools.cycle(DEFAULT_SCHEDULE):
            self._switch(name)
            if self._stop.wait(max(1.0, seconds * self._phase_scale)):
                return


# --------------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------------


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def hex_id(n_bytes: int) -> str:
    return os.urandom(n_bytes).hex()


def short_id(n: int = 6) -> str:
    return uuid.uuid4().hex[:n].upper()


def lognormal_ms(median_ms: float, sigma: float = 0.45, multiplier: float = 1.0,
                 floor: int = 2, cap: int = 30000) -> int:
    value = median_ms * multiplier * math.exp(random.gauss(0.0, sigma))
    return int(max(floor, min(cap, value)))


def chance(p: float) -> bool:
    return random.random() < p


def weighted_choice(items: List[Any], weights: List[float]) -> Any:
    return random.choices(items, weights=weights, k=1)[0]


def jitter_coordinates(lat: float, lon: float, radius_deg: float = 0.06) -> Dict[str, float]:
    return {"lat": round(lat + random.uniform(-radius_deg, radius_deg), 6),
            "lon": round(lon + random.uniform(-radius_deg, radius_deg), 6)}


class Stats:
    """Thread-safe counters for the console reporter."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Dict[str, int] = {}

    def incr(self, key: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + amount

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._counters)


# --------------------------------------------------------------------------
# Emitters (Kafka / stdout)
# --------------------------------------------------------------------------


class Emitter:
    def send(self, key: str, value: bytes, headers: Optional[List[Tuple[str, bytes]]] = None) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


class StdoutEmitter(Emitter):
    def __init__(self, stats: Stats):
        self._stats = stats
        self._lock = threading.Lock()

    def send(self, key: str, value: bytes, headers: Optional[List[Tuple[str, bytes]]] = None) -> None:
        with self._lock:
            sys.stdout.write(value.decode("utf-8", errors="replace") + "\n")
            sys.stdout.flush()
        self._stats.incr("delivered")

    def close(self) -> None:
        sys.stdout.flush()


class KafkaEmitter(Emitter):
    """
    Idempotent, compressed, batched Kafka producer.

    - acks=all + enable.idempotence: no duplicates from producer retries and
      no data loss while at least one in-sync replica is alive.
    - Message key = correlation_id: every event of a journey hashes to the
      same partition, preserving per-journey ordering.
    - BufferError (local queue full) is the client-side backpressure signal:
      we poll to drain delivery reports and retry instead of dropping.
    """

    def __init__(self, bootstrap: str, topic: str, stats: Stats, stop: threading.Event):
        from confluent_kafka import Producer  # imported lazily so --dry-run needs no deps

        self._topic = topic
        self._stats = stats
        self._stop = stop
        self._producer = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": "quickbite-telemetry-simulator",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 25,
            "batch.size": 262144,
            "queue.buffering.max.messages": 200000,
            "queue.buffering.max.kbytes": 262144,
            "message.timeout.ms": 60000,
            "retry.backoff.ms": 250,
            "socket.keepalive.enable": True,
        })
        self._poller = threading.Thread(target=self._poll_loop, name="kafka-poller", daemon=True)
        self._poller.start()

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            self._producer.poll(0.2)

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self._stats.incr("delivery_failed")
            LOG.error("delivery failed: %s", err)
        else:
            self._stats.incr("delivered")
            self._stats.incr(f"partition_{msg.partition()}")

    def send(self, key: str, value: bytes, headers: Optional[List[Tuple[str, bytes]]] = None) -> None:
        while True:
            try:
                self._producer.produce(self._topic, key=key.encode("utf-8"), value=value,
                                       headers=headers or [], on_delivery=self._on_delivery)
                return
            except BufferError:
                self._stats.incr("producer_buffer_full")
                self._producer.poll(0.1)
                if self._stop.is_set():
                    return

    def close(self) -> None:
        remaining = self._producer.flush(20)
        if remaining:
            LOG.error("%d messages were still undelivered at shutdown", remaining)


# --------------------------------------------------------------------------
# Delayed-task scheduler (retries with backoff, delivery lifecycle)
# --------------------------------------------------------------------------


class Scheduler(threading.Thread):
    def __init__(self, stop: threading.Event):
        super().__init__(name="scheduler", daemon=True)
        self._stop = stop
        self._heap: List[Tuple[float, int, Callable[..., None], Tuple[Any, ...]]] = []
        self._cv = threading.Condition()
        self._seq = itertools.count()

    def call_later(self, delay_s: float, fn: Callable[..., None], *args: Any) -> None:
        with self._cv:
            heapq.heappush(self._heap, (time.monotonic() + delay_s, next(self._seq), fn, args))
            self._cv.notify()

    def pending(self) -> int:
        with self._cv:
            return len(self._heap)

    def run(self) -> None:
        while not self._stop.is_set():
            with self._cv:
                if not self._heap:
                    self._cv.wait(0.5)
                    continue
                due, _, fn, args = self._heap[0]
                delay = due - time.monotonic()
                if delay > 0:
                    self._cv.wait(min(delay, 0.5))
                    continue
                heapq.heappop(self._heap)
            try:
                fn(*args)
            except Exception:  # noqa: BLE001 - a bad callback must not kill the scheduler
                LOG.exception("scheduled task failed")


# --------------------------------------------------------------------------
# Journey context
# --------------------------------------------------------------------------


@dataclass
class Journey:
    correlation_id: str
    trace_id: str
    user_id: str
    session_id: str
    zone: str
    area: str
    client_platform: str
    app_version: str
    client_ip: str
    phone: str
    is_bot: bool
    scenario: str
    started_at: float
    last_span_id: Optional[str] = None
    order_id: Optional[str] = None
    restaurant: Optional[Dict[str, Any]] = None
    items_count: int = 0
    amount_inr: float = 0.0
    payment_method: Optional[str] = None
    upi_id: Optional[str] = None
    upi_provider: Optional[str] = None
    card_network: Optional[str] = None
    card_pan: Optional[str] = None
    card_token: Optional[str] = None
    driver: Optional[Dict[str, Any]] = None


def new_journey(scenario: ScenarioProfile) -> Journey:
    zone = weighted_choice(ZONE_NAMES, ZONE_WEIGHTS)
    platform_row = weighted_choice(CLIENT_PLATFORMS, [row[2] for row in CLIENT_PLATFORMS])
    is_bot = chance(scenario.bot_share)
    if is_bot:
        client_ip = f"{BOT_IP_PREFIX}.{random.randint(2, 254)}"
    else:
        client_ip = f"{random.choice(INDIAN_IP_PREFIXES)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
    return Journey(
        correlation_id=f"JRN-{uuid.uuid4().hex[:16]}",
        trace_id=hex_id(16),
        user_id=f"U{random.randint(1000000, 9999999)}",
        session_id=f"S-{uuid.uuid4().hex[:12]}",
        zone=zone,
        area=random.choice(ZONES[zone]["areas"]),
        client_platform=platform_row[0],
        app_version=random.choice(platform_row[1]),
        client_ip=client_ip,
        phone=f"+91 {random.choice('6789')}{random.randint(100000000, 999999999)}",
        is_bot=is_bot,
        scenario=scenario.name,
        started_at=time.time(),
    )


# --------------------------------------------------------------------------
# Runtime + service base class
# --------------------------------------------------------------------------


class Runtime:
    def __init__(self, args: argparse.Namespace, emitter: Emitter, stats: Stats,
                 controller: ScenarioController, scheduler: Scheduler, stop: threading.Event):
        self.args = args
        self.emitter = emitter
        self.stats = stats
        self.controller = controller
        self.scheduler = scheduler
        self.stop = stop
        self.services: Dict[str, "Service"] = {}

    def service(self, name: str) -> "Service":
        return self.services[name]


class Service:
    """
    A decoupled microservice: a bounded inbox (queue.Queue) drained by a pool
    of worker threads. When the inbox is full the caller cannot enqueue within
    its timeout and the request is load-shed with a 503 - exactly how real
    services protect themselves under backpressure.
    """

    name = "service"

    def __init__(self, rt: Runtime, workers: int, queue_size: int):
        self.rt = rt
        self.workers = workers
        self.inbox: "queue.Queue[Tuple[Journey, str, Dict[str, Any]]]" = queue.Queue(maxsize=queue_size)
        replica_set = uuid.uuid4().hex[:5]
        self.pods = [f"{self.name}-{replica_set}-{i}" for i in range(workers)]
        self.version = SERVICE_VERSIONS[self.name]
        self._threads: List[threading.Thread] = []

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        for idx in range(self.workers):
            t = threading.Thread(target=self._worker, args=(idx,), name=f"{self.name}-w{idx}", daemon=True)
            t.start()
            self._threads.append(t)

    def submit(self, journey: Journey, step: str, payload: Optional[Dict[str, Any]] = None,
               timeout: float = 0.25) -> bool:
        try:
            self.inbox.put((journey, step, payload or {}), timeout=timeout)
            return True
        except queue.Full:
            self.rt.stats.incr("load_shed")
            self.rt.stats.incr(f"load_shed.{self.name}")
            self.log(journey, pod=random.choice(self.pods), operation=f"{step}.rejected",
                     method="POST", endpoint=f"/internal/{step}", status=503,
                     latency_ms=random.randint(1, 4),
                     message=f"{self.name} inbox saturated ({self.inbox.qsize()} queued) - request shed",
                     error={"code": "UPSTREAM_SATURATED", "type": "LoadShed", "retryable": True},
                     simulate_work=False)
            return False

    def _worker(self, idx: int) -> None:
        pod = self.pods[idx]
        while not self.rt.stop.is_set():
            try:
                journey, step, payload = self.inbox.get(timeout=0.5)
            except queue.Empty:
                continue
            handler = getattr(self, f"handle_{step}", None)
            try:
                if handler is None:
                    raise AttributeError(f"{self.name} has no handler for step '{step}'")
                handler(journey, payload, pod)
            except Exception:  # noqa: BLE001 - keep the worker alive
                LOG.exception("%s worker crashed on step %s", self.name, step)
            finally:
                self.inbox.task_done()

    # -- logging -----------------------------------------------------------

    def log(self, journey: Journey, *, pod: str, operation: str, method: str, endpoint: str,
            status: int, latency_ms: int, message: str, error: Optional[Dict[str, Any]] = None,
            extra: Optional[Dict[str, Any]] = None, level: Optional[str] = None,
            retry_count: int = 0, simulate_work: bool = True) -> str:
        if simulate_work:
            # Real wall-clock work proportional to simulated latency so that slow
            # dependencies genuinely back up queues (time_scale compresses it).
            time.sleep(min(0.75, latency_ms * self.rt.args.time_scale / 1000.0))

        if level is None:
            level = "ERROR" if status >= 500 else "WARN" if status >= 400 else "INFO"
        span_id = hex_id(8)
        record: Dict[str, Any] = {
            "event_id": str(uuid.uuid4()),
            "event_time": utc_now_iso(),
            "schema_version": SCHEMA_VERSION,
            "platform": PLATFORM,
            "environment": ENVIRONMENT,
            "service": self.name,
            "service_version": self.version,
            "host": pod,
            "level": level,
            "operation": operation,
            "http_method": method,
            "endpoint": endpoint,
            "status_code": status,
            "latency_ms": latency_ms,
            "trace_id": journey.trace_id,
            "span_id": span_id,
            "parent_span_id": journey.last_span_id,
            "correlation_id": journey.correlation_id,
            "order_id": journey.order_id,
            "user_id": journey.user_id,
            "session_id": journey.session_id,
            "geo_zone": journey.zone,
            "geo_area": journey.area,
            "client_ip": journey.client_ip,
            "client": {"platform": journey.client_platform, "app_version": journey.app_version},
            "user": {"phone": journey.phone, "is_bot": journey.is_bot},
            "retry_count": retry_count,
            "scenario": self.rt.controller.current.name,
            "message": message,
        }
        if error:
            record["error"] = error
        if extra:
            record.update(extra)
        journey.last_span_id = span_id

        self.rt.stats.incr("events")
        self.rt.stats.incr(f"status_{status // 100}xx")

        if self.rt.args.malformed_rate > 0 and chance(self.rt.args.malformed_rate):
            # Poison pill: a truncated, non-JSON payload. Logstash routes it to the dead-letter index.
            self.rt.stats.incr("malformed")
            broken = json.dumps(record)[: random.randint(20, 120)]
            self.rt.emitter.send(journey.correlation_id, broken.encode("utf-8"))
            return span_id

        headers = [("trace_id", journey.trace_id.encode()), ("service", self.name.encode()),
                   ("schema_version", SCHEMA_VERSION.encode())]
        self.rt.emitter.send(journey.correlation_id,
                             json.dumps(record, separators=(",", ":")).encode("utf-8"), headers)
        return span_id

    # -- helpers ---------------------------------------------------------------

    @property
    def scenario(self) -> ScenarioProfile:
        return self.rt.controller.current

    def notify(self, journey: Journey, template: str) -> None:
        self.rt.service("notification-service").submit(journey, "send", {"template": template, "channel": "push"})


# --------------------------------------------------------------------------
# Services
# --------------------------------------------------------------------------


class AuthVault(Service):
    name = "auth-vault"

    def handle_login(self, j: Journey, payload: Dict[str, Any], pod: str) -> None:
        sc = self.scenario
        if j.is_bot:
            # Credential stuffing: many attempts per journey, mostly failing, then rate limited.
            attempts = random.randint(3, 8)
            for attempt in range(attempts):
                if attempt >= 5 or chance(0.15):
                    self.log(j, pod=pod, operation="auth.password_login", method="POST",
                             endpoint="/v1/auth/login", status=429, latency_ms=lognormal_ms(8),
                             message=f"rate limit exceeded for ip={j.client_ip} (velocity rule AV-17)",
                             error={"code": "RATE_LIMITED", "type": "TooManyRequests", "retryable": False},
                             retry_count=attempt)
                    return
                if chance(0.9):
                    self.log(j, pod=pod, operation="auth.password_login", method="POST",
                             endpoint="/v1/auth/login", status=401, latency_ms=lognormal_ms(55),
                             message=f"invalid credentials for user={j.user_id} from ip={j.client_ip}",
                             error={"code": "INVALID_CREDENTIALS", "type": "Unauthorized", "retryable": False},
                             retry_count=attempt)
                else:
                    self.log(j, pod=pod, operation="auth.password_login", method="POST",
                             endpoint="/v1/auth/login", status=200, latency_ms=lognormal_ms(60),
                             message=f"login succeeded for user={j.user_id} - flagged for step-up verification",
                             level="WARN", retry_count=attempt)
                    return
            return

        roll = random.random()
        if roll < 0.002:
            self.log(j, pod=pod, operation="auth.otp_verify", method="POST", endpoint="/v1/auth/otp/verify",
                     status=500, latency_ms=lognormal_ms(400, multiplier=2),
                     message="token signing key fetch from vault failed",
                     error={"code": "KMS_UNAVAILABLE", "type": "InternalServerError", "retryable": True})
            return
        if roll < 0.002 + sc.auth_failure_rate:
            self.log(j, pod=pod, operation="auth.otp_verify", method="POST", endpoint="/v1/auth/otp/verify",
                     status=401, latency_ms=lognormal_ms(40),
                     message=f"OTP mismatch for phone={j.phone}",
                     error={"code": "OTP_INVALID", "type": "Unauthorized", "retryable": True})
            return

        self.log(j, pod=pod, operation="auth.otp_verify", method="POST", endpoint="/v1/auth/otp/verify",
                 status=200, latency_ms=lognormal_ms(45),
                 message=f"session established for user={j.user_id} on {j.client_platform}")
        self.rt.service("menu-catalog").submit(j, "search")


class MenuCatalog(Service):
    name = "menu-catalog"

    def _throttled(self, j: Journey, pod: str, operation: str, endpoint: str) -> bool:
        if chance(self.scenario.menu_throttle_rate):
            self.log(j, pod=pod, operation=operation, method="GET", endpoint=endpoint, status=429,
                     latency_ms=lognormal_ms(6),
                     message="catalogue read quota exceeded - shedding at edge (flash-sale guard)",
                     error={"code": "THROTTLED", "type": "TooManyRequests", "retryable": True})
            return True
        if chance(0.002):
            self.log(j, pod=pod, operation=operation, method="GET", endpoint=endpoint, status=503,
                     latency_ms=lognormal_ms(1200),
                     message="search cluster shard unavailable, partial results suppressed",
                     error={"code": "SEARCH_BACKEND_UNAVAILABLE", "type": "ServiceUnavailable", "retryable": True})
            return True
        return False

    def handle_search(self, j: Journey, payload: Dict[str, Any], pod: str) -> None:
        term = random.choice(SEARCH_TERMS)
        endpoint = "/v2/search"
        if self._throttled(j, pod, "catalog.search", endpoint):
            return
        results = random.randint(0, 60)
        self.log(j, pod=pod, operation="catalog.search", method="GET", endpoint=endpoint, status=200,
                 latency_ms=lognormal_ms(85, multiplier=self.scenario.menu_latency_multiplier),
                 message=f"search q='{term}' zone={j.zone} returned {results} restaurants",
                 extra={"search": {"query": term, "results_count": results}})
        if results == 0 or chance(0.10):
            return  # user bounced
        j.restaurant = random.choice(RESTAURANTS[j.zone])
        self.rt.service("menu-catalog").submit(j, "menu")

    def handle_menu(self, j: Journey, payload: Dict[str, Any], pod: str) -> None:
        r = j.restaurant or random.choice(RESTAURANTS[j.zone])
        endpoint = f"/v2/restaurants/{r['restaurant_id']}/menu"
        if self._throttled(j, pod, "catalog.menu", endpoint):
            return
        self.log(j, pod=pod, operation="catalog.menu", method="GET", endpoint=endpoint, status=200,
                 latency_ms=lognormal_ms(60, multiplier=self.scenario.menu_latency_multiplier),
                 message=f"menu served for {r['restaurant_name']} ({r['cuisine']})",
                 extra={"order": {"restaurant_id": r["restaurant_id"], "restaurant_name": r["restaurant_name"],
                                  "cuisine": r["cuisine"]}})
        if chance(0.12):
            return  # browsed, did not add to cart
        self.rt.service("menu-catalog").submit(j, "checkout")

    def handle_checkout(self, j: Journey, payload: Dict[str, Any], pod: str) -> None:
        r = j.restaurant or random.choice(RESTAURANTS[j.zone])
        j.items_count = random.randint(1, 6)
        j.amount_inr = round(j.items_count * r["avg_item_price"] * random.uniform(0.9, 1.3) + 35, 2)
        order_meta = {"restaurant_id": r["restaurant_id"], "restaurant_name": r["restaurant_name"],
                      "cuisine": r["cuisine"], "items_count": j.items_count, "amount_inr": j.amount_inr}

        if chance(self.scenario.stockout_rate):
            self.log(j, pod=pod, operation="cart.checkout", method="POST", endpoint="/v1/cart/checkout",
                     status=409, latency_ms=lognormal_ms(110, multiplier=self.scenario.menu_latency_multiplier),
                     message=f"item out of stock at {r['restaurant_name']} during checkout",
                     error={"code": "ITEM_OUT_OF_STOCK", "type": "Conflict", "retryable": False},
                     extra={"order": order_meta})
            return
        if chance(0.004):
            self.log(j, pod=pod, operation="cart.checkout", method="POST", endpoint="/v1/cart/checkout",
                     status=422, latency_ms=lognormal_ms(30),
                     message="coupon FLAT50 not applicable for cart value",
                     error={"code": "COUPON_INVALID", "type": "UnprocessableEntity", "retryable": False},
                     extra={"order": order_meta})
            return

        j.order_id = f"ORD-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{short_id(8)}"
        self.log(j, pod=pod, operation="cart.checkout", method="POST", endpoint="/v1/cart/checkout",
                 status=201, latency_ms=lognormal_ms(140, multiplier=self.scenario.menu_latency_multiplier),
                 message=f"order {j.order_id} created: {j.items_count} items, INR {j.amount_inr}",
                 extra={"order": order_meta})
        self.rt.service("payment-gateway").submit(j, "authorize", {"attempt": 1})


class PaymentGateway(Service):
    name = "payment-gateway"
    MAX_UPI_ATTEMPTS = 3

    def _choose_method(self, j: Journey) -> None:
        method = weighted_choice(["UPI", "CARD", "WALLET", "COD"], [0.62, 0.25, 0.08, 0.05])
        j.payment_method = method
        if method == "UPI":
            j.upi_provider = random.choice(UPI_HANDLES)
            j.upi_id = f"{random.choice(FIRST_NAMES)}.{random.choice(LAST_NAMES)}{random.randint(1, 99)}@{j.upi_provider}"
        elif method == "CARD":
            j.card_network, j.card_pan = random.choice(TEST_CARD_PANS)
            j.card_token = f"tok_{uuid.uuid4().hex[:24]}"

    def _payment_fields(self, j: Journey, attempt: int) -> Dict[str, Any]:
        payment: Dict[str, Any] = {"method": j.payment_method, "amount_inr": j.amount_inr,
                                   "currency": "INR", "attempt": attempt}
        if j.payment_method == "UPI":
            payment.update({"upi_id": j.upi_id, "provider": j.upi_provider, "psp": "npci-switch"})
        elif j.payment_method == "CARD":
            payment.update({"card_token": j.card_token, "card_network": j.card_network,
                            "card_last4": (j.card_pan or "0000")[-4:], "provider": "acquirer-hdfc"})
        elif j.payment_method == "WALLET":
            payment.update({"provider": "quickbite-money"})
        else:
            payment.update({"provider": "cash"})
        return {"payment": payment,
                "order": {"amount_inr": j.amount_inr, "items_count": j.items_count,
                          "restaurant_id": (j.restaurant or {}).get("restaurant_id")}}

    def handle_authorize(self, j: Journey, payload: Dict[str, Any], pod: str) -> None:
        attempt = int(payload.get("attempt", 1))
        if attempt == 1:
            self._choose_method(j)
        sc = self.scenario
        extra = self._payment_fields(j, attempt)
        method = j.payment_method

        if method == "UPI":
            endpoint = "/v2/payments/upi/collect"
            if chance(sc.upi_timeout_rate):
                latency = lognormal_ms(9000, sigma=0.15, cap=10000)
                self.log(j, pod=pod, operation="payment.authorize", method="POST", endpoint=endpoint,
                         status=504, latency_ms=latency,
                         message=f"UPI collect to {j.upi_id} timed out waiting for NPCI switch (attempt {attempt})",
                         error={"code": "UPI_TIMEOUT", "type": "GatewayTimeout", "retryable": True},
                         extra=extra, retry_count=attempt - 1)
                if attempt < self.MAX_UPI_ATTEMPTS:
                    backoff = 0.5 * (2 ** (attempt - 1)) + random.uniform(0, 0.3)  # exponential backoff + jitter
                    self.rt.scheduler.call_later(backoff, self._retry, j, attempt + 1)
                else:
                    self._fail(j, pod, extra, "UPI_RETRIES_EXHAUSTED")
                return
            if chance(0.02):
                self.log(j, pod=pod, operation="payment.authorize", method="POST", endpoint=endpoint,
                         status=402, latency_ms=lognormal_ms(4000, sigma=0.6),
                         message=f"UPI collect declined by payer {j.upi_id}",
                         error={"code": "UPI_COLLECT_DECLINED", "type": "PaymentRequired", "retryable": False},
                         extra=extra, retry_count=attempt - 1)
                self.notify(j, "payment_failed")
                return
            if chance(0.005):
                self.log(j, pod=pod, operation="payment.authorize", method="POST", endpoint=endpoint,
                         status=400, latency_ms=lognormal_ms(120),
                         message=f"VPA {j.upi_id} could not be resolved",
                         error={"code": "INVALID_VPA", "type": "BadRequest", "retryable": False},
                         extra=extra, retry_count=attempt - 1)
                self.notify(j, "payment_failed")
                return
            latency = lognormal_ms(1800, sigma=0.5, multiplier=sc.upi_latency_multiplier * 0.5 + 0.5, cap=9500)
            self.log(j, pod=pod, operation="payment.authorize", method="POST", endpoint=endpoint, status=200,
                     latency_ms=latency,
                     message=f"UPI collect approved by {j.upi_id} for INR {j.amount_inr}",
                     extra=extra, retry_count=attempt - 1)
            self._success(j)
            return

        if method == "CARD":
            endpoint = "/v2/payments/card/authorize"
            if chance(0.005):
                # Deliberate PII leak in free text: Logstash must scrub the PAN from `message`.
                self.log(j, pod=pod, operation="payment.debug", method="POST", endpoint=endpoint, status=200,
                         latency_ms=lognormal_ms(5), level="DEBUG",
                         message=f"DEBUG acquirer request pan={j.card_pan} exp=12/29 token={j.card_token}",
                         extra=extra, simulate_work=False)
            if chance(sc.card_decline_rate):
                self.log(j, pod=pod, operation="payment.authorize", method="POST", endpoint=endpoint,
                         status=402, latency_ms=lognormal_ms(900, multiplier=sc.payment_latency_multiplier),
                         message=f"{j.card_network} card ending {(j.card_pan or '')[-4:]} declined by issuer (insufficient funds)",
                         error={"code": "CARD_DECLINED", "type": "PaymentRequired", "retryable": False},
                         extra=extra)
                self.notify(j, "payment_failed")
                return
            if chance(0.003):
                self.log(j, pod=pod, operation="payment.authorize", method="POST", endpoint=endpoint,
                         status=500, latency_ms=lognormal_ms(2500),
                         message="acquirer returned malformed ISO-8583 response",
                         error={"code": "ACQUIRER_ERROR", "type": "InternalServerError", "retryable": True},
                         extra=extra)
                self.notify(j, "payment_failed")
                return
            self.log(j, pod=pod, operation="payment.authorize", method="POST", endpoint=endpoint, status=200,
                     latency_ms=lognormal_ms(1100, multiplier=sc.payment_latency_multiplier),
                     message=f"3DS authorised {j.card_network} token {j.card_token} for INR {j.amount_inr}",
                     extra=extra)
            self._success(j)
            return

        if method == "WALLET":
            self.log(j, pod=pod, operation="payment.authorize", method="POST", endpoint="/v2/payments/wallet/debit",
                     status=200, latency_ms=lognormal_ms(150, multiplier=sc.payment_latency_multiplier),
                     message=f"wallet debited INR {j.amount_inr}", extra=extra)
            self._success(j)
            return

        self.log(j, pod=pod, operation="payment.authorize", method="POST", endpoint="/v2/payments/cod/confirm",
                 status=200, latency_ms=lognormal_ms(25),
                 message=f"cash on delivery confirmed for INR {j.amount_inr}", extra=extra)
        self._success(j)

    def handle_refund(self, j: Journey, payload: Dict[str, Any], pod: str) -> None:
        extra = self._payment_fields(j, 1)
        if j.payment_method == "COD":
            return
        self.log(j, pod=pod, operation="payment.refund", method="POST", endpoint="/v2/payments/refunds",
                 status=202, latency_ms=lognormal_ms(300),
                 message=f"refund of INR {j.amount_inr} initiated for {j.order_id} ({payload.get('reason')})",
                 extra=extra)
        self.notify(j, "refund_initiated")

    def _retry(self, j: Journey, attempt: int) -> None:
        self.submit(j, "authorize", {"attempt": attempt}, timeout=0.05)

    def _fail(self, j: Journey, pod: str, extra: Dict[str, Any], code: str) -> None:
        self.log(j, pod=pod, operation="payment.finalize", method="POST", endpoint="/v2/payments/finalize",
                 status=502, latency_ms=lognormal_ms(20),
                 message=f"payment for {j.order_id} abandoned after {self.MAX_UPI_ATTEMPTS} attempts",
                 error={"code": code, "type": "BadGateway", "retryable": False}, extra=extra)
        self.notify(j, "payment_failed")

    def _success(self, j: Journey) -> None:
        self.rt.service("driver-dispatcher").submit(j, "allocate", {"attempt": 1})


class DriverDispatcher(Service):
    name = "driver-dispatcher"
    MAX_ALLOCATION_ATTEMPTS = 3

    def _unavailable_rate(self, j: Journey) -> float:
        sc = self.scenario
        if sc.affected_zone is None or sc.affected_zone == j.zone:
            return sc.driver_unavailable_rate
        return SCENARIOS["normal"].driver_unavailable_rate

    def _driver_fields(self, j: Journey, state: str, location: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
        d = dict(j.driver or {})
        d["delivery_state"] = state
        if location is not None:
            d["location"] = location
        return {"driver": d}

    def handle_allocate(self, j: Journey, payload: Dict[str, Any], pod: str) -> None:
        attempt = int(payload.get("attempt", 1))
        if chance(self._unavailable_rate(j)):
            self.log(j, pod=pod, operation="dispatch.allocate", method="POST", endpoint="/v1/dispatch/allocate",
                     status=503, latency_ms=lognormal_ms(700, sigma=0.4),
                     message=f"no driver available within 4km of {j.area}, {j.zone} (attempt {attempt})",
                     error={"code": "NO_DRIVER_AVAILABLE", "type": "ServiceUnavailable", "retryable": True},
                     retry_count=attempt - 1)
            if attempt < self.MAX_ALLOCATION_ATTEMPTS:
                self.rt.scheduler.call_later(1.0 * attempt + random.uniform(0, 0.5),
                                             lambda: self.submit(j, "allocate", {"attempt": attempt + 1}, timeout=0.05))
            else:
                self.log(j, pod=pod, operation="dispatch.cancel", method="POST", endpoint="/v1/orders/cancel",
                         status=503, latency_ms=lognormal_ms(40),
                         message=f"order {j.order_id} cancelled - driver pool exhausted in {j.zone}",
                         error={"code": "ORDER_CANCELLED_NO_DRIVER", "type": "ServiceUnavailable", "retryable": False})
                self.notify(j, "order_cancelled")
                self.rt.service("payment-gateway").submit(j, "refund", {"reason": "no_driver"})
            return

        centroid = ZONES[j.zone]["centroid"]
        j.driver = {
            "driver_id": f"DRV{random.randint(10000, 99999)}",
            "vehicle": random.choice(VEHICLES),
            "eta_min": random.randint(18, 45),
            "distance_km": round(random.uniform(0.8, 7.5), 2),
        }
        self.log(j, pod=pod, operation="dispatch.allocate", method="POST", endpoint="/v1/dispatch/allocate",
                 status=200, latency_ms=lognormal_ms(220),
                 message=f"driver {j.driver['driver_id']} assigned to {j.order_id}, ETA {j.driver['eta_min']} min",
                 extra=self._driver_fields(j, "ASSIGNED", jitter_coordinates(*centroid)), retry_count=attempt - 1)
        self.notify(j, "order_confirmed")

        # Compressed real-time delivery lifecycle: pickup, location pings, delivered.
        total = random.uniform(20, 50) * self.rt.args.delivery_scale
        pickup_at = total * 0.3
        self.rt.scheduler.call_later(pickup_at, lambda: self.submit(j, "pickup", timeout=0.05))
        ping_every = max(2.0, total / 10)
        t = pickup_at + ping_every
        while t < total:
            self.rt.scheduler.call_later(t, lambda: self.submit(j, "ping", timeout=0.05))
            t += ping_every
        self.rt.scheduler.call_later(total, lambda: self.submit(j, "delivered", timeout=0.05))

    def handle_pickup(self, j: Journey, payload: Dict[str, Any], pod: str) -> None:
        self.log(j, pod=pod, operation="dispatch.pickup", method="PUT", endpoint=f"/v1/orders/{j.order_id}/status",
                 status=200, latency_ms=lognormal_ms(70),
                 message=f"order {j.order_id} picked up from {(j.restaurant or {}).get('restaurant_name')}",
                 extra=self._driver_fields(j, "PICKED_UP", jitter_coordinates(*ZONES[j.zone]["centroid"])))
        self.notify(j, "order_picked_up")

    def handle_ping(self, j: Journey, payload: Dict[str, Any], pod: str) -> None:
        status, err, msg = 200, None, f"location update for {j.order_id}"
        if chance(0.01):
            status = 408
            err = {"code": "GPS_STALE", "type": "RequestTimeout", "retryable": True}
            msg = f"driver app GPS fix stale >30s for {j.order_id}"
        self.log(j, pod=pod, operation="tracking.location_ping", method="POST",
                 endpoint=f"/v1/orders/{j.order_id}/track", status=status, latency_ms=lognormal_ms(35),
                 message=msg, error=err,
                 extra=self._driver_fields(j, "IN_TRANSIT", jitter_coordinates(*ZONES[j.zone]["centroid"])))

    def handle_delivered(self, j: Journey, payload: Dict[str, Any], pod: str) -> None:
        self.log(j, pod=pod, operation="dispatch.delivered", method="PUT", endpoint=f"/v1/orders/{j.order_id}/status",
                 status=200, latency_ms=lognormal_ms(60),
                 message=f"order {j.order_id} delivered in {int(time.time() - j.started_at)}s (compressed time)",
                 extra=self._driver_fields(j, "DELIVERED", jitter_coordinates(*ZONES[j.zone]["centroid"])))
        self.notify(j, "order_delivered")


class NotificationService(Service):
    name = "notification-service"

    def handle_send(self, j: Journey, payload: Dict[str, Any], pod: str) -> None:
        template = payload.get("template", "generic")
        channel = payload.get("channel", "push")
        provider = {"push": "fcm" if j.client_platform != "ios" else "apns",
                    "sms": "sms-gateway-in", "whatsapp": "wa-business-api"}[channel]
        extra = {"notification": {"channel": channel, "template": template, "provider": provider}}

        failure_rate = self.scenario.push_failure_rate if channel == "push" else 0.005
        if chance(failure_rate):
            self.log(j, pod=pod, operation="notify.send", method="POST", endpoint=f"/v1/notify/{channel}",
                     status=502, latency_ms=lognormal_ms(1500, sigma=0.6),
                     message=f"{provider} rejected {template} for user={j.user_id}: upstream 503",
                     error={"code": "PROVIDER_ERROR", "type": "BadGateway", "retryable": True}, extra=extra)
            if channel == "push":
                self.submit(j, "send", {"template": template, "channel": "sms"}, timeout=0.05)
            return

        target = f"phone={j.phone}" if channel in ("sms", "whatsapp") else f"device of user={j.user_id}"
        self.log(j, pod=pod, operation="notify.send", method="POST", endpoint=f"/v1/notify/{channel}",
                 status=202, latency_ms=lognormal_ms(90),
                 message=f"{template} queued via {provider} to {target}", extra=extra)


# --------------------------------------------------------------------------
# Traffic generation + reporting
# --------------------------------------------------------------------------


def traffic_generator(rt: Runtime, idx: int, generators: int) -> None:
    """Poisson arrival process; rate follows the active scenario plus a gentle wave."""
    auth = rt.service("auth-vault")
    started = time.monotonic()
    while not rt.stop.is_set():
        sc = rt.controller.current
        wave = 1.0 + 0.15 * math.sin((time.monotonic() - started) / 30.0)
        rate = max(0.05, rt.args.rate * sc.traffic_multiplier * wave / generators)
        if rt.stop.wait(random.expovariate(rate)):
            return
        journey = new_journey(sc)
        rt.stats.incr("journeys")
        if not auth.submit(journey, "login", timeout=0.5):
            rt.stats.incr("backpressure_stalls")


def reporter(rt: Runtime, interval: float) -> None:
    last = rt.stats.snapshot()
    last_t = time.monotonic()
    while not rt.stop.wait(interval):
        now = rt.stats.snapshot()
        now_t = time.monotonic()
        dt = max(1e-6, now_t - last_t)
        eps = (now.get("events", 0) - last.get("events", 0)) / dt
        depths = " ".join(f"{name.split('-')[0]}={svc.inbox.qsize()}" for name, svc in rt.services.items())
        LOG.info(
            "phase=%-24s eps=%7.1f events=%d journeys=%d 2xx=%d 4xx=%d 5xx=%d delivered=%d failed=%d "
            "shed=%d buffer_full=%d malformed=%d | queues: %s scheduled=%d",
            rt.controller.current.name, eps, now.get("events", 0), now.get("journeys", 0),
            now.get("status_2xx", 0), now.get("status_4xx", 0), now.get("status_5xx", 0),
            now.get("delivered", 0), now.get("delivery_failed", 0), now.get("load_shed", 0),
            now.get("producer_buffer_full", 0), now.get("malformed", 0), depths, rt.scheduler.pending(),
        )
        last, last_t = now, now_t


# --------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    env = os.environ.get
    p = argparse.ArgumentParser(description="QuickBite multi-service telemetry simulator")
    p.add_argument("--bootstrap", default=env("KAFKA_BOOTSTRAP", "localhost:9092"),
                   help="Kafka bootstrap servers (env KAFKA_BOOTSTRAP)")
    p.add_argument("--topic", default=env("KAFKA_TOPIC", "platform.telemetry.v1"),
                   help="Kafka topic (env KAFKA_TOPIC)")
    p.add_argument("--rate", type=float, default=float(env("JOURNEY_RATE", "8")),
                   help="baseline new user journeys per second (env JOURNEY_RATE)")
    p.add_argument("--generators", type=int, default=int(env("GENERATOR_THREADS", "2")),
                   help="traffic generator threads")
    p.add_argument("--scenario", choices=sorted(SCENARIOS.keys()), default=env("PIN_SCENARIO") or None,
                   help="pin a single scenario instead of cycling (env PIN_SCENARIO)")
    p.add_argument("--phase-scale", type=float, default=float(env("PHASE_SCALE", "1.0")),
                   help="multiply every phase duration (0.5 = cycle twice as fast)")
    p.add_argument("--time-scale", type=float, default=float(env("TIME_SCALE", "0.02")),
                   help="fraction of simulated latency actually slept by workers")
    p.add_argument("--delivery-scale", type=float, default=float(env("DELIVERY_SCALE", "1.0")),
                   help="multiply the compressed 20-50s delivery lifecycle")
    p.add_argument("--malformed-rate", type=float, default=float(env("MALFORMED_RATE", "0.001")),
                   help="probability of emitting a non-JSON poison-pill record")
    p.add_argument("--queue-size", type=int, default=int(env("SERVICE_QUEUE_SIZE", "1000")),
                   help="bounded inbox size per service (backpressure threshold)")
    p.add_argument("--duration", type=float, default=float(env("RUN_SECONDS", "0")),
                   help="stop after N seconds (0 = run forever)")
    p.add_argument("--report-interval", type=float, default=float(env("REPORT_INTERVAL", "5")),
                   help="seconds between console stat lines")
    p.add_argument("--seed", type=int, default=int(env("SEED", "0")) or None, help="random seed")
    p.add_argument("--dry-run", action="store_true", default=env("DRY_RUN", "false").lower() == "true",
                   help="write JSON to stdout instead of Kafka")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)-7s %(threadName)-20s %(message)s")
    if args.seed is not None:
        random.seed(args.seed)

    stop = threading.Event()

    def _shutdown(signum: int, _frame: Any) -> None:
        LOG.warning("signal %s received - shutting down", signum)
        stop.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    stats = Stats()
    if args.dry_run:
        emitter: Emitter = StdoutEmitter(stats)
        LOG.info("dry-run: writing records to stdout")
    else:
        emitter = KafkaEmitter(args.bootstrap, args.topic, stats, stop)
        LOG.info("producing to kafka bootstrap=%s topic=%s", args.bootstrap, args.topic)

    controller = ScenarioController(stop, args.scenario, args.phase_scale)
    scheduler = Scheduler(stop)
    rt = Runtime(args, emitter, stats, controller, scheduler, stop)

    for cls, workers in ((AuthVault, 3), (MenuCatalog, 4), (PaymentGateway, 4),
                         (DriverDispatcher, 3), (NotificationService, 3)):
        svc = cls(rt, workers=workers, queue_size=args.queue_size)
        rt.services[svc.name] = svc

    controller.start()
    scheduler.start()
    for svc in rt.services.values():
        svc.start()

    threads = [threading.Thread(target=traffic_generator, args=(rt, i, args.generators),
                                name=f"traffic-gen-{i}", daemon=True) for i in range(args.generators)]
    threads.append(threading.Thread(target=reporter, args=(rt, args.report_interval), name="reporter", daemon=True))
    for t in threads:
        t.start()

    LOG.info("simulator started: rate=%.1f journeys/s, generators=%d, services=%s",
             args.rate, args.generators, ", ".join(rt.services))

    deadline = time.monotonic() + args.duration if args.duration > 0 else None
    while not stop.is_set():
        if deadline is not None and time.monotonic() >= deadline:
            LOG.info("duration reached - stopping")
            stop.set()
            break
        stop.wait(0.5)

    time.sleep(0.5)  # let in-flight workers finish their current record
    emitter.close()
    final = stats.snapshot()
    LOG.info("final counters: %s", json.dumps(final, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
