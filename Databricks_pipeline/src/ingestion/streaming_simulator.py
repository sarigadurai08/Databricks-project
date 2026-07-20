"""
Streaming event simulator for e-commerce lakehouse landing zones.

Generates realistic JSON event files for every entity and writes them into
``PATHS.landing_path(entity, "json")`` — never into the Git repository.
"""

from __future__ import annotations

import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

from pyspark.sql import SparkSession

from config.config import CONFIG, EcommerceConfig
from config.constants import (
    ALL_ENTITIES,
    CATEGORIES,
    ENTITY_CLICK_LOGS,
    ENTITY_COUPONS,
    ENTITY_DELIVERY,
    ENTITY_INVENTORY,
    ENTITY_ORDERS,
    ENTITY_PAYMENTS,
    ENTITY_PRODUCTS,
    ENTITY_REVIEWS,
    ENTITY_SHOPPING_CART,
    ENTITY_SUPPORT_EVENTS,
    ENTITY_USERS,
    PAYMENT_METHODS,
    REGIONS,
    SIMULATOR_INTERVAL_SECONDS,
    VALID_CART_STATUSES,
    VALID_DELIVERY_STATUSES,
    VALID_ORDER_STATUSES,
    VALID_PAYMENT_STATUSES,
    VALID_SUPPORT_STATUSES,
    WAREHOUSES,
)
from config.paths import PATHS
from src.utilities.exceptions import SimulatorError

try:
    from faker import Faker

    _FAKER: Optional[Any] = Faker()
except ImportError:
    _FAKER = None


def _dbutils(spark: Optional[SparkSession] = None):
    spark = spark or SparkSession.getActiveSession()
    try:
        from pyspark.dbutils import DBUtils  # type: ignore

        if spark is not None:
            return DBUtils(spark)
    except Exception:
        pass
    try:
        import IPython

        return IPython.get_ipython().user_ns.get("dbutils")  # type: ignore[union-attr]
    except Exception:
        return None


def _mkdirs(path: str, spark: Optional[SparkSession] = None) -> None:
    if PATHS.is_cloud_storage or path.startswith("/Volumes/") or path.startswith("dbfs:"):
        try:
            dbutils = _dbutils(spark)
            if dbutils is not None:
                dbutils.fs.mkdirs(path)
                return
        except Exception:
            pass
        try:
            Path(path).mkdir(parents=True, exist_ok=True)
        except OSError:
            pass
        return
    Path(path).mkdir(parents=True, exist_ok=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")


def _rand_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _choice(seq: Sequence[Any]) -> Any:
    return random.choice(list(seq))


def _email() -> str:
    if _FAKER:
        return _FAKER.email()
    return f"user{random.randint(1000, 99999)}@example.com"


def _name() -> tuple[str, str]:
    if _FAKER:
        return _FAKER.first_name(), _FAKER.last_name()
    firsts = ("Alex", "Jordan", "Sam", "Taylor", "Casey", "Riley", "Morgan", "Avery")
    lasts = ("Smith", "Johnson", "Lee", "Patel", "Garcia", "Kim", "Brown", "Davis")
    return _choice(firsts), _choice(lasts)


def _phone() -> str:
    if _FAKER:
        return _FAKER.phone_number()
    return f"+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"


def _product_name(category: str) -> str:
    if _FAKER:
        return f"{_FAKER.word().title()} {category.split()[0]}"
    adjectives = ("Pro", "Ultra", "Smart", "Eco", "Classic", "Premium", "Lite")
    return f"{_choice(adjectives)} {category.split()[0]} {random.randint(100, 999)}"


class StreamingEventSimulator:
    """
    Generate realistic JSON landing files for all e-commerce entities.

    Usage:
        sim = StreamingEventSimulator(spark)
        counts = sim.generate_batch()
        sim.run_ticks(ticks=3)
    """

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        config: Optional[EcommerceConfig] = None,
        interval_seconds: Optional[int] = None,
        events_per_tick: Optional[int] = None,
        ticks: Optional[int] = None,
        entities: Optional[Sequence[str]] = None,
        ndjson: bool = False,
    ) -> None:
        self.spark = spark or SparkSession.getActiveSession()
        self.config = config or CONFIG
        self.interval_seconds = (
            interval_seconds
            if interval_seconds is not None
            else self.config.streaming.simulator_interval_seconds
            or SIMULATOR_INTERVAL_SECONDS
        )
        self.events_per_tick = (
            events_per_tick
            if events_per_tick is not None
            else self.config.streaming.simulator_events_per_tick
        )
        self.ticks = ticks if ticks is not None else self.config.streaming.simulator_ticks
        self.entities = list(entities) if entities is not None else list(ALL_ENTITIES)
        self.ndjson = ndjson

        # Shared ID pools so FK-ish relationships stay coherent within a tick
        self._user_ids: list[str] = []
        self._product_ids: list[str] = []
        self._order_ids: list[str] = []
        self._session_ids: list[str] = []

    def run_ticks(self, ticks: Optional[int] = None) -> dict[str, int]:
        """Run N ticks, sleeping ``interval_seconds`` between ticks (except the last)."""
        n = self.ticks if ticks is None else ticks
        totals: dict[str, int] = {e: 0 for e in self.entities}
        for i in range(n):
            batch = self.generate_batch()
            for entity, count in batch.items():
                totals[entity] = totals.get(entity, 0) + count
            if i < n - 1 and self.interval_seconds > 0:
                time.sleep(self.interval_seconds)
        return totals

    def generate_batch(self) -> dict[str, int]:
        """Generate one tick of events for every configured entity; return counts."""
        counts: dict[str, int] = {}
        for entity in self.entities:
            try:
                events = self._generate_events(entity, self.events_per_tick)
                self._write_events(entity, events)
                counts[entity] = len(events)
            except Exception as exc:
                raise SimulatorError(
                    f"Failed to simulate events for {entity}: {exc}",
                    details={"entity": entity},
                ) from exc
        return counts

    # ------------------------------------------------------------------
    # Generators
    # ------------------------------------------------------------------
    def _generate_events(self, entity: str, n: int) -> list[dict[str, Any]]:
        generators = {
            ENTITY_USERS: self._gen_users,
            ENTITY_PRODUCTS: self._gen_products,
            ENTITY_ORDERS: self._gen_orders,
            ENTITY_PAYMENTS: self._gen_payments,
            ENTITY_REVIEWS: self._gen_reviews,
            ENTITY_SHOPPING_CART: self._gen_shopping_cart,
            ENTITY_CLICK_LOGS: self._gen_click_logs,
            ENTITY_INVENTORY: self._gen_inventory,
            ENTITY_COUPONS: self._gen_coupons,
            ENTITY_DELIVERY: self._gen_delivery,
            ENTITY_SUPPORT_EVENTS: self._gen_support_events,
        }
        if entity not in generators:
            raise SimulatorError(f"Unknown entity: {entity}")
        return generators[entity](n)

    def _user_id(self) -> str:
        if self._user_ids and random.random() < 0.7:
            return _choice(self._user_ids)
        uid = _rand_id("USR")
        self._user_ids.append(uid)
        return uid

    def _product_id(self) -> str:
        if self._product_ids and random.random() < 0.7:
            return _choice(self._product_ids)
        pid = _rand_id("PRD")
        self._product_ids.append(pid)
        return pid

    def _order_id(self) -> str:
        if self._order_ids and random.random() < 0.5:
            return _choice(self._order_ids)
        oid = _rand_id("ORD")
        self._order_ids.append(oid)
        return oid

    def _session_id(self) -> str:
        if self._session_ids and random.random() < 0.6:
            return _choice(self._session_ids)
        sid = _rand_id("SES")
        self._session_ids.append(sid)
        return sid

    def _gen_users(self, n: int) -> list[dict[str, Any]]:
        rows = []
        for _ in range(n):
            first, last = _name()
            uid = _rand_id("USR")
            self._user_ids.append(uid)
            rows.append(
                {
                    "user_id": uid,
                    "email": _email(),
                    "first_name": first,
                    "last_name": last,
                    "phone": _phone(),
                    "region": _choice(REGIONS),
                    "signup_date": (
                        datetime.now(timezone.utc).date() - timedelta(days=random.randint(0, 730))
                    ).isoformat(),
                    "status": _choice(("Active", "Inactive", "Suspended")),
                    "loyalty_tier": _choice(("Bronze", "Silver", "Gold", "Platinum")),
                    "event_time": _now_iso(),
                }
            )
        return rows

    def _gen_products(self, n: int) -> list[dict[str, Any]]:
        rows = []
        for _ in range(n):
            category = _choice(CATEGORIES)
            price = round(random.uniform(5.0, 999.0), 2)
            pid = _rand_id("PRD")
            self._product_ids.append(pid)
            rows.append(
                {
                    "product_id": pid,
                    "product_name": _product_name(category),
                    "category": category,
                    "brand": _choice(("Acme", "Nova", "Zenith", "Orbit", "Pulse", "Harbor")),
                    "price": price,
                    "cost": round(price * random.uniform(0.4, 0.75), 2),
                    "is_active": random.random() > 0.05,
                    "sku": f"SKU-{uuid.uuid4().hex[:8].upper()}",
                    "event_time": _now_iso(),
                }
            )
        return rows

    def _gen_orders(self, n: int) -> list[dict[str, Any]]:
        rows = []
        for _ in range(n):
            oid = _rand_id("ORD")
            self._order_ids.append(oid)
            total = round(random.uniform(15.0, 1500.0), 2)
            discount = round(total * random.uniform(0, 0.25), 2)
            rows.append(
                {
                    "order_id": oid,
                    "user_id": self._user_id(),
                    "order_time": _now_iso(),
                    "status": _choice(list(VALID_ORDER_STATUSES)),
                    "total_amount": total,
                    "discount_amount": discount,
                    "shipping_amount": round(random.uniform(0, 25.0), 2),
                    "shipping_region": _choice(REGIONS),
                    "coupon_code": _choice((None, "SAVE10", "WELCOME20", "FREESHIP", "FLASH15")),
                    "item_count": random.randint(1, 8),
                }
            )
        return rows

    def _gen_payments(self, n: int) -> list[dict[str, Any]]:
        return [
            {
                "payment_id": _rand_id("PAY"),
                "order_id": self._order_id(),
                "user_id": self._user_id(),
                "payment_time": _now_iso(),
                "amount": round(random.uniform(10.0, 1500.0), 2),
                "method": _choice(PAYMENT_METHODS),
                "status": _choice(list(VALID_PAYMENT_STATUSES)),
                "currency": "USD",
            }
            for _ in range(n)
        ]

    def _gen_reviews(self, n: int) -> list[dict[str, Any]]:
        return [
            {
                "review_id": _rand_id("REV"),
                "product_id": self._product_id(),
                "user_id": self._user_id(),
                "order_id": self._order_id(),
                "rating": random.randint(1, 5),
                "review_text": (
                    _FAKER.sentence(nb_words=12)
                    if _FAKER
                    else _choice(
                        (
                            "Great product, fast shipping.",
                            "Okay quality for the price.",
                            "Not as expected.",
                            "Excellent value!",
                            "Would buy again.",
                        )
                    )
                ),
                "review_time": _now_iso(),
                "verified_purchase": random.random() > 0.2,
            }
            for _ in range(n)
        ]

    def _gen_shopping_cart(self, n: int) -> list[dict[str, Any]]:
        return [
            {
                "cart_id": _rand_id("CRT"),
                "user_id": self._user_id(),
                "product_id": self._product_id(),
                "session_id": self._session_id(),
                "quantity": random.randint(1, 5),
                "unit_price": round(random.uniform(5.0, 500.0), 2),
                "status": _choice(list(VALID_CART_STATUSES)),
                "event_time": _now_iso(),
            }
            for _ in range(n)
        ]

    def _gen_click_logs(self, n: int) -> list[dict[str, Any]]:
        event_types = ("page_view", "product_view", "add_to_cart", "search", "checkout_start")
        devices = ("desktop", "mobile", "tablet")
        return [
            {
                "event_id": _rand_id("CLK"),
                "user_id": self._user_id() if random.random() > 0.15 else None,
                "session_id": self._session_id(),
                "product_id": self._product_id() if random.random() > 0.3 else None,
                "page_url": f"/catalog/{_choice(CATEGORIES).lower().replace(' & ', '-').replace(' ', '-')}",
                "event_type": _choice(event_types),
                "device_type": _choice(devices),
                "referrer": _choice(("google", "direct", "email", "social", "affiliate")),
                "event_time": _now_iso(),
            }
            for _ in range(n)
        ]

    def _gen_inventory(self, n: int) -> list[dict[str, Any]]:
        return [
            {
                "product_id": self._product_id(),
                "warehouse_id": _choice(WAREHOUSES),
                "quantity_on_hand": random.randint(0, 5000),
                "quantity_reserved": random.randint(0, 200),
                "reorder_level": random.randint(20, 200),
                "event_time": _now_iso(),
            }
            for _ in range(n)
        ]

    def _gen_coupons(self, n: int) -> list[dict[str, Any]]:
        codes = ("SAVE10", "WELCOME20", "FREESHIP", "FLASH15", "VIP25")
        return [
            {
                "coupon_usage_id": _rand_id("CPU"),
                "coupon_code": _choice(codes),
                "user_id": self._user_id(),
                "order_id": self._order_id(),
                "discount_amount": round(random.uniform(5.0, 75.0), 2),
                "discount_pct": round(random.uniform(5.0, 25.0), 2),
                "redeemed_at": _now_iso(),
            }
            for _ in range(n)
        ]

    def _gen_delivery(self, n: int) -> list[dict[str, Any]]:
        carriers = ("UPS", "FedEx", "USPS", "DHL", "AmazonLogistics")
        return [
            {
                "delivery_id": _rand_id("DLV"),
                "order_id": self._order_id(),
                "carrier": _choice(carriers),
                "tracking_number": f"1Z{uuid.uuid4().hex[:16].upper()}",
                "status": _choice(list(VALID_DELIVERY_STATUSES)),
                "status_time": _now_iso(),
                "region": _choice(REGIONS),
                "estimated_delivery": (
                    datetime.now(timezone.utc).date() + timedelta(days=random.randint(1, 7))
                ).isoformat(),
            }
            for _ in range(n)
        ]

    def _gen_support_events(self, n: int) -> list[dict[str, Any]]:
        channels = ("email", "chat", "phone", "social")
        priorities = ("Low", "Medium", "High", "Urgent")
        subjects = (
            "Order delay",
            "Refund request",
            "Damaged item",
            "Account access",
            "Payment issue",
            "Product question",
        )
        return [
            {
                "ticket_id": _rand_id("TKT"),
                "user_id": self._user_id(),
                "order_id": self._order_id() if random.random() > 0.3 else None,
                "channel": _choice(channels),
                "status": _choice(list(VALID_SUPPORT_STATUSES)),
                "priority": _choice(priorities),
                "subject": _choice(subjects),
                "event_time": _now_iso(),
            }
            for _ in range(n)
        ]

    # ------------------------------------------------------------------
    # Writers
    # ------------------------------------------------------------------
    def _write_events(self, entity: str, events: list[dict[str, Any]]) -> str:
        landing = PATHS.landing_path(entity, "json")
        _mkdirs(landing, self.spark)

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        file_id = uuid.uuid4().hex[:8]
        filename = f"{entity}_{ts}_{file_id}.json"
        target = f"{landing.rstrip('/')}/{filename}"

        if self.ndjson:
            payload = "\n".join(json.dumps(e, default=str) for e in events)
        else:
            payload = json.dumps(events, default=str, indent=2)

        # Prefer dbutils for Volume / DBFS writes
        if PATHS.is_cloud_storage or target.startswith("/Volumes/") or target.startswith("dbfs:"):
            dbutils = _dbutils(self.spark)
            if dbutils is not None:
                try:
                    dbutils.fs.put(target, payload, overwrite=True)
                    return target
                except Exception:
                    pass

        # Local / FUSE-mounted Volume fallback
        path = Path(target)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")
        return target
