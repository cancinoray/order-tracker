import logging
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.metrics import set_meter_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, ConsoleLogExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import set_tracer_provider


def setup_opentelemetry():
    resource = Resource.create({"service.name": "order-tracker"})
    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317")

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    try:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        tracer_provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True))
        )
    except Exception as exc:  # collector not available (e.g. unit tests)
        logging.getLogger(__name__).debug("OTLP span exporter disabled: %s", exc)
    try:
        set_tracer_provider(tracer_provider)
    except Exception:  # already set (e.g. tests reloading module)
        pass

    readers = [
        PeriodicExportingMetricReader(
            ConsoleMetricExporter(), export_interval_millis=1000
        )
    ]
    try:
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

        readers.append(
            PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True),
                export_interval_millis=5000,
            )
        )
    except Exception as exc:
        logging.getLogger(__name__).debug("OTLP metric exporter disabled: %s", exc)
    meter_provider = MeterProvider(resource=resource, metric_readers=readers)
    try:
        set_meter_provider(meter_provider)
    except Exception:
        pass

    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(ConsoleLogExporter()))
    try:
        from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

        logger_provider.add_log_record_processor(
            BatchLogRecordProcessor(OTLPLogExporter(endpoint=otlp_endpoint, insecure=True))
        )
    except Exception as exc:
        logging.getLogger(__name__).debug("OTLP log exporter disabled: %s", exc)
    try:
        set_logger_provider(logger_provider)
    except Exception:
        pass

    handler = LoggingHandler(level=logging.INFO, logger_provider=logger_provider)
    root_logger = logging.getLogger()
    if not any(isinstance(h, LoggingHandler) for h in root_logger.handlers):
        root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)


setup_opentelemetry()

tracer = trace.get_tracer(__name__)
meter = metrics.get_meter(__name__)
otel_logger = logging.getLogger("order-tracker")

request_counter = meter.create_counter(
    name="http.server.request.count",
    description="Total HTTP requests",
    unit="{request}",
)
request_duration = meter.create_histogram(
    name="http.server.request.duration",
    description="HTTP request duration",
    unit="s",
)


DB_PATH = Path(os.getenv("ORDER_DB_PATH", "data/orders.db"))
STATUSES = {"received", "preparing", "shipped", "delivered"}


def connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    return db


def init_db():
    with connect() as db:
        db.execute(
            """CREATE TABLE IF NOT EXISTS orders (
                id TEXT PRIMARY KEY,
                customer TEXT NOT NULL,
                item TEXT NOT NULL,
                priority TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            )"""
        )
        if db.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 0:
            now = datetime.now(timezone.utc)
            previous_month_end = now.replace(day=1) - timedelta(days=1)
            for order in (
                ("standard-1001", "Avery", "Notebook", "standard", "received", now),
                ("express-1002", "Sam", "Headphones", "express", "preparing", previous_month_end),
                ("standard-1003", "Riley", "Water bottle", "standard", "shipped", now),
            ):
                db.execute(
                    "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)",
                    (*order[:5], order[5].isoformat()),
                )


def as_dict(row):
    return dict(row) if row else None


def order_detail(row):
    order = as_dict(row)
    if order["priority"] == "express":
        placed_at = datetime.fromisoformat(order["created_at"])
        estimated_at = placed_at + timedelta(days=2)
        order["estimated_delivery"] = estimated_at.date().isoformat()
    return order


class NewOrder(BaseModel):
    customer: str = Field(min_length=1, max_length=80)
    item: str = Field(min_length=1, max_length=120)
    priority: str = "standard"


class StatusUpdate(BaseModel):
    status: str


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Order Tracker", lifespan=lifespan)


def _get_route_template(request: Request) -> str:
    route = request.scope.get("route")
    if route is not None and hasattr(route, "path"):
        return route.path
    return request.url.path


@app.middleware("http")
async def otel_metrics_logs_middleware(request: Request, call_next):
    start = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    except Exception:
        status_code = 500
        raise
    finally:
        duration = time.perf_counter() - start
        route = _get_route_template(request)
        attributes = {
            "http.route": route,
            "http.method": request.method,
            "http.status_code": status_code,
            "route": route,
            "status_code": status_code,
        }
        request_counter.add(1, attributes)
        request_duration.record(duration, attributes)
        otel_logger.info(
            "HTTP %s %s route=%s status=%s duration=%.4fs",
            request.method,
            request.url.path,
            route,
            status_code,
            duration,
            extra={"http.route": route, "http.status_code": status_code},
        )


FastAPIInstrumentor().instrument_app(app)


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent.parent / "static" / "index.html")


@app.get("/healthz")
def health():
    with connect() as db:
        db.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/api/orders")
def list_orders():
    with connect() as db:
        rows = db.execute("SELECT * FROM orders ORDER BY created_at DESC").fetchall()
    return [as_dict(row) for row in rows]


@app.get("/api/orders/{order_id}")
def get_order(order_id: str):
    with tracer.start_as_current_span(
        "order.lookup", attributes={"order.id": order_id, "http.route": "/api/orders/{order_id}"}
    ) as span:
        otel_logger.info(
            "Looking up order order_id=%s route=/api/orders/{order_id}",
            order_id,
            extra={"order.id": order_id, "http.route": "/api/orders/{order_id}"},
        )
        with connect() as db:
            row = db.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        if row is None:
            span.set_attribute("order.found", False)
            span.set_attribute("http.status_code", 404)
            otel_logger.warning(
                "Order not found order_id=%s status=404",
                order_id,
                extra={"order.id": order_id, "http.status_code": 404},
            )
            raise HTTPException(404, "Order not found")
        try:
            order = order_detail(row)
        except Exception as exc:
            span.record_exception(exc)
            span.set_attribute("order.found", True)
            span.set_attribute("http.status_code", 500)
            otel_logger.exception(
                "Order lookup failed order_id=%s status=500",
                order_id,
                extra={"order.id": order_id, "http.status_code": 500},
            )
            raise
        span.set_attribute("order.found", True)
        span.set_attribute("http.status_code", 200)
        otel_logger.info(
            "Order lookup succeeded order_id=%s status=200",
            order_id,
            extra={"order.id": order_id, "http.status_code": 200},
        )
        return order


@app.post("/api/orders", status_code=201)
def create_order(order: NewOrder):
    if order.priority not in {"standard", "express"}:
        raise HTTPException(422, "Priority must be standard or express")
    order_id = str(uuid4())
    with connect() as db:
        db.execute(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)",
            (order_id, order.customer, order.item, order.priority, "received",
             datetime.now(timezone.utc).isoformat()),
        )
    return get_order(order_id)


@app.patch("/api/orders/{order_id}")
def update_status(order_id: str, update: StatusUpdate):
    if update.status not in STATUSES:
        raise HTTPException(422, "Invalid status")
    with connect() as db:
        cursor = db.execute(
            "UPDATE orders SET status = ? WHERE id = ?",
            (update.status, order_id),
        )
    if cursor.rowcount == 0:
        raise HTTPException(404, "Order not found")
    return get_order(order_id)
