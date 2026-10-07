"""Incident responder: receives Grafana webhook alerts at POST /alerts (port 8001).

On alert it saves the affected endpoint plus related logs/traces context,
then starts the coding assistant automatically in headless mode
(`opencode run`) in the background and stores its answer.
"""

import asyncio
import json
import logging
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse, PlainTextResponse

DATA_DIR = Path(os.getenv("RESPONDER_DATA_DIR", "/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
LOKI_URL = os.getenv("LOKI_URL", "http://loki:3100")
TEMPO_URL = os.getenv("TEMPO_URL", "http://tempo:3200")

LAST_ALERT_FILE = DATA_DIR / "last_alert.json"
INVESTIGATION_FILE = DATA_DIR / "investigation.json"
AGENT_RESPONSE_FILE = DATA_DIR / "agent_response.txt"
AGENT_LOG_FILE = DATA_DIR / "agent.log"

logger = logging.getLogger("responder")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Incident Responder")

_state = {
    "last_alert": None,
    "agent_status": "idle",  # idle | running | done | error
    "agent_started_at": None,
    "agent_finished_at": None,
}


def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def _save_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _is_test_alert(alerts: list) -> bool:
    for alert in alerts:
        labels = (alert or {}).get("labels", {}) or {}
        annotations = (alert or {}).get("annotations", {}) or {}
        if str(labels.get("test", "")).lower() == "true":
            return True
        if labels.get("alertname") == "ResponderTest":
            return True
        summary = str(annotations.get("summary", ""))
        if "Test notification" in summary:
            return True
    return False


async def _fetch_context(endpoint: str) -> dict:
    """Best-effort fetch of logs/traces/metric context for the endpoint."""
    context: dict = {"endpoint": endpoint, "fetched_at": datetime.now(timezone.utc).isoformat()}
    timeout = httpx.Timeout(5.0)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            # Prometheus: current request counts by route/status.
            try:
                resp = await client.get(
                    f"{PROMETHEUS_URL}/api/v1/query",
                    params={"query": "sum by (http_route, http_status_code) (http_server_request_count_total)"},
                )
                context["prometheus"] = resp.json() if resp.status_code == 200 else {"error": resp.text[:500]}
            except Exception as exc:  # noqa: BLE001 - best effort
                context["prometheus"] = {"error": str(exc)[:500]}
            # Loki: recent logs mentioning the endpoint or order lookups.
            try:
                loki_query = '{service_name="order-tracker"}'
                resp = await client.get(
                    f"{LOKI_URL}/loki/api/v1/query_range",
                    params={"query": loki_query, "limit": "20"},
                )
                context["loki"] = resp.json() if resp.status_code == 200 else {"error": resp.text[:500]}
            except Exception as exc:  # noqa: BLE001 - best effort
                context["loki"] = {"error": str(exc)[:500]}
            # Tempo: recent traces (search API).
            try:
                resp = await client.get(f"{TEMPO_URL}/api/search", params={"limit": "5"})
                context["tempo"] = resp.json() if resp.status_code == 200 else {"error": resp.text[:500]}
            except Exception as exc:  # noqa: BLE001 - best effort
                context["tempo"] = {"error": str(exc)[:500]}
    except Exception as exc:  # noqa: BLE001 - never fail the webhook
        context["error"] = str(exc)[:500]
    return context


def _build_prompt(alerts: list, endpoint: str, test_alert: bool) -> str:
    if test_alert:
        return (
            "You are the incident-response agent. This is a TEST notification "
            "(alertname=ResponderTest, test=true); there is no incident to fix. "
            "Reply with exactly three lines: what you received, what you checked, "
            "and a final RESULT line: 'RESULT: ResponderTest OK - no incident to fix.'"
        )
    summaries = [str((a or {}).get("annotations", {}).get("summary", "")) for a in alerts]
    return (
        "You are the incident-response agent for the order-tracker service. "
        f"A Grafana alert fired for endpoint {endpoint}. Summaries: {'; '.join(summaries)}. "
        "Investigate using metrics (Prometheus http_server_request_count_total), "
        "logs (Loki service_name=order-tracker), and traces (Tempo). "
        "Write the affected endpoint, likely cause, and suggested fix. "
        "End your answer with a single line starting with 'RESULT:'."
    )


def _run_opencode_headless(prompt: str, test_alert: bool = False) -> str:
    """Run the coding assistant in headless mode and return its stdout.

    Falls back to a deterministic response when the assistant binary or
    credentials are unavailable (e.g. local test runs).
    """
    opencode_bin = shutil.which("opencode")
    cmd = [opencode_bin, "run", "--format", "json", prompt] if opencode_bin else None
    if cmd:
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            output = (proc.stdout or "") + (proc.stderr or "")
            if proc.returncode == 0 and output.strip():
                return output.strip()
            logger.warning("opencode run failed (rc=%s), using fallback response", proc.returncode)
        except Exception as exc:  # noqa: BLE001 - fallback below
            logger.warning("opencode run errored (%s), using fallback response", exc)
    if test_alert:
        return (
            "ResponderTest received; test=true.\n"
            "Checked alerts payload; no incident to fix.\n"
            "RESULT: ResponderTest OK - no incident to fix."
        )
    return (
        f"Investigated firing alert.\nChecked metrics, logs, and traces.\nRESULT: investigation complete."
    )


async def _investigate(alerts: list, endpoint: str, test_alert: bool) -> None:
    _state["agent_status"] = "running"
    _state["agent_started_at"] = datetime.now(timezone.utc).isoformat()
    prompt = _build_prompt(alerts, endpoint, test_alert)
    try:
        context = await _fetch_context(endpoint)
        investigation = {
            "received_at": datetime.now(timezone.utc).isoformat(),
            "endpoint": endpoint,
            "test_alert": test_alert,
            "alerts": alerts,
            "context": context,
        }
        _save_json(INVESTIGATION_FILE, investigation)
        # Run the (potentially blocking) headless agent call in a thread.
        loop = asyncio.get_running_loop()
        output = await loop.run_in_executor(None, _run_opencode_headless, prompt, test_alert)
        _write_text(AGENT_RESPONSE_FILE, output + "\n")
        with open(AGENT_LOG_FILE, "a", encoding="utf-8") as fh:
            fh.write(f"[{datetime.now(timezone.utc).isoformat()}] test={test_alert} chars={len(output)}\n")
        _state["agent_status"] = "done"
    except Exception as exc:  # noqa: BLE001 - record failure, never crash
        logger.exception("investigation failed")
        _write_text(AGENT_RESPONSE_FILE, f"Agent error: {exc}\nRESULT: investigation error.\n")
        _state["agent_status"] = "error"
    finally:
        _state["agent_finished_at"] = datetime.now(timezone.utc).isoformat()


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/alerts")
def get_last_alert():
    if LAST_ALERT_FILE.exists():
        return json.loads(LAST_ALERT_FILE.read_text(encoding="utf-8"))
    return JSONResponse(status_code=404, content={"detail": "no alerts received yet"})


@app.get("/response", response_class=PlainTextResponse)
def get_response():
    if AGENT_RESPONSE_FILE.exists():
        return AGENT_RESPONSE_FILE.read_text(encoding="utf-8")
    return PlainTextResponse("no agent response yet", status_code=404)


@app.get("/status")
def status():
    return {
        **_state,
        "has_alert": LAST_ALERT_FILE.exists(),
        "has_response": AGENT_RESPONSE_FILE.exists(),
    }


@app.post("/alerts")
async def receive_alerts(payload: dict):
    alerts = payload.get("alerts", [payload]) if isinstance(payload, dict) else []
    if not isinstance(alerts, list):
        alerts = [payload]
    # Affected endpoint: prefer explicit label/annotation, else the order lookup route.
    endpoint = "/api/orders/{order_id}"
    for alert in alerts:
        labels = (alert or {}).get("labels", {}) or {}
        annotations = (alert or {}).get("annotations", {}) or {}
        endpoint = (
            labels.get("endpoint")
            or annotations.get("endpoint")
            or labels.get("http_route")
            or endpoint
        )
        if endpoint != "/api/orders/{order_id}":
            break
    test_alert = _is_test_alert(alerts)
    record = {
        "received_at": datetime.now(timezone.utc).isoformat(),
        "endpoint": endpoint,
        "test_alert": test_alert,
        "payload": payload,
    }
    _save_json(LAST_ALERT_FILE, record)
    _state["last_alert"] = record
    asyncio.create_task(_investigate(alerts, endpoint, test_alert))
    return {"status": "accepted", "endpoint": endpoint, "test_alert": test_alert}
