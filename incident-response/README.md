# Incident responder

Receives Grafana webhook alerts at `POST /alerts` (port 8001).

On each alert it saves:

- the raw alert payload → `/data/last_alert.json`
- the affected endpoint plus related Prometheus/Loki/Tempo context → `/data/investigation.json`

and starts the coding assistant automatically in headless mode
(`opencode run ...`). The assistant's answer is stored in
`/data/agent_response.txt` (also served at `GET /response`).

## Run locally

```bash
uvicorn main:app --host 0.0.0.0 --port 8001
```

## Test

```bash
curl -X POST http://localhost:8001/alerts \
  -H 'Content-Type: application/json' \
  -d '{"alerts":[{"status":"firing","labels":{"alertname":"ResponderTest","test":"true"},"annotations":{"summary":"Test notification; no incident to fix"}}]}'
curl http://localhost:8001/response
```
