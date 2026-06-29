# billing-availity-mock

Standalone mock of the **Availity Coverage API**, used to stand in for the real
clearinghouse during Eligibility load/capacity runs. The eligibility pipeline
(fan-out, mapping, completion, webhooks) runs for real at full speed without
hitting — or measuring — the real Availity.

It mocks **both** eligibility engines, so a load run exercises whichever channel
production routes a payer to:

- **REST** (`billing.eligibility.engines.availity-api`) — three calls:
  `POST /token`, `POST /coverages`, `GET /coverages/{id}` → mapping-valid JSON
  coverage body.
- **SOAP** (`billing.eligibility.engines.availity-soap`) — one CAQH CORE call to
  the base URL: a SOAP envelope carrying an X12 **270** (1..N transaction sets) in,
  a SOAP envelope carrying a matching X12 **271** out. This is the channel
  production prefers (ADR-0008: `soap-engine-enabled?` is true, so RealTime-capable
  payers route to SOAP), and it drives the heavier X12 270→271 mapping path.

> **SOAP matching:** each 271 transaction set echoes the inbound 270 set's `BHT03`
> into its own `BHT03`. That is exactly how the engine matches a 271 back to its
> request (`x12-271/parse-response-x12` keys `st->request-id` on `BHT03`). The 271
> body is the proven-good fixture from the billing repo
> (`engines/availity_soap/x12_271_test.clj`) with only element values substituted
> in place, so it parses cleanly into a `CoverageEligibilityResponse`.

Why it lives in its own repo: the eligibility service runs in the **customer's
cluster**, while this mock runs in **HS tooling infra**. Keeping it standalone
(stdlib-only single file, no test-suite coupling) means the deployed service is
stable and nothing in the e2e/load repo touches it.

## Image

CI (`.github/workflows/build.yml`) builds and pushes to
`ghcr.io/healthsamurai/billing-availity-mock` on every push to `main`.

> After the first publish, set the GHCR package visibility to **public** (or wire
> an imagePullSecret in the cluster) so the GitOps cluster can pull it.

## Deploy

Deployed via Flux from
[`billing-tools-gitops`](https://github.com/HealthSamurai/billing-tools-gitops)
under `gitops/apps/tools/availity-mock/` — a Deployment + ClusterIP Service +
HTTPS Ingress at `https://availity-mock.billing.health-samurai.io`.

Wire the eligibility payer's ExchangeProfile `credentials.base-url` to that URL
(no path, no trailing slash). The REST engine appends `/token`, `/coverages`,
`/coverages/{id}`; the SOAP engine POSTs the CORE envelope to the base URL root.
Both share one service — the same `base-url` works for either engine, so you do
not need to know in advance which channel a payer routes to.

## Run locally

```bash
python availity_mock.py            # binds 0.0.0.0:8090
curl -s localhost:8090/            # health
curl -s localhost:8090/stats       # call counters (REST + SOAP)

# SOAP: POST a CAQH CORE envelope wrapping an X12 270 to the base URL root.
# The mock returns a CORE envelope wrapping a matching X12 271 (BHT03 echoed).
curl -s -X POST localhost:8090/ -H 'Content-Type: text/xml' --data-binary @core-270.xml
```

## Tunables (env)

| Var | Effect |
|---|---|
| `AVAILITY_MOCK_HOST` / `AVAILITY_MOCK_PORT` | bind address (default `0.0.0.0:8090`) |
| `AVAILITY_MOCK_LATENCY_MS` / `_LATENCY_JITTER_MS` | mimic Availity round-trip per call |
| `AVAILITY_MOCK_PENDING_PCT` | % of GETs that return `In Progress` → poll/retry path |
| `AVAILITY_MOCK_ERROR_PCT` | % of submits that fail → failure path |
| `AVAILITY_MOCK_TEMPLATE` | path to a JSON coverage body to return instead of the built-in one |
| `AVAILITY_MOCK_CLIENT_ID` / `_CLIENT_SECRET` | if set, `/token` rejects other credentials (401) |
| `AVAILITY_MOCK_TRACK_MEMBERS` | `1` to count `POST /coverages` per memberId → `/stats` surfaces duplicate clearinghouse submits |
| `AVAILITY_MOCK_SOAP_USERNAME` / `_PASSWORD` | if set, the SOAP WSSE UsernameToken must match or the mock returns a SOAP fault |

`AVAILITY_MOCK_LATENCY_MS`, `_ERROR_PCT` apply to SOAP too (`_ERROR_PCT` swaps in an
`AAA*N*` subscriber rejection per transaction set → engine `x12-response-rejected`
→ per-request validation error). `PENDING_PCT` is REST-only — SOAP is synchronous
real-time (the engine never polls it).

`GET /stats` reports call counts (sanity-check that OAuth caching keeps
`token_requests` ≪ `coverages_posts`) and, with member tracking on and
`PENDING_PCT=0`, validates **≤1 clearinghouse submit per request**.
