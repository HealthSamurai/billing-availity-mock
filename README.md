# billing-availity-mock

Standalone mock of the **Availity Coverage API**, used to stand in for the real
clearinghouse during Eligibility load/capacity runs. It implements exactly the
three calls the `billing.eligibility.engines.availity-api` engine makes
(`POST /token`, `POST /coverages`, `GET /coverages/{id}`) and returns a
mapping-valid coverage body, so the eligibility pipeline (fan-out, mapping,
completion, webhooks) runs for real at full speed without hitting — or measuring —
the real Availity.

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
(no path, no trailing slash — the engine appends `/token`, `/coverages`,
`/coverages/{id}` itself).

## Run locally

```bash
python availity_mock.py            # binds 0.0.0.0:8090
curl -s localhost:8090/            # health
curl -s localhost:8090/stats       # call counters
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

`GET /stats` reports call counts (sanity-check that OAuth caching keeps
`token_requests` ≪ `coverages_posts`) and, with member tracking on and
`PENDING_PCT=0`, validates **≤1 clearinghouse submit per request**.
