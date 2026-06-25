"""
Standalone Availity Coverage API mock for Eligibility load runs.

The eligibility engine (`billing.eligibility.engines.availity-api`) is the
clearinghouse boundary the throughput ADR (`0007-deferred-eligibility-throughput-
optimization`) cares about: every request costs 1 OAuth + 1 POST + 1 GET against
Availity. For a load/capacity run we do NOT want to hammer the real Availity (rate
limits, cost, flakiness, and it would measure *their* throughput, not ours). This
mock stands in for Availity so the run exercises OUR system — the fan-out deferred
pipeline, mapping, completion, webhooks — at full speed.

It implements exactly the three calls the engine makes (see availity_api.clj):

  POST {base}/token
      form: grant_type, client_id, client_secret, scope=hipaa
      -> 200 {"access_token": "...", "expires_in": 3600}

  POST {base}/coverages              (Authorization: Bearer <token>)
      form: payerId, memberId, providerNpi, patientFirstName, serviceType, ...
      -> 200 {"id": "<coverage-id>", "status": "Complete"}

  GET  {base}/coverages/{id}         (Authorization: Bearer <token>)
      -> 200 <full coverage body>            (mapping -> CoverageEligibilityResponse)
      or 200 {"status": "In Progress"}       (engine treats as pending -> poll path)
      or 4xx / {"errors": [...]}              (engine treats as failure)

Pointing the service at this mock is a config change on the payer's ExchangeProfile
(`credentials.base-url`), NOT something the load client sends per request — the mock
must be reachable from where the eligibility service runs (the customer cluster),
which is why it is deployed as a standalone service in HS tooling infra behind a
public HTTPS endpoint. See README.md.

Stdlib only — no extra dependency — so it runs anywhere: a container (the deployed
form), locally + ngrok, or a port-forwarded pod.

Run:  python availity_mock.py                      (binds 0.0.0.0:8090 by default)
Stats: GET http://<host>:8090/stats               (call counts, pending/error, dupes)

Tunables (env):
  AVAILITY_MOCK_HOST            bind host                         (default 0.0.0.0)
  AVAILITY_MOCK_PORT            bind port                         (default 8090)
  AVAILITY_MOCK_LATENCY_MS      added delay per upstream call     (default 0)
  AVAILITY_MOCK_LATENCY_JITTER_MS  +/- uniform jitter on latency  (default 0)
  AVAILITY_MOCK_PENDING_PCT     % of GET /coverages/{id} that return "In Progress"
                                (exercise the 202/poll-retry path) (default 0)
  AVAILITY_MOCK_ERROR_PCT       % of submits that return an Availity error (default 0)
  AVAILITY_MOCK_TEMPLATE        path to a JSON coverage body to return instead of the
                                built-in one (e.g. the billing repo's
                                fixtures/availity-response-sample.json)
  AVAILITY_MOCK_CLIENT_ID       if set, /token rejects other client_ids (401)
  AVAILITY_MOCK_CLIENT_SECRET   if set, /token rejects other client_secrets (401)
  AVAILITY_MOCK_TRACK_MEMBERS   "1" to count POST /coverages per memberId so duplicate
                                clearinghouse submits are visible at /stats. Off by
                                default — at 1M distinct members the map costs memory.
                                With PENDING_PCT=0 each request hits POST exactly once,
                                so duplicates>1 here = a real "≤1 submit per request"
                                violation. With pending>0, re-polls re-submit by design
                                (availity-api poll == submit), so >1 is expected.
"""

import json
import os
import random
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

_HOST = os.getenv("AVAILITY_MOCK_HOST", "0.0.0.0")
_PORT = int(os.getenv("AVAILITY_MOCK_PORT", "8090"))
_LATENCY_MS = float(os.getenv("AVAILITY_MOCK_LATENCY_MS", "0"))
_LATENCY_JITTER_MS = float(os.getenv("AVAILITY_MOCK_LATENCY_JITTER_MS", "0"))
_PENDING_PCT = float(os.getenv("AVAILITY_MOCK_PENDING_PCT", "0"))
_ERROR_PCT = float(os.getenv("AVAILITY_MOCK_ERROR_PCT", "0"))
_TEMPLATE_PATH = os.getenv("AVAILITY_MOCK_TEMPLATE", "").strip()
_CLIENT_ID = os.getenv("AVAILITY_MOCK_CLIENT_ID", "").strip()
_CLIENT_SECRET = os.getenv("AVAILITY_MOCK_CLIENT_SECRET", "").strip()
_TRACK_MEMBERS = os.getenv("AVAILITY_MOCK_TRACK_MEMBERS", "0") == "1"

# --------------------------------------------------------------------------- #
# Coverage body template.
#
# A trimmed-but-representative Availity coverage response that drives every major
# branch of the mapping layer (billing.eligibility.engines.availity-api.mapping):
# patient/subscriber/payer/provider, a plan with class + dates, and benefits that
# cover statusDetails, amounts (USD + Percent + deductible total), limitations,
# nonCovered (excluded), and contact-only items. Faithful to the real
# fixtures/availity-response-sample.json so the mapper does real CPU work and never
# errors. Point AVAILITY_MOCK_TEMPLATE at that fixture for the full 16-benefit body.
# --------------------------------------------------------------------------- #
_BUILTIN_TEMPLATE = {
    "id": "MOCK-COVERAGE",
    "status": "Complete",
    "statusCode": "4",
    "controlNumber": "11112222333",
    "customerId": "111222",
    "asOfDate": "2025-12-19T05:00:00.000+0000",
    "updatedDate": "2025-12-19T11:30:03.000+0000",
    "createdDate": "2025-12-19T11:30:03.000+0000",
    "patient": {
        "firstName": "SPONGEBOB",
        "middleName": "S",
        "lastName": "SQUAREPANTS",
        "birthDate": "1999-05-01T05:00:00.000+0000",
        "genderCode": "M",
        "gender": "Male",
        "subscriberRelationship": "Self",
        "subscriberRelationshipCode": "18",
        "updateYourRecords": False,
        "address": {
            "line1": "124 CONCH STREET",
            "line2": "APT 2B",
            "city": "BIKINI BOTTOM",
            "state": "Pacific",
            "stateCode": "PO",
            "zipCode": "111122223",
        },
    },
    "subscriber": {
        "firstName": "SPONGEBOB",
        "lastName": "SQUAREPANTS",
        "memberId": "111222333",
        "priorIdentificationNumber": "PRIOR123456",
        "genderCode": "M",
        "birthDate": "1999-05-01T05:00:00.000+0000",
    },
    "payer": {
        "name": "OCEAN MEDICAID",
        "payerId": "11111",
        "responseName": "Pacific Ocean Healthcare Services",
        "responsePayerId": "111122223333P",
    },
    "requestingProvider": {"lastName": "KRUSTY KRAB CLINIC", "npi": "1111222233"},
    "supplementalInformation": {"eligibleForCOB": True},
    "plans": [
        {
            "groupNumber": "015",
            "planNumber": "MLTSSW",
            "planName": "MLTSSW MANAGED CARE",
            "insuranceType": "Health Maintenance Organization (HMO) - Medicare Risk",
            "insuranceTypeCode": "HN",
            "status": "Active Coverage",
            "statusCode": "1",
            "coverageStartDate": "2025-01-01T05:00:00.000+0000",
            "coverageEndDate": "2099-12-31T05:00:00.000+0000",
            "eligibilityStartDate": "2023-07-01T00:00:00.000+0000",
            "benefits": [
                {
                    "name": "Health Benefit Plan Coverage",
                    "type": "30",
                    "status": "Active Coverage",
                    "statusCode": "1",
                    "statusDetails": {
                        "noNetwork": [
                            {
                                "description": "TEST MEDICAID PROGRAM ABC",
                                "insuranceType": "Medicaid",
                                "insuranceTypeCode": "MC",
                                "status": "Active Coverage",
                                "statusCode": "1",
                                "level": "Individual",
                                "levelCode": "IND",
                                "addedDate": "2025-07-02T04:00:00.000+0000",
                                "eligibilityStartDate": "2025-08-01T04:00:00.000+0000",
                                "eligibilityEndDate": "2025-12-31T05:00:00.000+0000",
                                "benefitBeginDate": "2018-01-01T05:00:00.000+0000",
                                "benefitEndDate": "2099-12-31T05:00:00.000+0000",
                                "payerNotes": ["24 QUALIFIED MEDICARE BENEFICIARY (QMB)"],
                            }
                        ]
                    },
                    "amounts": {
                        "coPayment": {
                            "noNetwork": [
                                {
                                    "amount": "0",
                                    "units": "USD",
                                    "description": "MLTSSW",
                                    "periodStartDate": "2025-08-01T04:00:00.000+0000",
                                    "periodEndDate": "2025-12-31T05:00:00.000+0000",
                                    "payerNotes": ["LTSS - HOME HEALTH - PA REQ"],
                                }
                            ]
                        },
                        "coInsurance": {
                            "noNetwork": [{"amount": "0", "units": "Percent"}]
                        },
                        "deductibles": {
                            "noNetwork": [
                                {
                                    "amount": "0",
                                    "total": "0",
                                    "remaining": "0",
                                    "units": "USD",
                                    "insuranceTypeCode": "MB",
                                    "insuranceType": "Medicare Part B",
                                    "levelCode": "IND",
                                    "level": "Individual",
                                }
                            ]
                        },
                    },
                },
                {
                    "name": "Dental Care",
                    "type": "35",
                    "nonCovered": {
                        "noNetwork": [
                            {
                                "insuranceType": "Medicaid",
                                "insuranceTypeCode": "MC",
                                "level": "Individual",
                                "levelCode": "IND",
                                "description": "TEST MEDICAID PROGRAM ABC",
                                "contacts": [
                                    {
                                        "type": "Payer",
                                        "typeCode": "PR",
                                        "name": "TEST PAYER INC",
                                        "memberId": "MBR-999888",
                                        "network": "out",
                                    }
                                ],
                            }
                        ]
                    },
                    "limitations": {
                        "noNetwork": [
                            {"level": "Individual", "levelCode": "IND",
                             "lastVisitDate": "2001-01-17T05:00:00.000+0000"}
                        ]
                    },
                },
                {
                    "name": "Pharmacy Vendor",
                    "type": "89",
                    "contacts": [
                        {
                            "type": "Vendor",
                            "typeCode": "VN",
                            "name": "TEST PHARMACY RX",
                            "contactInformation": [{"url": "WWW.TESTPHARMACY.COM"}],
                        }
                    ],
                },
            ],
        }
    ],
}


def _load_template() -> dict:
    if _TEMPLATE_PATH:
        with open(_TEMPLATE_PATH, "r") as fh:
            return json.load(fh)
    return _BUILTIN_TEMPLATE


_TEMPLATE = _load_template()

# --------------------------------------------------------------------------- #
# Counters (aggregate, always on) + optional per-member tracking.
# --------------------------------------------------------------------------- #
_lock = threading.Lock()
_counts = Counter()           # token / coverages_post / coverage_get / pending / error / unauthorized
_member_submits = Counter()   # memberId -> POST /coverages count (only if _TRACK_MEMBERS)


def _sleep_latency() -> None:
    """Mimic Availity round-trip so per-request cost resembles production."""
    if _LATENCY_MS <= 0 and _LATENCY_JITTER_MS <= 0:
        return
    delay = _LATENCY_MS
    if _LATENCY_JITTER_MS > 0:
        delay += random.uniform(-_LATENCY_JITTER_MS, _LATENCY_JITTER_MS)
    if delay > 0:
        time.sleep(delay / 1000.0)


def _roll(pct: float) -> bool:
    return pct > 0 and random.uniform(0, 100) < pct


def _coverage_body(member_id: str | None) -> dict:
    """Return the template body, stamping the per-request id/memberId so responses
    look distinct and the contained Coverage carries the submitted member id."""
    body = json.loads(json.dumps(_TEMPLATE))  # cheap deep copy; isolates per-request edits
    cov_id = f"MOCK-{member_id or random.randrange(10**12)}-{random.randrange(10**6)}"
    body["id"] = cov_id
    if member_id:
        body.setdefault("subscriber", {})["memberId"] = member_id
    return body


class _Handler(BaseHTTPRequestHandler):
    # ----- response helpers -------------------------------------------------
    def _json(self, code: int, obj) -> None:
        payload = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _read_form(self) -> dict:
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length).decode() if length else ""
        # Engine sends application/x-www-form-urlencoded for both /token and /coverages.
        return {k: v[-1] for k, v in parse_qs(raw, keep_blank_values=True).items()}

    def _bump(self, key: str) -> None:
        with _lock:
            _counts[key] += 1

    # ----- routing ----------------------------------------------------------
    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/stats":
            self._stats()
            return
        if path.startswith("/coverages/"):
            self._get_coverage(path.rsplit("/", 1)[-1])
            return
        self._json(200, {"status": "alive"})

    def do_POST(self):
        path = urlsplit(self.path).path
        if path == "/token":
            self._token()
        elif path == "/coverages":
            self._post_coverages()
        else:
            self._json(404, {"errors": [{"field": "path", "errorMessage": "not found"}]})

    # ----- endpoints --------------------------------------------------------
    def _token(self):
        self._bump("token")
        form = self._read_form()
        _sleep_latency()
        if (_CLIENT_ID and form.get("client_id") != _CLIENT_ID) or (
            _CLIENT_SECRET and form.get("client_secret") != _CLIENT_SECRET
        ):
            self._bump("unauthorized")
            self._json(401, {"error": "invalid_client"})
            return
        self._json(200, {"access_token": "mock-token", "token_type": "Bearer",
                         "expires_in": 3600, "scope": form.get("scope", "hipaa")})

    def _post_coverages(self):
        self._bump("coverages_post")
        form = self._read_form()
        member_id = form.get("memberId")
        if _TRACK_MEMBERS and member_id:
            with _lock:
                _member_submits[member_id] += 1
        _sleep_latency()
        if _roll(_ERROR_PCT):
            self._bump("error")
            self._json(400, {"errors": [
                {"field": "memberId",
                 "errorMessage": "Invalid/Missing Subscriber/Insured ID"}]})
            return
        # Availity returns the created coverage; engine only reads :id from this body.
        self._json(200, {"id": f"MOCK-{member_id or random.randrange(10**12)}"
                                f"-{random.randrange(10**6)}",
                         "status": "Complete"})

    def _get_coverage(self, coverage_id: str):
        self._bump("coverage_get")
        _sleep_latency()
        if _roll(_PENDING_PCT):
            self._bump("pending")
            # engine pending set: {"In Progress", "Communication Error, Retrying"}
            self._json(200, {"id": coverage_id, "status": "In Progress"})
            return
        # member id is embedded in the coverage id by _post_coverages (MOCK-<member>-<n>)
        member_id = coverage_id.split("-")[1] if coverage_id.startswith("MOCK-") else None
        self._json(200, self._coverage_body_for(coverage_id, member_id))

    def _coverage_body_for(self, coverage_id: str, member_id: str | None) -> dict:
        body = _coverage_body(member_id)
        body["id"] = coverage_id
        return body

    def _stats(self):
        with _lock:
            stats = {
                "token_requests": _counts["token"],
                "coverages_posts": _counts["coverages_post"],
                "coverage_gets": _counts["coverage_get"],
                "pending_returned": _counts["pending"],
                "errors_returned": _counts["error"],
                "unauthorized": _counts["unauthorized"],
            }
            if _TRACK_MEMBERS:
                dupes = {m: c for m, c in _member_submits.items() if c > 1}
                stats["distinct_members"] = len(_member_submits)
                stats["duplicate_member_submits"] = dupes
        self._json(200, stats)

    def log_message(self, *_args):  # silence per-request stderr noise
        pass


def main():
    server = ThreadingHTTPServer((_HOST, _PORT), _Handler)
    src = _TEMPLATE_PATH or "<built-in coverage body>"
    print(f"[availity-mock] listening on http://{_HOST}:{_PORT}  (body: {src})")
    print(f"[availity-mock] latency={_LATENCY_MS}±{_LATENCY_JITTER_MS}ms "
          f"pending={_PENDING_PCT}% error={_ERROR_PCT}% "
          f"track_members={_TRACK_MEMBERS}")
    print(f"[availity-mock] point the payer ExchangeProfile credentials.base-url here")
    print(f"[availity-mock] stats at http://{_HOST}:{_PORT}/stats")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[availity-mock] stopping")
        server.shutdown()


if __name__ == "__main__":
    main()
