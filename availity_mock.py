"""
Standalone Availity Coverage API mock for Eligibility load runs.

The eligibility engine (`billing.eligibility.engines.availity-api`) is the
clearinghouse boundary the throughput ADR (`0007-deferred-eligibility-throughput-
optimization`) cares about: every request costs 1 OAuth + 1 POST + 1 GET against
Availity. For a load/capacity run we do NOT want to hammer the real Availity (rate
limits, cost, flakiness, and it would measure *their* throughput, not ours). This
mock stands in for Availity so the run exercises OUR system — the fan-out deferred
pipeline, mapping, completion, webhooks — at full speed.

It mocks BOTH eligibility engines so a load run can exercise whichever channel
production routes a payer to (SOAP is preferred when available, see
ADR-0008 — soap-engine-enabled? is true in prod, so RealTime-capable payers
route to availity-soap, NOT availity-api).

REST engine (`billing.eligibility.engines.availity-api`) — three calls:

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

SOAP engine (`billing.eligibility.engines.availity-soap`) — one call:

  POST {base}                        (Content-Type: text/xml, CAQH CORE envelope)
      body: SOAP <Payload><![CDATA[ X12 270 (1..N transaction sets) ]]></Payload>
      -> 200 SOAP <Payload><![CDATA[ X12 271 (one ST/SE per inbound 270 set) ]]>
      Each 271 echoes the inbound 270 transaction set's BHT03 into its own BHT03 —
      this is how the engine matches a 271 back to its request
      (x12-271/parse-response-x12 keys st->request-id on BHT03). Get this wrong and
      every response fails with "No response matched this request via TRN".
      The 271 body drives the full X12 271 -> FHIR mapping (the heavy CPU path the
      perf run targets). ERROR_PCT swaps in an AAA*N* rejection per set.

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
import re
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
# SOAP engine: optional WSSE check. If set, the CORE envelope's UsernameToken must
# match or the mock returns a SOAP fault (-> engine fails the whole batch).
_SOAP_USERNAME = os.getenv("AVAILITY_MOCK_SOAP_USERNAME", "").strip()
_SOAP_PASSWORD = os.getenv("AVAILITY_MOCK_SOAP_PASSWORD", "").strip()

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


# --------------------------------------------------------------------------- #
# SOAP / X12 270 -> 271  (availity-soap engine).
#
# The SOAP engine posts ONE CAQH CORE envelope carrying an X12 270 (1..N
# transaction sets — deferred batches share an interchange). We parse it, echo
# each set's BHT03, and emit a matching X12 271 wrapped in a CORE response
# envelope. The 271 body below is the proven-good fixture from the billing repo
# (engines/availity_soap/x12_271_test.clj `x12-271-sample`) — it parses through
# parse-271 + assembly into a complete CoverageEligibilityResponse, so the mapper
# does real CPU work. We only substitute element VALUES in place (BHT03, TRN,
# NM1*IL, DMG, ST/SE control) — never add/remove segments — so SE01 stays valid.
# --------------------------------------------------------------------------- #

_PAYLOAD_RE = re.compile(r"<(?:\w+:)?Payload\b[^>]*>(.*?)</(?:\w+:)?Payload>", re.S)
_CDATA_RE = re.compile(r"<!\[CDATA\[(.*?)\]\]>", re.S)
_WSSE_USER_RE = re.compile(r"<(?:\w+:)?Username>(.*?)</(?:\w+:)?Username>", re.S)
_WSSE_PASS_RE = re.compile(r"<(?:\w+:)?Password\b[^>]*>(.*?)</(?:\w+:)?Password>", re.S)


def _first(rx: re.Pattern, s: str) -> str | None:
    m = rx.search(s or "")
    return m.group(1).strip() if m else None

# 271 envelope reused verbatim from the test fixture so it is guaranteed parseable
# (fixed-width ISA, repetition separator '|'). Control numbers are constant — they
# do not affect request<->response matching (that is BHT03 only) and need not be
# unique for parse-271.
_271_ISA = ("ISA*00*          *00*          *ZZ*6175910AAC21T  *ZZ*54503516A      "
            "*061130*1445*|*00501*309242122*0*P*:")
_271_GS = "GS*HB*617591011C21T*545035165*20030924*21000083*309001*X*005010X279A1"
_271_IEA_CTRL = "309242122"
_271_GS_CTRL = "309001"


def _extract_270(soap_body: str) -> str | None:
    """Pull the X12 270 string out of the CORE envelope's <Payload> (CDATA or
    escaped). Returns None if no ISA-bearing payload is present."""
    for payload in _PAYLOAD_RE.findall(soap_body):
        m = _CDATA_RE.search(payload)
        text = (m.group(1) if m else payload).strip()
        if "ISA" in text:
            return text
    return None


def _x12_delims(x12: str) -> tuple[str, str]:
    """Detect (element_sep, segment_terminator) from the 270's ISA. element_sep is
    the 4th char of ISA; the segment terminator is whatever ends ISA right before
    the GS segment. Falls back to the common '*' / '~'."""
    i = x12.find("ISA")
    elem = x12[i + 3] if i != -1 and len(x12) > i + 3 else "*"
    gs = x12.find("GS" + elem)
    seg = x12[gs - 1] if gs > 0 else "~"
    return elem, seg


def _split_270_sets(x12: str) -> list[list[list[str]]]:
    """Split a 270 interchange into transaction sets (ST..SE), each a list of
    segments, each segment a list of elements."""
    elem, seg = _x12_delims(x12)
    segments = [s.strip() for s in x12.split(seg) if s.strip()]
    sets, current = [], None
    for raw in segments:
        els = raw.split(elem)
        tag = els[0]
        if tag == "ST":
            current = [els]
        elif current is not None:
            current.append(els)
            if tag == "SE":
                sets.append(current)
                current = None
    return sets


def _subscriber_from_set(segments: list[list[str]]) -> dict:
    """Extract BHT03 (required for matching) and subscriber NM1*IL / DMG (for
    distinct-looking responses) from one 270 transaction set."""
    out = {"bht03": None, "last": None, "first": None, "member": None, "dob": None}
    for els in segments:
        tag = els[0]
        if tag == "BHT" and len(els) > 3:
            out["bht03"] = els[3]
        elif tag == "NM1" and len(els) > 1 and els[1] == "IL":
            out["last"] = els[3] if len(els) > 3 and els[3] else None
            out["first"] = els[4] if len(els) > 4 and els[4] else None
            out["member"] = els[9] if len(els) > 9 and els[9] else None
        elif tag == "DMG" and len(els) > 2:
            out["dob"] = els[2] or None
    return out


def _build_271_set(sub: dict) -> list[str]:
    """Build one 271 ST/SE transaction set (segment strings) echoing the inbound
    set's BHT03 and subscriber. SE01 is computed from the real segment count."""
    bht = sub["bht03"] or "".join(random.choice("0123456789") for _ in range(8))
    last = sub["last"] or "LASTNAME"
    first = sub["first"] or "FIRSTNAME"
    member = sub["member"] or "11111"
    dob = sub["dob"] or "19991231"
    body = [
        f"BHT*0022*11*{bht}*20030924*21000083",
        "HL*1**20*1",
        "NM1*PR*2*Texas Medicaid/Healthcare Services*****PI*617591011C21P",
        "HL*2*1*21*1",
        "NM1*1P*2*ORGANIZATION NAME*****SV*1111111111",
        "HL*3*2*22*0",
        f"TRN*2*{bht}*9999999999",
        "TRN*1*XXXXXXXXEL.199912310000000*1111111111",
        f"NM1*IL*1*{last}*{first}*M***MI*{member}",
        "REF*SY*111111111",
        "REF*F6*HICN123456",
        "REF*1W*MR789012",
        "N3*100 MAIN STREET",
        "N4*TOWN*TX*12345",
        f"DMG*D8*{dob}",
        "DTP*346*D8*20141201",
        "EB*1*IND*30|98|48|47|33|MH|1|UC|AL|86|50*MC*100 TRADITIONAL MEDICAID",
        "REF*9F*GRP001",
        "DTP*318*D8*20140918",
        "DTP*356*D8*20140901",
        "DTP*357*D8*20150430",
        "EB*A**30|98|48|47|33|MH|1|UC|AL|86|50**100 TRADITIONAL MEDICAID***0",
        "DTP*193*D8*20140901",
        "DTP*194*D8*20150430",
        "EB*B**30|98|48|47|33|MH|1|UC|AL|86|50**100 TRADITIONAL MEDICAID**0",
        "DTP*193*D8*20140901",
        "DTP*194*D8*20150430",
        "EB*C**30**100 TRADITIONAL MEDICAID*23*0",
        "DTP*193*D8*20140901",
        "DTP*194*D8*20150430",
        "EB*C**30**100 TRADITIONAL MEDICAID*29*0",
        "DTP*356*D8*20090101",
        "DTP*357*D8*20090202",
        "EB*I*IND*35|88*MC*100 TRADITIONAL MEDICAID",
        "DTP*193*D8*20140901",
        "DTP*194*D8*20150430",
        "EB*1*IND*30|98|48|47|33|MH|1|UC|AL|86|50*OT*A1HEALTHPLAN NAME",
        "DTP*318*D8*20141007",
        "DTP*356*D8*20141001",
        "DTP*357*D8*20150430",
        "LS*2120",
        "NM1*1P*2*HEALTH PLAN INC*****PI*9876543210",
        "PER*IC**TE*2125551234*FX*2125555678",
        "N3*500 PARK AVE",
        "N4*NEW YORK*NY*10001",
        "LE*2120",
    ]
    segs = [f"ST*271*{bht}*005010X279A1"] + body
    segs.append(f"SE*{len(segs) + 1}*{bht}")
    return segs


def _build_271_rejection_set(sub: dict) -> list[str]:
    """Build a 271 set carrying a subscriber-level AAA*N* rejection (engine ->
    x12-response-rejected -> per-request validation error). BHT03 still echoed."""
    bht = sub["bht03"] or "".join(random.choice("0123456789") for _ in range(8))
    last = sub["last"] or "DOE"
    first = sub["first"] or "JANE"
    member = sub["member"] or "W1"
    dob = sub["dob"] or "19900101"
    segs = [
        f"ST*271*{bht}*005010X279A1",
        f"BHT*0022*11*{bht}*20260325*1113",
        "HL*1**20*1",
        "NM1*PR*2*TEXAS MEDICAID*****46*10186",
        "HL*2*1*21*1",
        "NM1*1P*2*PROVIDER*****XX*1234567890",
        "HL*3*2*22*0",
        f"NM1*IL*1*{last}*{first}****MI*{member}",
        f"DMG*D8*{dob}*F",
        "AAA*N**79*C",
    ]
    segs.append(f"SE*{len(segs) + 1}*{bht}")
    return segs


def _build_271_interchange(set_segment_lists: list[list[str]]) -> str:
    """Wrap N transaction sets in the fixture ISA/GS .. GE/IEA envelope."""
    lines = [_271_ISA, _271_GS]
    for segs in set_segment_lists:
        lines.extend(segs)
    lines.append(f"GE*{len(set_segment_lists)}*{_271_GS_CTRL}")
    lines.append(f"IEA*1*{_271_IEA_CTRL}")
    return "~".join(lines) + "~"


def _soap_271_envelope(x12_271: str) -> str:
    payload_id = str(random.randrange(10 ** 18))
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">'
        "<soapenv:Body>"
        '<ns1:COREEnvelopeRealTimeResponse '
        'xmlns:ns1="http://www.caqh.org/SOAP/WSDL/CORERule2.0.1.xsd">'
        "<PayloadType>X12_271_Response_005010X279A1</PayloadType>"
        "<ProcessingMode>RealTime</ProcessingMode>"
        f"<PayloadID>{payload_id}</PayloadID>"
        f"<Payload><![CDATA[{x12_271}]]></Payload>"
        "</ns1:COREEnvelopeRealTimeResponse>"
        "</soapenv:Body></soapenv:Envelope>"
    )


def _soap_fault_envelope(reason: str) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">'
        "<soapenv:Body><soapenv:Fault>"
        "<faultcode>soapenv:Server</faultcode>"
        f"<faultstring>{reason}</faultstring>"
        "</soapenv:Fault></soapenv:Body></soapenv:Envelope>"
    )


def _build_271_for_270(x12_270: str) -> tuple[str, int, int]:
    """Turn an inbound 270 into a 271 interchange string. Returns
    (x12_271, set_count, rejected_count)."""
    sets = _split_270_sets(x12_270)
    rejected = 0
    out_sets = []
    for s in sets:
        sub = _subscriber_from_set(s)
        if _roll(_ERROR_PCT):
            out_sets.append(_build_271_rejection_set(sub))
            rejected += 1
        else:
            out_sets.append(_build_271_set(sub))
    return _build_271_interchange(out_sets), len(out_sets), rejected


class _Handler(BaseHTTPRequestHandler):
    # ----- response helpers -------------------------------------------------
    def _json(self, code: int, obj) -> None:
        payload = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _xml(self, code: int, body: str) -> None:
        payload = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", "text/xml; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _read_raw(self) -> str:
        length = int(self.headers.get("Content-Length", 0) or 0)
        return self.rfile.read(length).decode("utf-8", "replace") if length else ""

    def _read_form(self) -> dict:
        # Engine sends application/x-www-form-urlencoded for both /token and /coverages.
        return {k: v[-1] for k, v in parse_qs(self._read_raw(), keep_blank_values=True).items()}

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
            # availity-soap posts the CORE envelope to base-url root (no path
            # appended). Route anything XML/SOAP-shaped here regardless of path.
            ctype = (self.headers.get("Content-Type") or "").lower()
            body = self._read_raw()
            if "xml" in ctype or "soap" in ctype or body.lstrip().startswith("<"):
                self._soap(body)
            else:
                self._json(404, {"errors": [{"field": "path",
                                             "errorMessage": "not found"}]})

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

    def _soap(self, body: str):
        """availity-soap: parse the 270 from the CORE envelope, emit a matching
        271 (one set per inbound set, BHT03 echoed). One round trip — no poll."""
        self._bump("soap_request")
        if (_SOAP_USERNAME and _first(_WSSE_USER_RE, body) != _SOAP_USERNAME) or (
            _SOAP_PASSWORD and _first(_WSSE_PASS_RE, body) != _SOAP_PASSWORD
        ):
            self._bump("unauthorized")
            self._xml(500, _soap_fault_envelope("WSSE authentication failed"))
            return
        x12_270 = _extract_270(body)
        if not x12_270:
            self._bump("soap_fault")
            self._xml(500, _soap_fault_envelope("No X12 270 payload found"))
            return
        _sleep_latency()
        x12_271, set_count, rejected = _build_271_for_270(x12_270)
        with _lock:
            _counts["soap_sets"] += set_count
            _counts["soap_rejected"] += rejected
        self._xml(200, _soap_271_envelope(x12_271))

    def _stats(self):
        with _lock:
            stats = {
                "token_requests": _counts["token"],
                "coverages_posts": _counts["coverages_post"],
                "coverage_gets": _counts["coverage_get"],
                "pending_returned": _counts["pending"],
                "errors_returned": _counts["error"],
                "unauthorized": _counts["unauthorized"],
                "soap_requests": _counts["soap_request"],
                "soap_transaction_sets": _counts["soap_sets"],
                "soap_rejected": _counts["soap_rejected"],
                "soap_faults": _counts["soap_fault"],
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
    print(f"[availity-mock] engines: REST (/token,/coverages) + SOAP (X12 270->271 at /)")
    print(f"[availity-mock] point the payer ExchangeProfile credentials.base-url here")
    print(f"[availity-mock] stats at http://{_HOST}:{_PORT}/stats")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[availity-mock] stopping")
        server.shutdown()


if __name__ == "__main__":
    main()
