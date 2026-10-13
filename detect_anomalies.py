#!/usr/bin/env python3
"""Event-stream anomaly detector for the Engineer 004 fixture.

Runs the same normalise -> validate -> classify pass the proposed stream
consumer would run, and reports each anomaly class with the event_ids it
affects and the pipeline action. Pure stdlib.

    python3 detect_anomalies.py fixtures/event_sample.jsonl
    python3 detect_anomalies.py fixtures/event_sample.jsonl --json output/anomalies.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import urlparse

# Pipeline policy knobs. Values are design assumptions, not measurements.
SKEW_TOLERANCE = timedelta(minutes=5)      # client clock ahead of server beyond this: use received_at
MAX_FUTURE = timedelta(days=1)             # beyond this the client ts is implausible
BOT_BURST_WINDOW_MS = 1000                 # N page views from one anon id inside this window
BOT_BURST_MIN_EVENTS = 3
BOT_REFERRER_HINTS = ("bot", "scanner", "crawler", "spider")

# Legacy SDK payloads we must accept without an SDK change.
TYPE_ALIASES = {"pageview": "page_view", "page-view": "page_view", "formsubmit": "form_submit"}
PROPERTY_ALIASES = {"page_path": "path", "ref": "referrer"}

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"^\+?[\d\s().-]{7,}$")
PII_KEY_HINTS = ("email", "phone", "mobile", "ssn", "address", "dob")
CLIENT_AGGREGATE_RE = re.compile(r"(^count_|_count$|_today$|_total$)")


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def ms(delta: timedelta) -> int:
    return int(delta.total_seconds() * 1000)


def load(path: str):
    """Yield (line_no, raw_line, event_or_None)."""
    with open(path, encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, 1):
            raw = raw.rstrip("\n")
            if not raw.strip():
                continue
            try:
                yield line_no, raw, json.loads(raw)
            except json.JSONDecodeError:
                yield line_no, raw, None


def normalise(event: dict) -> tuple[dict, list[str]]:
    """Map legacy SDK shapes onto the canonical schema. Returns (event, changes)."""
    e = json.loads(json.dumps(event))  # deep copy
    changes = []
    if "ts" not in e and "timestamp" in e:
        e["ts"] = e.pop("timestamp")
        changes.append("timestamp->ts")
    if e.get("type") in TYPE_ALIASES:
        changes.append(f"type {e['type']}->{TYPE_ALIASES[e['type']]}")
        e["type"] = TYPE_ALIASES[e["type"]]
    props = e.get("properties") or {}
    for old, new in PROPERTY_ALIASES.items():
        if old in props and new not in props:
            props[new] = props.pop(old)
            changes.append(f"properties.{old}->{new}")
    e["properties"] = props
    return e, changes


def analyse(path: str) -> dict:
    findings: dict[str, list[dict]] = defaultdict(list)
    events: list[dict] = []
    seen: dict[tuple, dict] = {}
    line_count = 0

    for line_no, raw, event in load(path):
        line_count += 1
        if event is None:
            m = re.search(r'"event_id"\s*:\s*"([^"]+)"', raw)
            findings["MALFORMED_JSON"].append({
                "event_id": m.group(1) if m else None, "line": line_no,
                "action": "Reject from the parse step; write raw bytes to the dead-letter bucket with line and tenant; count per tenant; never silently drop.",
            })
            continue

        e, changes = normalise(event)
        e["_line"] = line_no
        eid, tenant = e.get("event_id"), e.get("tenant_id")

        if changes:
            findings["LEGACY_SCHEMA"].append({
                "event_id": eid, "line": line_no, "changes": changes,
                "action": "Normalise through a versioned adapter at the consumer; keep the original payload in raw storage.",
            })

        if tenant is None:
            findings["MISSING_TENANT"].append({
                "event_id": eid, "line": line_no,
                "action": "Tenant comes from the write key at the collector, not the payload. If the key cannot resolve it, quarantine; never assign a guessed tenant.",
            })

        key = (tenant, eid)
        if key in seen:
            first = seen[key]
            same_payload = {k: v for k, v in first["_raw"].items() if k != "received_at"} == \
                           {k: v for k, v in event.items() if k != "received_at"}
            gap = ms(parse_ts(event.get("received_at")) - parse_ts(first["_raw"].get("received_at")))
            findings["DUPLICATE_EVENT_ID"].append({
                "event_id": eid, "lines": [first["_line"], line_no],
                "identical_except_received_at": same_payload, "redelivery_gap_ms": gap,
                "action": "Dedupe on (tenant_id, event_id): Redis SET NX with 24h TTL in the hot path, ReplacingMergeTree in the store.",
            })
            continue
        e["_raw"] = event
        seen[key] = e
        events.append(e)

        ts, received = parse_ts(e.get("ts")), parse_ts(e.get("received_at"))
        if received is None:
            findings["MISSING_RECEIVED_AT"].append({
                "event_id": eid, "line": line_no,
                "action": "Collector always stamps received_at server-side; a payload without it came through a path that bypassed the stamp.",
            })
        if ts and received and ts > received:
            ahead = ts - received
            cls = "IMPLAUSIBLE_TIMESTAMP" if ahead > MAX_FUTURE else (
                "CLOCK_SKEW_MAJOR" if ahead > SKEW_TOLERANCE else "CLOCK_SKEW_MINOR")
            action = {
                "IMPLAUSIBLE_TIMESTAMP": "Use received_at as event time; keep client ts in client_ts; flag for SDK/device follow-up.",
                "CLOCK_SKEW_MAJOR": "Use received_at as event time (beyond tolerance); keep client_ts.",
                "CLOCK_SKEW_MINOR": "Keep client ts (within tolerance) but record skew; aggregate skew per tenant to spot bad devices.",
            }[cls]
            findings[cls].append({"event_id": eid, "line": line_no,
                                  "client_ahead_by_ms": ms(ahead), "action": action})

        pii = []
        for k, v in (e.get("properties") or {}).items():
            if isinstance(v, str) and (EMAIL_RE.search(v) or (PHONE_RE.match(v) and any(h in k for h in PII_KEY_HINTS))):
                pii.append(k)
            elif any(h in k.lower() for h in PII_KEY_HINTS) and v:
                pii.append(k)
        if pii:
            findings["PII_IN_PROPERTIES"].append({
                "event_id": eid, "line": line_no, "fields": sorted(set(pii)),
                "action": "Redact before any write to storage (hash with a per-tenant salt or drop, per tenant setting); alert the tenant.",
            })

        if e.get("type") == "privacy_request":
            findings["PRIVACY_REQUEST_VIA_SDK"].append({
                "event_id": eid, "line": line_no, "user_id": e.get("user_id"),
                "anonymous_id": e.get("anonymous_id"), "request": (e.get("properties") or {}).get("request"),
                "action": "Not an analytics event. Apply suppression now (reversible); queue deletion for verification by the tenant's authenticated API before executing.",
            })

        for k, v in (e.get("properties") or {}).items():
            if CLIENT_AGGREGATE_RE.search(k) and isinstance(v, (int, float)):
                findings["CLIENT_COMPUTED_AGGREGATE"].append({
                    "event_id": eid, "line": line_no, "field": k, "claimed": v,
                    "action": "Do not segment on client-computed counts; recompute server-side from raw events.",
                })

    # Bot bursts: >= N page views from one (tenant, anon) inside the window, or bot-like referrer.
    by_anon = defaultdict(list)
    for e in events:
        if e.get("type") == "page_view" and parse_ts(e.get("ts")):
            by_anon[(e.get("tenant_id"), e.get("anonymous_id"))].append(e)
    flagged_bot = set()
    for (tenant, anon), evs in by_anon.items():
        evs.sort(key=lambda x: parse_ts(x["ts"]))
        for i in range(len(evs)):
            window = [x for x in evs[i:] if ms(parse_ts(x["ts"]) - parse_ts(evs[i]["ts"])) <= BOT_BURST_WINDOW_MS]
            if len(window) >= BOT_BURST_MIN_EVENTS:
                flagged_bot.update(x["event_id"] for x in window)
        for x in evs:
            host = urlparse((x.get("properties") or {}).get("referrer") or "").hostname or ""
            if any(h in host for h in BOT_REFERRER_HINTS):
                flagged_bot.add(x["event_id"])
    if flagged_bot:
        ids = sorted(flagged_bot)
        span = [parse_ts(seen_e["ts"]) for seen_e in events if seen_e["event_id"] in flagged_bot]
        findings["BOT_TRAFFIC"].append({
            "event_ids": ids, "count": len(ids), "span_ms": ms(max(span) - min(span)),
            "action": "Keep in raw storage with is_bot=true; exclude from default dashboards, segments and personalisation; tenant can override.",
        })

    # Identity stitching: identify events link anonymous_id -> user_id.
    links = {}
    for e in events:
        if e.get("user_id") and e.get("anonymous_id"):
            links.setdefault((e["tenant_id"], e["anonymous_id"]), e["user_id"])
    stitched = []
    for e in events:
        k = (e.get("tenant_id"), e.get("anonymous_id"))
        if not e.get("user_id") and k in links:
            stitched.append({"event_id": e["event_id"], "anonymous_id": e["anonymous_id"], "resolved_user_id": links[k]})
    if stitched:
        findings["RETROACTIVE_IDENTITY_STITCH"].append({
            "events": stitched,
            "action": "Store events against anonymous_id; resolve user_id at query time from the identity table, so stitching never rewrites history.",
        })

    # Recompute any client aggregate claim against raw data.
    for f in findings.get("CLIENT_COMPUTED_AGGREGATE", []):
        src = next(e for e in events if e["event_id"] == f["event_id"])
        user = src.get("user_id")
        anons = {a for (t, a), u in links.items() if u == user and t == src["tenant_id"]}
        day = parse_ts(src["ts"]).date()
        observed = [e["event_id"] for e in events
                    if e.get("tenant_id") == src["tenant_id"] and e.get("type") == "page_view"
                    and (e.get("anonymous_id") in anons or e.get("user_id") == user)
                    and (e.get("properties") or {}).get("path") == "/pricing"
                    and parse_ts(e["ts"]).date() == day and e["event_id"] not in flagged_bot]
        f["server_side_count_in_sample"] = len(observed)
        f["server_side_event_ids"] = observed

    # Deletion scope for privacy requests: every event tied to the user or their anonymous ids.
    for f in findings.get("PRIVACY_REQUEST_VIA_SDK", []):
        anons = {a for (t, a), u in links.items() if u == f["user_id"]} | {f["anonymous_id"]}
        f["deletion_scope_event_ids"] = [e["event_id"] for e in events
                                         if e.get("user_id") == f["user_id"] or e.get("anonymous_id") in anons]

    # Ingest delay for clean events (received_at - ts), the latency baseline in the sample.
    dirty = {f.get("event_id") for cls, fs in findings.items() for f in fs
             if cls.startswith(("CLOCK", "IMPLAUSIBLE"))}
    delays = sorted(ms(parse_ts(e["received_at"]) - parse_ts(e["ts"])) for e in events
                    if parse_ts(e.get("ts")) and parse_ts(e.get("received_at")) and e["event_id"] not in dirty)

    with open(path, "rb") as fh:
        sha = hashlib.sha256(fh.read()).hexdigest()
    return {
        "fixture_sha256": sha, "lines": line_count, "unique_events": len(events),
        "clean_ingest_delay_ms": {"n": len(delays), "min": delays[0], "median": delays[len(delays) // 2], "max": delays[-1]} if delays else None,
        "findings": dict(findings),
    }


def to_markdown(report: dict) -> str:
    out = [f"# Anomaly report", "",
           f"- Fixture sha256: `{report['fixture_sha256']}`",
           f"- Lines: {report['lines']}, unique parsed events: {report['unique_events']}"]
    d = report["clean_ingest_delay_ms"]
    if d:
        out.append(f"- Ingest delay on clean events (received_at - ts): n={d['n']}, min {d['min']} ms, median {d['median']} ms, max {d['max']} ms")
    out.append("")
    for cls, items in report["findings"].items():
        out.append(f"## {cls}")
        for it in items:
            detail = {k: v for k, v in it.items() if k != "action"}
            out.append(f"- `{json.dumps(detail)}`")
            out.append(f"  - Action: {it['action']}")
        out.append("")
    return "\n".join(out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("fixture")
    ap.add_argument("--json")
    ap.add_argument("--md")
    args = ap.parse_args()
    report = analyse(args.fixture)
    if args.json:
        with open(args.json, "w") as fh:
            json.dump(report, fh, indent=2, default=str)
    md = to_markdown(report)
    if args.md:
        with open(args.md, "w") as fh:
            fh.write(md)
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
