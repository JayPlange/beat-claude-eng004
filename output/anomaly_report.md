# Anomaly report

- Fixture sha256: `1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235`
- Lines: 25, unique parsed events: 23
- Ingest delay on clean events (received_at - ts): n=19, min 119 ms, median 163 ms, max 206 ms

## DUPLICATE_EVENT_ID
- `{"event_id": "evt-0002", "lines": [2, 5], "identical_except_received_at": true, "redelivery_gap_ms": 7428}`
  - Action: Dedupe on (tenant_id, event_id): Redis SET NX with 24h TTL in the hot path, ReplacingMergeTree in the store.

## CLOCK_SKEW_MINOR
- `{"event_id": "evt-0005", "line": 6, "client_ahead_by_ms": 47190}`
  - Action: Keep client ts (within tolerance) but record skew; aggregate skew per tenant to spot bad devices.

## CLOCK_SKEW_MAJOR
- `{"event_id": "evt-0006", "line": 7, "client_ahead_by_ms": 3900000}`
  - Action: Use received_at as event time (beyond tolerance); keep client_ts.

## PII_IN_PROPERTIES
- `{"event_id": "evt-0007", "line": 8, "fields": ["contact_email", "phone"]}`
  - Action: Redact before any write to storage (hash with a per-tenant salt or drop, per tenant setting); alert the tenant.

## LEGACY_SCHEMA
- `{"event_id": "evt-0009", "line": 10, "changes": ["timestamp->ts", "type pageview->page_view", "properties.page_path->path", "properties.ref->referrer"]}`
  - Action: Normalise through a versioned adapter at the consumer; keep the original payload in raw storage.

## MISSING_RECEIVED_AT
- `{"event_id": "evt-0009", "line": 10}`
  - Action: Collector always stamps received_at server-side; a payload without it came through a path that bypassed the stamp.

## MISSING_TENANT
- `{"event_id": "evt-0011", "line": 12}`
  - Action: Tenant comes from the write key at the collector, not the payload. If the key cannot resolve it, quarantine; never assign a guessed tenant.

## IMPLAUSIBLE_TIMESTAMP
- `{"event_id": "evt-0016", "line": 17, "client_ahead_by_ms": 31535999788}`
  - Action: Use received_at as event time; keep client ts in client_ts; flag for SDK/device follow-up.

## PRIVACY_REQUEST_VIA_SDK
- `{"event_id": "evt-0017", "line": 18, "user_id": "u-1077", "anonymous_id": "anon-77a", "request": "delete_all_data", "deletion_scope_event_ids": ["evt-0006", "evt-0017"]}`
  - Action: Not an analytics event. Apply suppression now (reversible); queue deletion for verification by the tenant's authenticated API before executing.

## CLIENT_COMPUTED_AGGREGATE
- `{"event_id": "evt-0019", "line": 20, "field": "count_today", "claimed": 3, "server_side_count_in_sample": 1, "server_side_event_ids": ["evt-0001"]}`
  - Action: Do not segment on client-computed counts; recompute server-side from raw events.

## MALFORMED_JSON
- `{"event_id": "evt-0020", "line": 21}`
  - Action: Reject from the parse step; write raw bytes to the dead-letter bucket with line and tenant; count per tenant; never silently drop.

## BOT_TRAFFIC
- `{"event_ids": ["evt-0012", "evt-0013", "evt-0014", "evt-0015"], "count": 4, "span_ms": 50}`
  - Action: Keep in raw storage with is_bot=true; exclude from default dashboards, segments and personalisation; tenant can override.

## RETROACTIVE_IDENTITY_STITCH
- `{"events": [{"event_id": "evt-0001", "anonymous_id": "anon-9f2", "resolved_user_id": "u-5511"}, {"event_id": "evt-0002", "anonymous_id": "anon-9f2", "resolved_user_id": "u-5511"}, {"event_id": "evt-0004", "anonymous_id": "anon-c81", "resolved_user_id": "u-2209"}, {"event_id": "evt-0005", "anonymous_id": "anon-c81", "resolved_user_id": "u-2209"}, {"event_id": "evt-0006", "anonymous_id": "anon-77a", "resolved_user_id": "u-1077"}, {"event_id": "evt-0007", "anonymous_id": "anon-3d0", "resolved_user_id": "u-7304"}]}`
  - Action: Store events against anonymous_id; resolve user_id at query time from the identity table, so stitching never rewrites history.
