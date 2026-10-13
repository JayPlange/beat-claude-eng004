"""Unit tests for the anomaly detector: each class on minimal synthetic input,
plus regression checks pinned to the published fixture.

    python3 -m unittest discover -s tests -v
"""
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from detect_anomalies import analyse  # noqa: E402

FIXTURE = os.path.join(os.path.dirname(__file__), "..", "fixtures", "event_sample.jsonl")


def base(eid, **over):
    e = {"event_id": eid, "tenant_id": "t-1", "anonymous_id": "a-1", "user_id": None,
         "type": "page_view", "ts": "2026-06-15T10:00:00.000Z",
         "received_at": "2026-06-15T10:00:00.150Z", "properties": {"path": "/"}}
    e.update(over)
    return e


def run(lines):
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as fh:
        for line in lines:
            fh.write((line if isinstance(line, str) else json.dumps(line)) + "\n")
    try:
        return analyse(fh.name)["findings"]
    finally:
        os.unlink(fh.name)


class Classes(unittest.TestCase):
    def test_clean_event_has_no_findings(self):
        self.assertEqual(run([base("e1")]), {})

    def test_duplicate_is_dropped_once(self):
        f = run([base("e1"), base("e1", received_at="2026-06-15T10:00:05.000Z")])
        self.assertEqual(f["DUPLICATE_EVENT_ID"][0]["event_id"], "e1")

    def test_same_event_id_in_two_tenants_is_not_duplicate(self):
        f = run([base("e1"), base("e1", tenant_id="t-2")])
        self.assertNotIn("DUPLICATE_EVENT_ID", f)

    def test_skew_tiers(self):
        f = run([base("minor", ts="2026-06-15T10:01:00.150Z"),
                 base("major", ts="2026-06-15T11:00:00.150Z"),
                 base("absurd", ts="2027-06-15T10:00:00.150Z")])
        self.assertEqual(f["CLOCK_SKEW_MINOR"][0]["event_id"], "minor")
        self.assertEqual(f["CLOCK_SKEW_MAJOR"][0]["event_id"], "major")
        self.assertEqual(f["IMPLAUSIBLE_TIMESTAMP"][0]["event_id"], "absurd")

    def test_legacy_payload_is_normalised_not_rejected(self):
        legacy = {"event_id": "e1", "tenant_id": "t-1", "anonymous_id": "a-1", "user_id": None,
                  "type": "pageview", "timestamp": "2026-06-15T10:00:00Z",
                  "properties": {"page_path": "/pricing", "ref": "https://x.com"}}
        f = run([legacy])
        self.assertIn("timestamp->ts", f["LEGACY_SCHEMA"][0]["changes"])
        self.assertIn("MISSING_RECEIVED_AT", f)

    def test_pii_detected_by_value_and_key(self):
        f = run([base("e1", type="custom", properties={"note": "reach me at a.b@c.io", "phone": "+1 555 0100"})])
        self.assertEqual(f["PII_IN_PROPERTIES"][0]["fields"], ["note", "phone"])

    def test_bot_burst(self):
        evs = [base(f"e{i}", ts=f"2026-06-15T10:00:00.{i*10:03d}Z",
                    received_at=f"2026-06-15T10:00:00.{i*10+150:03d}Z") for i in range(3)]
        self.assertEqual(run(evs)["BOT_TRAFFIC"][0]["count"], 3)

    def test_two_page_views_far_apart_are_not_bot(self):
        f = run([base("e1"), base("e2", ts="2026-06-15T10:05:00.000Z", received_at="2026-06-15T10:05:00.200Z")])
        self.assertNotIn("BOT_TRAFFIC", f)

    def test_malformed_line_keeps_event_id(self):
        f = run([json.dumps(base("e9"))[:-1]])
        self.assertEqual(f["MALFORMED_JSON"][0]["event_id"], "e9")

    def test_missing_tenant(self):
        self.assertIn("MISSING_TENANT", run([base("e1", tenant_id=None)]))


class Fixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.r = analyse(FIXTURE)
        cls.f = cls.r["findings"]

    def test_checksum(self):
        self.assertEqual(self.r["fixture_sha256"],
                         "1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235")

    def test_every_planted_class_is_found(self):
        expected = {"DUPLICATE_EVENT_ID", "CLOCK_SKEW_MINOR", "CLOCK_SKEW_MAJOR", "IMPLAUSIBLE_TIMESTAMP",
                    "PII_IN_PROPERTIES", "LEGACY_SCHEMA", "MISSING_RECEIVED_AT", "MISSING_TENANT",
                    "PRIVACY_REQUEST_VIA_SDK", "CLIENT_COMPUTED_AGGREGATE", "MALFORMED_JSON",
                    "BOT_TRAFFIC", "RETROACTIVE_IDENTITY_STITCH"}
        self.assertEqual(set(self.f), expected)

    def test_client_claim_of_three_pricing_views_is_not_supported(self):
        agg = self.f["CLIENT_COMPUTED_AGGREGATE"][0]
        self.assertEqual((agg["claimed"], agg["server_side_count_in_sample"]), (3, 1))

    def test_bot_ids(self):
        self.assertEqual(self.f["BOT_TRAFFIC"][0]["event_ids"], ["evt-0012", "evt-0013", "evt-0014", "evt-0015"])

    def test_deletion_scope_reaches_earlier_anonymous_event(self):
        self.assertIn("evt-0006", self.f["PRIVACY_REQUEST_VIA_SDK"][0]["deletion_scope_event_ids"])


if __name__ == "__main__":
    unittest.main()
