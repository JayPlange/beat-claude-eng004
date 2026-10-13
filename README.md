# Beat Claude, Engineer 004 (brief version 2026-07)

Submission by Edwin Plange for Single Grain's real-time analytics pipeline brief.

- `SUBMISSION.md`: the written answer and full submission packet.
- `detect_anomalies.py`: the stream consumer's validation pass, run against the fixture.
- `tests/test_detect.py`: 15 tests, one per anomaly class plus fixture regression checks.
- `sizing.py`: capacity and cost model; every input labelled Observed, Estimated, Benchmarked or Assumed.
- `architecture.mmd`: system diagram (renders on GitHub).
- `fixtures/event_sample.jsonl`: the published fixture, sha256 `1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235`.
- `output/`: generated report, test log and sizing tables.

Standard library only. Reproduce:

```
python3 detect_anomalies.py fixtures/event_sample.jsonl
python3 -m unittest discover -s tests -v
python3 sizing.py
python3 sizing.py --spike 20 --kb 2
```
