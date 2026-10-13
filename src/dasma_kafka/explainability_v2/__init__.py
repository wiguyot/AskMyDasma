"""Subpackage for the explainability-v2 service.

This is a clean, isolated second explainability model. It is a fine copy of the
`explainability` (regular) baseline, kept separate so that regular behavior is
never silently changed. v2 owns its own Kafka topics (`explainability-v2` /
`explainability-v2-viz`), consumer group (`explainability-v2-consumer`), service,
and dashboard. See `docs/planning/explainability-v2.md` for the execution plan.
"""
