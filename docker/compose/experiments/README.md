# Experimental Docker Compose Configurations

The root [docker-compose.yml](../../../docker-compose.yml) is the operational
reference. These isolated configurations support validation, comparisons and
optional traces. `start.sh` does not launch them automatically; integration
runners select them explicitly.

| File | Purpose |
|---|---|
| `docker-compose.iforest-treeshap.yml` | iForest TreeSHAP qualification campaigns |
| `docker-compose.v1v2.yml` | Explainability V1/V2 parity campaigns |
| `docker-compose.q3-trace.yml` | Optional partition-trace overlay for V1/V2 campaigns |
| `docker-compose.oc12.yml` | Explainability V2 campaign validation |
| `docker-compose.oc13.yml` | Controlled V1/V2 comparison |
| `docker-compose.oc14.yml` | SHAP capacity benchmarks |
| `docker-compose.oc15.yml` | Minimal validation and seed-stability campaigns |

These files describe test configurations. Their existence does not establish
that a particular campaign has been run on the current revision.
