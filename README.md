# Claude plan
We have an application described in @../holmes-2/ . It consists of a python API in @../holmes/components/ and @../holmes/bases/ . The react typescript frontend is in @../holmes-2/frontend/ . The database migrations are in @../holmes-2/alembic/ . The database is available with this port forward:
```
kubectl port-forward services/pgstac 5433:5432 --namespace hasura --context c-core
```
The username is `username`. The password is `password`.

Workflow orchestration is handled by Prefect. We can access the Prefect Server with:
```
kubectl port-forward service/prefect-server 4200:4200 -n prefect --context c-core
```

We have many Prefect Flows cataloged in @../holmes-2/components/prefect/flows.py. The CI code for these flows and APIs are described in @../holmes-2/projects/ and @../holmes-2/deployments/ .

The main purpose of this application is to detect targets and AIS data in satellite scenes, mostly Sentinel 1 and RCM SAR data.

## The problem
Occasionally we have a data processing error in a Prefect Deployment Flow. It can be difficult to determine which `scene_id` the error effects (or perhaps multiple scene_ids). All the various Prefect Flows have unique random UUIDs and some run in parallel, but might rely on each other downstream, making it diffifult to know which Prefect Flows need to be re-run. Also, when a provessing workflow breaks, it can leave data behind in a broken state.

## The goal
We want to build Python FastAPI API that lists recent SAR scene_ids, a few summary statistics (e.g. number of targets detected, type of targets detected), and a button that re-runs the parent Prefect Flow deployment. Optionally, we want to be able to delete as data associated with that scene_id so we can re-run from a clean state in case of collisions with data from the failed flow run. Later, we will add a web dashboard for interacting with the API. The 

Please create a plan. You may think hard. Please follow data science engineering best practices.

## Plan and execution summary

### Plan

Decisions made before building:

- **Standalone service, fresh logic.** A uv project in this repo, with no dependency on
  holmes-2 code. holmes-2 already has `/workflow/rerun-flow` endpoints; they were
  deliberately not reused.
- **Purge scope: derived database rows only.** Detections and their products are
  deleted. The scene's pgstac item, event history, order records and object storage are
  kept.
- **Find the parent from Prefect's own lineage** instead of a hard-coded flow map. The
  Holmes event tables give the run names for a scene. Prefect's
  `parent_task_run_id` / `parent_flow_run_id` links give the tree, and the walk upward
  stops below the multi-scene scheduler flows.
- **Safe by default.** Read-only database engine; mutations off unless explicitly
  enabled with an API key; dry run is the default for re-runs and purges.
- **Validate before building.** A read-only "Phase 0" spike against the live cluster
  came first, to confirm the schema and lineage assumptions.

### Execution

1. **Phase 0 (read-only, live `c-core` cluster).** Confirmed the SAR collections, event
   coverage, Prefect lineage in both directions, FK delete rules, and parent deployment
   parameter schemas. Several plan details changed as a result; see
   [Phase 0 findings](#phase-0-findings-live-c-core-cluster-2026-09-29). The main ones:
   - Target counts use `targets`, because `grouped_targets` is empty.
   - Chips aren't STAC items, so the purge has no chip step.
   - The `workflow.*` tables are stale or missing, so they aren't used.
   - RCM's Kpler AIS run is a *sibling* of `download-rcm-scene`, which led to the
     `include_sibling_roots` option.
   - `jacmd-*` collections reuse `dalo-*` scene ids, so scenes are always keyed by
     `(collection, scene_id)`.
2. **Built** the modules in `src/c_core_health/`:
   - `settings` and `db`
   - `prefect_api`: a typed REST client
   - `scenes`: keyset-paginated listing and aggregated stats
   - `lineage`, `rerun` and `purge`
   - `api/`: FastAPI routes, the API-key and mutation gate, CORS, and health checks
3. **Verified:**
   - Every endpoint was exercised against the live database and Prefect with mutations
     disabled. Dry-run re-runs resolved
     `fast-sar-detector-processing-chain-deployment` (S1) and
     `download-rcm-scene-deployment` (RCM), each with `force: true`.
   - 49 unit and database tests: Prefect is faked with respx, and purge and SQL behaviour
     run on a testcontainers Postgres.
   - 4 read-only live smoke tests (`pytest -m live`).
   - ruff and pyright (strict) are clean.

### Not yet done

- No real (non-dry-run) re-run or purge has been executed against the live cluster.
  Those paths are covered only by the container-based tests.
- Annotation `subject_key` semantics are unconfirmed. Detection annotations are counted
  as analyst data and kept, never deleted.
- The scene detail and purge-preview endpoints take about 3 seconds each, mostly
  Prefect round-trips.
- The web dashboard and Firebase auth are deferred, as the brief asked.

## Running the service

```sh
uv sync
cp .env.example .env          # values match the port-forwards above
uv run c-core-health          # http://127.0.0.1:8000/docs
```

The service is read-only by default. Re-runs and purges need both
`CCH_ENABLE_MUTATIONS=true` and `CCH_API_KEY`, and callers must send `X-Api-Key`. Every
setting is a `CCH_*` environment variable (see
[`src/c_core_health/settings.py`](src/c_core_health/settings.py)).

| Endpoint | Purpose |
| --- | --- |
| `GET /scenes?since=&until=&collection=&sensor=&limit=&cursor=` | Recent SAR scenes with target stats and failed/unfinished flows |
| `GET /scenes/{scene_id}` | Stats plus the full Prefect run tree |
| `GET /scenes/{scene_id}/lineage` | Run tree only |
| `POST /scenes/{scene_id}/rerun` | Re-run the per-scene parent deployment (`dry_run` defaults to true) |
| `GET /scenes/{scene_id}/purge-preview` | Row counts a purge would delete |
| `POST /scenes/{scene_id}/purge` | Delete derived rows (`dry_run` defaults to true) |
| `GET /healthz`, `GET /readyz` | Liveness, and DB plus Prefect reachability |

`jacmd-*` collections mirror `dalo-*` scene ids, so pass `?collection=` when a scene id
is ambiguous (the 409 response lists the candidates).

### How lineage and re-runs work

1. Run names for the scene are read from `features.scene_event` / `features.flow_event`,
   then matched to Prefect runs by `(flow name, run name)`. Ties are broken by whether
   the run's parameters mention the scene, then by start time.
2. From each run, the service walks up via `parent_task_run_id` and stops just below a
   scheduler flow (`CCH_SCHEDULER_FLOW_NAMES`). It then walks down via
   `parent_flow_run_id`, which also finds runs that crashed before writing any events.
3. The parent is the newest root whose flow is in `CCH_PARENT_FLOW_NAMES`
   (`fast-sar-detector-processing-chain` for S1, `download-rcm-scene` for RCM).
   `include_sibling_roots=true` also re-runs roots started by the same scheduler run,
   such as RCM Kpler AIS acquisition.
4. The re-run reuses the parent's deployment and original parameters, plus
   `CCH_RERUN_PARAMETER_OVERRIDES` (default `{"force": true}`) for keys the deployment
   declares. New runs are tagged `c-core-health` and `scene:<collection>/<scene_id>`, so
   later lookups find them before they write events. The idempotency key is derived from
   the source run, so a double-click cannot create two runs.

A re-run is refused (409) when the scene has active runs, no parent can be found, or the
parent's parameters don't mention the scene (the guard against re-running a
multi-scene batch flow).

### Purge

A purge deletes `features.targets`, together with their `ais_targets`,
`iceberg_drift_forecasting` rows and in-scene duplicates. It also deletes
`grouped_targets`, `oil_spill_detection`, `oil_spill_alert`, `scene_qa` and
`stac_pmtiles`, and nulls `unseenlabs_target_information` / `ais_vessels` links. With
`include_ais_messages=true` it also deletes `ais_message`. It keeps the pgstac item,
event history, order records, `annotations` and object storage.

Suggested flow: `GET purge-preview`, then
`POST purge {"dry_run": false, "confirm_scene_id": ..., "expected_deletes": <preview.deletes>}`,
then `POST rerun {"dry_run": false}`. The purge runs in one REPEATABLE READ transaction.
Before deleting, it re-checks for blockers (active runs, analyst-authored data unless
`allow_analyst_data_loss=true`, and cross-scene `duplicate_of` links) and checks that the
counts still match the preview. It then writes every affected row as JSON Lines to
`CCH_BACKUP_DIR/<timestamp>_<collection>_<scene>/`. If any delete count differs from the
preview, the whole transaction rolls back. Re-runs and purges are also appended to
`CCH_BACKUP_DIR/audit.jsonl`.

### Deployment

The service runs in the `c-core-iceland` GKE cluster (namespace `holmes`) and is served
at `https://iceland.c-core.app/health/docs` behind IAP. Manifests live in the sibling
`c-core-cd` repo under `overlays/c-core-iceland/holmes/c-core-health/`; the ArgoCD
Application is `overlays/c-core-autopilot/argocd/iceland-c-core-health-app.yaml`.

- **Image:** build with Cloud Build, then set `newTag` in the overlay's
  `kustomization.yaml` to the tag.

  ```sh
  gcloud builds submit --project ccore-holmes --config cloudbuild.yaml \
    --substitutions=SHORT_SHA=$(git rev-parse --short=9 HEAD) .
  ```

- **Config:** the Deployment sets `CCH_PREFECT_API_URL` to the in-cluster Prefect server,
  `CCH_ENABLE_MUTATIONS=true` and `CCH_ROOT_PATH=/health` (the gateway strips the
  prefix). `CCH_PG_DSN` and `CCH_API_KEY` come from the SOPS secret
  `c-core-health-secrets`.
- **Database:** the service connects as the `c_core_health` role on `holmes-db`. CNPG
  creates it (`holmes-cluster.yaml`) and a PostSync Job applies the grants in
  `overlays/c-core-iceland/cloudnative-pg/c-core-health-grants.sql`. It can read the
  tables in `tables.py` and write only what a purge changes. Update that SQL when
  `purge.py` starts touching other tables.
- **Backups:** purge backups and `audit.jsonl` are written to a 5Gi PVC mounted at
  `/data`. The Deployment uses `Recreate` and one replica because the volume is
  single-writer.

### Development

```sh
uv run ruff check . && uv run ruff format --check . && uv run pyright
uv run pytest                 # unit + DB tests (DB tests skip without Docker)
uv run pytest -m live         # read-only smoke tests against the port-forwards
uv run pre-commit install
```

The DB tests start `postgres:16-alpine` via testcontainers and load
[`tests/db/schema.sql`](tests/db/schema.sql), a minimal mirror of the live tables and
FKs. With colima, first run
`export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock`.

## Phase 0 findings (live `c-core` cluster, 2026-09-29)

Verified read-only against the port-forwarded database (`holmes`) and Prefect server (3.6.15).

- **SAR collections.** Scenes are `pgstac.items` rows in collections matching `%-s1` or
  `%-rcm` (`dalo-greenland-eez-s1`, `dalo-faroe-eez-s1`, `dalo-greenland-rcm`,
  `dalo-faroe-rcm` and their `jacmd-*` mirrors). Unseenlabs, ice charts and AOI
  collections are not SAR scenes and are excluded by default.
- **Scene-to-run link.** `features.scene_event` joined to `features.flow_event` covers the
  whole chain for recent S1 and RCM scenes. Each flow run writes `start` and `end` (or
  `fail`) events. `flow_event.flow_id` is the Prefect run *name* (for example
  `clever-crab`), which is not globally unique, so runs are matched on
  `(flow name, run name)` and disambiguated by time.
- **Prefect lineage.** Child runs have `parent_task_run_id` set, and
  `flow_runs/filter` accepts `parent_flow_run_id`, so the run tree can be walked both up
  and down. Observed chains:
  - S1: `aoi-trigger-for-s1` (scheduler) -> `fast-sar-detector-processing-chain`
    (parent) -> `acquire-ais-data-from-kpler-for-scene`, `fast-sar-detect-objects` -> ...
  - RCM: `aoi-triggers` -> `search-rcm-scene` (both schedulers) -> `download-rcm-scene`
    (parent) -> `fast-sar-detect-objects` -> ... In parallel, `search-rcm-scene` also
    starts `acquire-ais-data-from-kpler-for-scene` as a *sibling* of the parent, so an
    RCM scene can have two top-level runs.
- **Parent parameters.** The S1 parent's `image_id` is the scene id plus `.SAFE`, so the
  "scene id appears in the parameters" guard is a substring match. Both parent
  deployments declare `force` in `parameter_openapi_schema`. The RCM `download_url` is
  presigned, but `download-rcm-scene` refreshes it from the EODMS order id, so re-running
  with the original parameters is safe.
- **Target counts.** `features.grouped_targets` is empty on this cluster, so stats use
  `features.targets` (one row per detection per polarization, excluding soft-deleted
  rows). Type names come from `features.target_types`.
- **Chip STAC items.** None exist: `fast-sar-detector-image-chips` has zero items, and
  chips are only referenced from `targets.chip_item_id`/`chip_collection_id` (object
  storage). The purge therefore has no chip STAC step.
- **Workflow schema.** `workflow.flow_runs` stopped updating on 2026-09-15 and
  `workflow.scene_flow_status` does not exist, so the service does not use them.
- **Delete rules.** Deleting `features.targets` cascades to `ais_targets`,
  `iceberg_drift_forecasting` and `duplicate_of` children, and sets
  `unseenlabs_target_information.targets_table_id` and `ais_vessels.targets_table_id` to
  NULL. `duplicate_of` links are within a scene for sampled scenes, but the purge still
  refuses if any cross-scene duplicate link exists. `stac_pmtiles` and `annotations` are
  empty today but are still handled.
