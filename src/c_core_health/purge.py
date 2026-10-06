"""Delete derived database rows for one scene so it can be re-processed from clean.

Scope is deliberately narrow: detections and their products are deleted; the scene's
pgstac item, Holmes event history, order/download records, ``workflow.*`` and object
storage are kept. Every step is an explicit statement (no reliance on FK cascades) so
row counts can be previewed, backed up and verified exactly.
"""

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from loguru import logger
from sqlalchemy import (
    ColumnElement,
    Connection,
    Engine,
    Select,
    Table,
    cast,
    delete,
    func,
    literal_column,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.types import Text

from c_core_health.errors import ConflictError
from c_core_health.models import PurgePreview, PurgeRequest, PurgeResult, SceneRef
from c_core_health.tables import (
    ais_message,
    ais_targets,
    ais_vessels,
    annotations,
    grouped_targets,
    iceberg_drift_forecasting,
    oil_spill_alert,
    oil_spill_detection,
    scene_qa,
    stac_pmtiles,
    targets,
    unseenlabs_target_information,
)


@dataclass(frozen=True)
class PurgeStep:
    table: Table
    where: ColumnElement[bool]
    action: Literal["delete", "nullify"]
    column: str | None = None

    @property
    def label(self) -> str:
        name = f"{self.table.schema}.{self.table.name}"
        return f"{name}.{self.column}" if self.column else name


def _scene_target_ids(ref: SceneRef) -> Select[tuple[int]]:
    return select(targets.c.id).where(
        targets.c.scene_item_id == ref.scene_id,
        targets.c.scene_collection_id == ref.collection,
    )


def _scene_ais_message_ids(ref: SceneRef) -> Select[tuple[int]]:
    return select(ais_message.c.id).where(
        ais_message.c.scene_item_id == ref.scene_id,
        ais_message.c.scene_collection_id == ref.collection,
    )


def _scene_rows(table: Table, item_col: str, collection_col: str, ref: SceneRef):
    return (table.c[item_col] == ref.scene_id) & (
        table.c[collection_col] == ref.collection
    )


def purge_steps(ref: SceneRef, *, include_ais_messages: bool) -> list[PurgeStep]:
    """Ordered so every row is removed by its own step before its parent is deleted."""
    target_ids = _scene_target_ids(ref)
    message_ids = _scene_ais_message_ids(ref)

    ais_link = ais_targets.c.target_id.in_(target_ids)
    if include_ais_messages:
        ais_link = or_(ais_link, ais_targets.c.ais_point_id.in_(message_ids))

    steps = [
        PurgeStep(ais_targets, ais_link, "delete"),
        PurgeStep(
            iceberg_drift_forecasting,
            iceberg_drift_forecasting.c.target_id.in_(target_ids),
            "delete",
        ),
        PurgeStep(
            unseenlabs_target_information,
            unseenlabs_target_information.c.targets_table_id.in_(target_ids),
            "nullify",
            "targets_table_id",
        ),
        PurgeStep(
            ais_vessels,
            ais_vessels.c.targets_table_id.in_(target_ids),
            "nullify",
            "targets_table_id",
        ),
    ]
    if include_ais_messages:
        steps += [
            PurgeStep(
                unseenlabs_target_information,
                unseenlabs_target_information.c.ais_message_id.in_(message_ids),
                "nullify",
                "ais_message_id",
            ),
            PurgeStep(
                ais_vessels,
                ais_vessels.c.ais_message_id.in_(message_ids),
                "nullify",
                "ais_message_id",
            ),
        ]
    steps += [
        PurgeStep(
            targets,
            _scene_rows(targets, "scene_item_id", "scene_collection_id", ref),
            "delete",
        ),
        PurgeStep(
            grouped_targets,
            _scene_rows(grouped_targets, "scene_item_id", "scene_collection_id", ref),
            "delete",
        ),
        PurgeStep(
            oil_spill_detection,
            _scene_rows(oil_spill_detection, "scene_item_id", "scene_collection_id", ref),
            "delete",
        ),
        PurgeStep(
            oil_spill_alert,
            _scene_rows(oil_spill_alert, "scene_item_id", "scene_collection_id", ref),
            "delete",
        ),
        PurgeStep(
            scene_qa,
            _scene_rows(scene_qa, "scene_item_id", "scene_collection_id", ref),
            "delete",
        ),
        PurgeStep(
            stac_pmtiles,
            _scene_rows(stac_pmtiles, "stac_item_id", "collection_id", ref),
            "delete",
        ),
    ]
    if include_ais_messages:
        steps.append(
            PurgeStep(
                ais_message,
                _scene_rows(ais_message, "scene_item_id", "scene_collection_id", ref),
                "delete",
            )
        )
    return steps


def _count(conn: Connection, table: Table, where: ColumnElement[bool]) -> int:
    return conn.execute(select(func.count()).select_from(table).where(where)).scalar_one()


def analyst_data_counts(conn: Connection, ref: SceneRef) -> dict[str, int]:
    """Human-authored rows a purge would destroy (or orphan, for annotations)."""
    annotated = (
        annotations.c.subject_type == "detection"
    ) & annotations.c.subject_key.in_(
        select(cast(targets.c.id, Text)).where(
            targets.c.scene_item_id == ref.scene_id,
            targets.c.scene_collection_id == ref.collection,
        )
    )
    reviewed_targets = _scene_rows(
        targets, "scene_item_id", "scene_collection_id", ref
    ) & (targets.c.analyst_confirmed.is_(True) | targets.c.notes.is_not(None))
    reviewed_grouped = _scene_rows(
        grouped_targets, "scene_item_id", "scene_collection_id", ref
    ) & (
        grouped_targets.c.analyst_confirmed.is_(True)
        | grouped_targets.c.notes.is_not(None)
    )
    return {
        "features.targets (analyst_confirmed or notes)": _count(
            conn, targets, reviewed_targets
        ),
        "features.grouped_targets (analyst_confirmed or notes)": _count(
            conn, grouped_targets, reviewed_grouped
        ),
        "features.annotations (detections, kept but orphaned)": _count(
            conn, annotations, annotated
        ),
        "features.scene_qa": _count(
            conn,
            scene_qa,
            _scene_rows(scene_qa, "scene_item_id", "scene_collection_id", ref),
        ),
        "features.oil_spill_alert": _count(
            conn,
            oil_spill_alert,
            _scene_rows(oil_spill_alert, "scene_item_id", "scene_collection_id", ref),
        ),
    }


def cross_scene_duplicate_count(conn: Connection, ref: SceneRef) -> int:
    """Targets in *other* scenes whose ``duplicate_of`` (ON DELETE CASCADE) points here."""
    where = targets.c.duplicate_of.in_(_scene_target_ids(ref)) & ~(
        (targets.c.scene_item_id == ref.scene_id)
        & (targets.c.scene_collection_id == ref.collection)
    )
    return _count(conn, targets, where)


def build_preview(
    conn: Connection,
    ref: SceneRef,
    *,
    include_ais_messages: bool,
    allow_analyst_data_loss: bool,
    active_run_ids: Sequence[UUID] = (),
) -> PurgePreview:
    steps = purge_steps(ref, include_ais_messages=include_ais_messages)
    deletes = {
        s.label: _count(conn, s.table, s.where) for s in steps if s.action == "delete"
    }
    nullifies = {
        s.label: _count(conn, s.table, s.where) for s in steps if s.action == "nullify"
    }
    analyst = analyst_data_counts(conn, ref)
    cross_scene = cross_scene_duplicate_count(conn, ref)

    blockers: list[str] = []
    if active_run_ids:
        blockers.append(
            f"{len(active_run_ids)} flow run(s) for this scene are still active and may "
            "write new rows"
        )
    if cross_scene:
        blockers.append(
            f"{cross_scene} target(s) in other scenes are duplicate_of this scene's "
            "targets and would be cascade-deleted"
        )
    if any(analyst.values()) and not allow_analyst_data_loss:
        blockers.append(
            "Scene has analyst-authored data; set allow_analyst_data_loss=true to proceed"
        )
    return PurgePreview(
        scene=ref,
        include_ais_messages=include_ais_messages,
        deletes=deletes,
        nullifies=nullifies,
        analyst_data=analyst,
        cross_scene_duplicate_targets=cross_scene,
        blockers=blockers,
    )


def _backup_rows(conn: Connection, step: PurgeStep) -> Iterable[Any]:
    whole_row = func.row_to_json(literal_column(f'"{step.table.name}"'))
    stmt = select(whole_row).select_from(step.table).where(step.where)
    return conn.execute(stmt).scalars()


def write_backup(
    conn: Connection,
    backup_root: Path,
    ref: SceneRef,
    steps: list[PurgeStep],
    manifest: dict[str, Any],
) -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    directory = backup_root / f"{stamp}_{ref.collection}_{ref.scene_id}"
    directory.mkdir(parents=True, exist_ok=False)
    for step in steps:
        path = directory / f"{step.label}.jsonl"
        with path.open("w") as handle:
            for row in _backup_rows(conn, step):
                handle.write(json.dumps(row, default=str) + "\n")
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str))
    return directory


def _execute_step(conn: Connection, step: PurgeStep) -> int:
    if step.action == "delete":
        return conn.execute(delete(step.table).where(step.where)).rowcount
    assert step.column is not None
    stmt = update(step.table).where(step.where).values({step.column: None})
    return conn.execute(stmt).rowcount


def check_confirmation(ref: SceneRef, request: PurgeRequest) -> None:
    if request.confirm_scene_id != ref.scene_id:
        raise ConflictError(
            "confirm_scene_id does not match the scene being purged", scene_id=ref.scene_id
        )


def _apply_steps(
    conn: Connection, steps: list[PurgeStep], expected: dict[str, int]
) -> dict[str, int]:
    done: dict[str, int] = {}
    for step in steps:
        done[step.label] = _execute_step(conn, step)
        if done[step.label] != expected[step.label]:
            raise ConflictError(
                f"Row count mismatch on {step.label}; transaction rolled back",
                expected=expected[step.label],
                actual=done[step.label],
            )
    return done


def execute_purge(
    engine: Engine,
    backup_root: Path,
    ref: SceneRef,
    request: PurgeRequest,
    *,
    active_run_ids: Sequence[UUID] = (),
) -> PurgeResult:
    """Re-preview, back up, delete and verify inside one REPEATABLE READ transaction.

    REPEATABLE READ gives the backup and the deletes the same snapshot; a concurrent
    update to a row being deleted aborts the transaction instead of losing that update.
    """
    check_confirmation(ref, request)
    with (
        engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn,
        conn.begin(),
    ):
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        preview = build_preview(
            conn,
            ref,
            include_ais_messages=request.include_ais_messages,
            allow_analyst_data_loss=request.allow_analyst_data_loss,
            active_run_ids=active_run_ids,
        )
        if preview.blockers:
            raise ConflictError("Purge blocked", blockers=preview.blockers)
        if (
            request.expected_deletes is not None
            and request.expected_deletes != preview.deletes
        ):
            raise ConflictError(
                "Scene data changed since the preview; preview again",
                expected=request.expected_deletes,
                actual=preview.deletes,
            )
        steps = purge_steps(ref, include_ais_messages=request.include_ais_messages)
        manifest = {
            "scene": ref.model_dump(),
            "request": request.model_dump(),
            "preview": preview.model_dump(),
        }
        backup = write_backup(conn, backup_root, ref, steps, manifest)
        try:
            done = _apply_steps(conn, steps, {**preview.deletes, **preview.nullifies})
        except Exception:
            (backup / "ROLLED_BACK").touch()
            raise
    logger.bind(audit=True).info(
        "purge scene={} collection={} deleted={} backup={}",
        ref.scene_id,
        ref.collection,
        done,
        backup,
    )
    return PurgeResult(
        preview=preview,
        dry_run=False,
        executed=True,
        backup_path=str(backup),
        deleted=done,
    )
