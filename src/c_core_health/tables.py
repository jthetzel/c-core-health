"""SQLAlchemy Core definitions for the Holmes tables this service reads or purges.

Only the columns used here are declared. The schema is owned by holmes-2 alembic
migrations; nothing in this service creates or alters these tables.
"""

from sqlalchemy import (
    JSON,
    TIMESTAMP,
    Boolean,
    Column,
    Integer,
    MetaData,
    Table,
    Text,
    Uuid,
)
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData()

items = Table(
    "items",
    metadata,
    Column("id", Text, primary_key=True),
    Column("collection", Text, primary_key=True),
    Column("datetime", TIMESTAMP(timezone=True)),
    Column("content", JSONB),
    schema="pgstac",
)

targets = Table(
    "targets",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("scene_item_id", Text, nullable=False),
    Column("scene_collection_id", Text, nullable=False),
    Column("target_type_id", Integer),
    Column("detected_from_polarization", Text),
    Column("analyst_confirmed", Boolean),
    Column("notes", Text),
    Column("duplicate_of", Integer),
    Column("deleted_at", TIMESTAMP),
    schema="features",
)

target_types = Table(
    "target_types",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("target_type_name", Text),
    schema="features",
)

grouped_targets = Table(
    "grouped_targets",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("scene_item_id", Text, nullable=False),
    Column("scene_collection_id", Text, nullable=False),
    Column("analyst_confirmed", Boolean),
    Column("notes", Text),
    schema="features",
)

ais_targets = Table(
    "ais_targets",
    metadata,
    Column("target_id", Integer, primary_key=True),
    Column("ais_point_id", Integer, primary_key=True),
    Column("deleted_at", TIMESTAMP),
    schema="features",
)

ais_message = Table(
    "ais_message",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("scene_item_id", Text),
    Column("scene_collection_id", Text),
    schema="features",
)

iceberg_drift_forecasting = Table(
    "iceberg_drift_forecasting",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("target_id", Integer),
    schema="features",
)

unseenlabs_target_information = Table(
    "unseenlabs_target_information",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("targets_table_id", Integer),
    Column("ais_message_id", Integer),
    schema="features",
)

ais_vessels = Table(
    "ais_vessels",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("targets_table_id", Integer),
    Column("ais_message_id", Integer),
    schema="features",
)

oil_spill_detection = Table(
    "oil_spill_detection",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("scene_item_id", Text, nullable=False),
    Column("scene_collection_id", Text, nullable=False),
    Column("deleted_at", TIMESTAMP),
    schema="features",
)

oil_spill_alert = Table(
    "oil_spill_alert",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("scene_item_id", Text, nullable=False),
    Column("scene_collection_id", Text, nullable=False),
    schema="features",
)

scene_qa = Table(
    "scene_qa",
    metadata,
    Column("scene_collection_id", Text, primary_key=True),
    Column("scene_item_id", Text, primary_key=True),
    schema="features",
)

stac_pmtiles = Table(
    "stac_pmtiles",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("collection_id", Text, nullable=False),
    Column("stac_item_id", Text, nullable=False),
    schema="features",
)

annotations = Table(
    "annotations",
    metadata,
    Column("id", Uuid(), primary_key=True),
    Column("subject_type", Text, nullable=False),
    Column("subject_key", Text, nullable=False),
    schema="features",
)

event = Table(
    "event",
    metadata,
    Column("id", Uuid(), primary_key=True),
    Column("time", TIMESTAMP, nullable=False),
    schema="features",
)

scene_event = Table(
    "scene_event",
    metadata,
    Column("event_id", Uuid(), primary_key=True),
    Column("collection_id", Text, nullable=False),
    Column("item_id", Text, nullable=False),
    schema="features",
)

flow_event = Table(
    "flow_event",
    metadata,
    Column("event_id", Uuid(), primary_key=True),
    Column("type", Text, nullable=False),
    Column("flow_name", Text, nullable=False),
    Column("flow_id", Text, nullable=False),
    Column("metadata", JSON),
    schema="features",
)
