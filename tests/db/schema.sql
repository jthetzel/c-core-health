-- Minimal mirror of the live Holmes tables this service touches (checked against the
-- c-core cluster on 2026-09-29). Only keys, FKs and the columns used are reproduced;
-- geometry columns are omitted so a plain Postgres image suffices.
CREATE SCHEMA pgstac;
CREATE SCHEMA features;

CREATE TABLE pgstac.collections (id text PRIMARY KEY);
CREATE TABLE pgstac.items (
    id text NOT NULL,
    collection text NOT NULL REFERENCES pgstac.collections (id),
    datetime timestamptz,
    content jsonb,
    PRIMARY KEY (collection, id)
);

CREATE TABLE features.target_types (id integer PRIMARY KEY, target_type_name varchar);

CREATE TABLE features.targets (
    id serial PRIMARY KEY,
    scene_item_id varchar NOT NULL,
    scene_collection_id varchar NOT NULL,
    target_type_id integer REFERENCES features.target_types (id),
    detected_from_polarization varchar,
    analyst_confirmed boolean,
    notes text,
    duplicate_of integer REFERENCES features.targets (id) ON DELETE CASCADE,
    created_at timestamp NOT NULL DEFAULT now(),
    deleted_at timestamp
);
CREATE INDEX ix_targets_scene_item_id ON features.targets (scene_item_id);

CREATE TABLE features.grouped_targets (
    id serial PRIMARY KEY,
    scene_item_id varchar NOT NULL,
    scene_collection_id varchar NOT NULL,
    analyst_confirmed boolean NOT NULL DEFAULT false,
    notes text,
    deleted_at timestamptz
);

CREATE TABLE features.ais_message (
    id serial PRIMARY KEY,
    scene_item_id varchar,
    scene_collection_id varchar,
    mmsi integer NOT NULL DEFAULT 0
);

CREATE TABLE features.ais_targets (
    target_id integer NOT NULL REFERENCES features.targets (id) ON DELETE CASCADE,
    ais_point_id integer NOT NULL REFERENCES features.ais_message (id) ON DELETE CASCADE,
    created_at timestamp,
    deleted_at timestamp,
    PRIMARY KEY (target_id, ais_point_id)
);

CREATE TABLE features.iceberg_drift_forecasting (
    id serial PRIMARY KEY,
    target_id integer REFERENCES features.targets (id) ON DELETE CASCADE
);

CREATE TABLE features.unseenlabs_target_information (
    id serial PRIMARY KEY,
    targets_table_id integer REFERENCES features.targets (id) ON DELETE SET NULL,
    ais_message_id integer REFERENCES features.ais_message (id) ON DELETE SET NULL
);

CREATE TABLE features.ais_vessels (
    id serial PRIMARY KEY,
    targets_table_id integer REFERENCES features.targets (id) ON DELETE SET NULL,
    ais_message_id integer REFERENCES features.ais_message (id) ON DELETE SET NULL
);

CREATE TABLE features.oil_spill_detection (
    id serial PRIMARY KEY,
    scene_item_id varchar NOT NULL,
    scene_collection_id varchar NOT NULL,
    deleted_at timestamp
);

CREATE TABLE features.oil_spill_alert (
    id serial PRIMARY KEY,
    scene_item_id text NOT NULL,
    scene_collection_id text NOT NULL
);

CREATE TABLE features.scene_qa (
    scene_collection_id text NOT NULL,
    scene_item_id text NOT NULL,
    qa_complete boolean NOT NULL DEFAULT false,
    PRIMARY KEY (scene_collection_id, scene_item_id)
);

CREATE TABLE features.stac_pmtiles (
    id serial PRIMARY KEY,
    collection_id varchar NOT NULL,
    stac_item_id varchar NOT NULL
);

CREATE TABLE features.annotations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    subject_type text NOT NULL,
    subject_key text NOT NULL
);

CREATE TABLE features.event (
    id uuid PRIMARY KEY,
    time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE features.scene_event (
    event_id uuid PRIMARY KEY REFERENCES features.event (id),
    collection_id text NOT NULL REFERENCES pgstac.collections (id),
    item_id text NOT NULL
);
CREATE TABLE features.flow_event (
    event_id uuid PRIMARY KEY REFERENCES features.event (id),
    type text NOT NULL,
    flow_name text NOT NULL,
    flow_id text NOT NULL,
    metadata json
);
