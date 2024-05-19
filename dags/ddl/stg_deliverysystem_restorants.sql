DROP TABLE IF EXISTS stg.deliverysystem_restorants CASCADE;
CREATE TABLE stg.deliverysystem_restorants(
    id serial,
    object_id varchar not null,
    object_value text not null,
    update_ts timestamp not null,
    CONSTRAINT deliverysystem_restorants_object_id_unique UNIQUE (object_id)
);