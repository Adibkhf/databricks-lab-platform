-- system.access.table_lineage contient les relations source → target
-- observées par Unity Catalog lors des lectures/écritures.
--
-- On conserve également l'entité responsable :
-- notebook, Job, Pipeline, Dashboard ou requête SQL.

CREATE OR REPLACE VIEW dbx_lab_dev.monitoring.data_lineage AS

SELECT
    workspace_id,

    event_date,
    event_time,

    entity_type,
    entity_id,
    entity_run_id,

    source_type,
    source_table_catalog,
    source_table_schema,
    source_table_name,
    source_table_full_name,
    source_path,

    target_type,
    target_table_catalog,
    target_table_schema,
    target_table_name,
    target_table_full_name,
    target_path,

    created_by,

    statement_id,

    -- TRUE = dépendance directement référencée.
    -- FALSE peut représenter une dépendance indirecte,
    -- par exemple une table située derrière une vue.
    direct_access,

    event_id,
    record_id

FROM system.access.table_lineage;