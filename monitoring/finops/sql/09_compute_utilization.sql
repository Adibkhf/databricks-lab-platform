-- system.compute.node_timeline contient une mesure par node et par minute.
-- C'est la source principale pour savoir si un Classic Compute
-- est réellement utilisé ou simplement allumé et coûteux.

CREATE OR REPLACE VIEW dbx_lab_dev.monitoring.compute_utilization AS

WITH latest_clusters AS (

    SELECT
        workspace_id,
        cluster_id,
        cluster_name,
        owned_by,
        cluster_source

    FROM system.compute.clusters

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY workspace_id, cluster_id
        ORDER BY change_time DESC
    ) = 1
)

SELECT
    nodes.workspace_id,
    nodes.cluster_id,
    clusters.cluster_name,

    nodes.start_time AS metric_time,
    DATE(nodes.start_time) AS metric_date,

    COUNT(DISTINCT nodes.instance_id) AS node_count,

    -- CPU réellement consacré au code utilisateur + système.
    ROUND(
        AVG(nodes.cpu_user_percent + nodes.cpu_system_percent),
        2
    ) AS avg_cpu_percent,

    ROUND(
        MAX(nodes.cpu_user_percent + nodes.cpu_system_percent),
        2
    ) AS max_cpu_percent,

    -- cpu_wait_percent élevé peut indiquer une attente I/O.
    ROUND(
        AVG(nodes.cpu_wait_percent),
        2
    ) AS avg_cpu_wait_percent,

    ROUND(
        AVG(nodes.mem_used_percent),
        2
    ) AS avg_memory_percent,

    ROUND(
        MAX(nodes.mem_used_percent),
        2
    ) AS max_memory_percent,

    ROUND(
        MAX(nodes.mem_swap_percent),
        2
    ) AS max_swap_percent,

    -- On sépare driver et workers car leurs comportements
    -- et leurs problèmes de dimensionnement sont différents.
    ROUND(
        AVG(
            CASE WHEN nodes.driver
                 THEN nodes.cpu_user_percent + nodes.cpu_system_percent
            END
        ),
        2
    ) AS avg_driver_cpu_percent,

    ROUND(
        AVG(
            CASE WHEN NOT nodes.driver
                 THEN nodes.cpu_user_percent + nodes.cpu_system_percent
            END
        ),
        2
    ) AS avg_worker_cpu_percent,

    SUM(nodes.network_sent_bytes) AS network_sent_bytes,
    SUM(nodes.network_received_bytes) AS network_received_bytes,

    clusters.owned_by,
    clusters.cluster_source

FROM system.compute.node_timeline AS nodes

LEFT JOIN latest_clusters AS clusters
    ON nodes.workspace_id = clusters.workspace_id
    AND nodes.cluster_id = clusters.cluster_id

GROUP BY
    nodes.workspace_id,
    nodes.cluster_id,
    clusters.cluster_name,
    nodes.start_time,
    clusters.owned_by,
    clusters.cluster_source;