-- system.access.audit centralise les événements d'audit Databricks :
-- authentification, Unity Catalog, Jobs, clusters, permissions, administration, etc.
--
-- user_identity permet d'identifier l'acteur.
-- request_params décrit les paramètres de l'action.
-- response indique si l'opération a réussi ou échoué.
--
-- Dans notre workspace, les champs internes de response sont :
-- status_code, error_message et result.

CREATE OR REPLACE VIEW dbx_lab_dev.monitoring.security_events AS

SELECT
    account_id,
    workspace_id,

    event_date,
    event_time,

    service_name,
    action_name,

    -- Utilisateur ayant initié directement l'action.
    user_identity.email AS actor,

    -- Utiles notamment pour les workloads exécutés
    -- au nom d'une autre identité.
    identity_metadata.run_by AS run_by,
    identity_metadata.run_as AS run_as,

    source_ip_address,
    user_agent,

    audit_level,

    request_id,
    event_id,

    -- MAP contenant les paramètres propres à chaque type d'événement.
    request_params,

    -- Le struct response utilise ici des noms snake_case.
    response.status_code AS response_status_code,
    response.error_message AS response_error_message,

    -- Classification que NOUS ajoutons pour faciliter
    -- l'analyse et le filtrage dans le dashboard.
    CASE
        WHEN CAST(response.status_code AS INT) >= 400
            THEN 'ERROR'

        WHEN LOWER(action_name) RLIKE
            'grant|revoke|permission|privilege|delete|remove|token|secret'
            THEN 'SENSITIVE'

        ELSE 'STANDARD'
    END AS event_category

FROM system.access.audit;