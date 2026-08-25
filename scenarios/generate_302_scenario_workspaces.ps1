# ============================================================
# Databricks Scenario Workspace Generator - 302 scenarios
# Run this script FROM the repository scenarios/ directory.
#
# Each scenario gets files selected for the actual type of work:
# PySpark, SQL, PowerShell, YAML, Terraform, tests, etc.
# Existing files are NEVER overwritten.
# ============================================================

$ErrorActionPreference = "Stop"
$ScenariosRoot = $PSScriptRoot

$definitions = @(
    @{ Number = "001"; Folder = "scenario_001_nouveaux_fichiers_incrementaux"; Files = @("generate_incremental_orders.ps1", "ingest_incremental_orders.py", "validate_incremental_ingestion.sql") }
    @{ Number = "002"; Folder = "scenario_002_reemission_d_un_fichier_deja_traite"; Files = @("replay_processed_payments.ps1", "ingest_replayed_payments.py", "validate_file_idempotence.sql") }
    @{ Number = "003"; Folder = "scenario_003_quarantine_d_un_fichier_illisible"; Files = @("generate_corrupt_transactions.ps1", "ingest_transactions_with_quarantine.py", "validate_quarantine.sql") }
    @{ Number = "004"; Folder = "scenario_004_montant_texte_dans_payments"; Files = @("generate_invalid_payments.ps1", "ingest_payments_with_type_validation.py", "validate_payment_casts.sql") }
    @{ Number = "005"; Folder = "scenario_005_colonne_inattendue_cote_source"; Files = @("generate_unexpected_events_column.ps1", "ingest_events_with_rescued_data.py", "inspect_rescued_data.sql") }
    @{ Number = "006"; Folder = "scenario_006_ajout_controle_d_une_colonne_source"; Files = @("generate_schema_evolution_customers.ps1", "ingest_customers_schema_evolution.py", "validate_schema_evolution.sql") }
    @{ Number = "007"; Folder = "scenario_007_nom_partition_de_fichier_non_conforme"; Files = @("generate_invalid_order_paths.ps1", "validate_landing_contract.py", "validate_file_quarantine.sql") }
    @{ Number = "008"; Folder = "scenario_008_arrivee_massive_de_petits_fichiers"; Files = @("generate_small_files.ps1", "ingest_small_files.py", "generate_compact_file.ps1", "ingest_compact_file.py", "compare_small_files.sql") }
    @{ Number = "009"; Folder = "scenario_009_reprise_apres_pause_de_2_heures"; Files = @("generate_backlog_after_pause.ps1", "ingest_after_pause.py", "measure_catchup.sql") }
    @{ Number = "010"; Folder = "scenario_010_suppression_du_checkpoint_auto_loader"; Files = @("baseline_autoloader.py", "delete_checkpoint.ps1", "replay_after_checkpoint_loss.py", "validate_checkpoint_replay.sql") }
    @{ Number = "011"; Folder = "scenario_011_appliquer_uniquement_les_nouveaux_enregistrements"; Files = @("generate_incremental_cdc.ps1", "apply_new_records_only.py", "validate_new_records.sql") }
    @{ Number = "012"; Folder = "scenario_012_propagation_d_une_mise_a_jour"; Files = @("generate_update_cdc.ps1", "apply_update_cdc.py", "validate_update_propagation.sql") }
    @{ Number = "013"; Folder = "scenario_013_propagation_d_une_suppression"; Files = @("generate_delete_cdc.ps1", "apply_delete_cdc.py", "validate_delete_propagation.sql") }
    @{ Number = "014"; Folder = "scenario_014_insert_update_delete_dans_le_meme_lot"; Files = @("generate_mixed_cdc_batch.ps1", "apply_mixed_cdc.py", "validate_mixed_cdc.sql") }
    @{ Number = "015"; Folder = "scenario_015_ancienne_version_recue_apres_la_nouvelle"; Files = @("generate_out_of_order_cdc.ps1", "apply_sequence_guard.py", "validate_out_of_order_cdc.sql") }
    @{ Number = "016"; Folder = "scenario_016_retraiter_une_journee_sans_doubler_silver"; Files = @("generate_silver_backfill.ps1", "backfill_silver_idempotent.py", "validate_silver_backfill.sql") }
    @{ Number = "017"; Folder = "scenario_017_fichier_j_1_livre_aujourd_hui"; Files = @("generate_late_partition_file.ps1", "ingest_late_partition.py", "validate_late_partition.sql") }
    @{ Number = "018"; Folder = "scenario_018_un_export_source_n_est_qu_a_moitie_livre"; Files = @("generate_partial_export.ps1", "detect_incomplete_export.py", "validate_export_completeness.sql") }
    @{ Number = "019"; Folder = "scenario_019_meme_evenement_avec_timestamp_d_ingestion_different"; Files = @("generate_duplicate_event_versions.ps1", "deduplicate_event_versions.py", "validate_event_dedup.sql") }
    @{ Number = "020"; Folder = "scenario_020_rejouer_un_mois_de_raw"; Files = @("generate_month_raw_replay.ps1", "replay_month_raw.py", "validate_month_replay.sql") }
    @{ Number = "021"; Folder = "scenario_021_donnees_legerement_tardives"; Files = @("generate_donnees_legerement_tardives_data.py", "stream_donnees_legerement_tardives.py", "monitor_donnees_legerement_tardives.sql") }
    @{ Number = "022"; Folder = "scenario_022_donnees_tres_tardives"; Files = @("generate_donnees_tres_tardives_data.py", "stream_donnees_tres_tardives.py", "monitor_donnees_tres_tardives.sql") }
    @{ Number = "023"; Folder = "scenario_023_fenetres_temporelles_de_ventes"; Files = @("generate_fenetres_temporelles_ventes_data.py", "stream_fenetres_temporelles_ventes.py", "validate_fenetres_temporelles_ventes.sql") }
    @{ Number = "024"; Folder = "scenario_024_event_id_duplique"; Files = @("generate_event_id_duplique_data.py", "stream_event_id_duplique.py", "validate_event_id_duplique.sql") }
    @{ Number = "025"; Folder = "scenario_025_etat_qui_augmente_sans_borne"; Files = @("generate_etat_augmente_borne_data.py", "stream_etat_augmente_borne.py", "monitor_etat_augmente_borne.sql") }
    @{ Number = "026"; Folder = "scenario_026_jointure_sans_contrainte_temporelle"; Files = @("generate_jointure_contrainte_temporelle_data.py", "stream_jointure_contrainte_temporelle.py", "validate_jointure_contrainte_temporelle.sql") }
    @{ Number = "027"; Folder = "scenario_027_jointure_avec_borne_temporelle"; Files = @("generate_jointure_borne_temporelle_data.py", "stream_jointure_borne_temporelle.py", "validate_jointure_borne_temporelle.sql") }
    @{ Number = "028"; Folder = "scenario_028_transformation_trop_lente"; Files = @("generate_transformation_trop_lente_data.py", "stream_transformation_trop_lente.py", "monitor_transformation_trop_lente.sql") }
    @{ Number = "029"; Folder = "scenario_029_input_rate_superieur_au_processing_rate"; Files = @("generate_input_rate_superieur_processing_rate_data.py", "stream_input_rate_superieur_processing_rate.py", "monitor_input_rate_superieur_processing_rate.sql") }
    @{ Number = "030"; Folder = "scenario_030_merge_idempotent"; Files = @("generate_merge_idempotent_data.py", "stream_merge_idempotent.py", "validate_merge_idempotent.sql") }
    @{ Number = "031"; Folder = "scenario_031_echec_d_ecriture_delta"; Files = @("generate_echec_ecriture_delta_data.py", "stream_echec_ecriture_delta.py", "validate_echec_ecriture_delta.sql") }
    @{ Number = "032"; Folder = "scenario_032_redemarrage_apres_arret_du_cluster"; Files = @("generate_redemarrage_arret_cluster_data.py", "stream_redemarrage_arret_cluster.py", "validate_redemarrage_arret_cluster.sql") }
    @{ Number = "033"; Folder = "scenario_033_backfill_avec_availablenow"; Files = @("generate_backfill_availablenow_data.py", "stream_backfill_availablenow.py", "validate_backfill_availablenow.sql") }
    @{ Number = "034"; Folder = "scenario_034_nouvelle_colonne_pendant_le_stream"; Files = @("generate_nouvelle_colonne_pendant_stream_data.py", "stream_nouvelle_colonne_pendant_stream.py", "validate_nouvelle_colonne_pendant_stream.sql") }
    @{ Number = "035"; Folder = "scenario_035_evenements_fortement_desordonnes"; Files = @("generate_evenements_fortement_desordonnes_data.py", "stream_evenements_fortement_desordonnes.py", "validate_evenements_fortement_desordonnes.sql") }
    @{ Number = "036"; Folder = "scenario_036_une_cle_monopolise_le_state"; Files = @("generate_cle_monopolise_state_data.py", "stream_cle_monopolise_state.py", "monitor_cle_monopolise_state.sql") }
    @{ Number = "037"; Folder = "scenario_037_deux_streams_ecrivent_la_meme_cible"; Files = @("generate_deux_streams_ecrivent_cible_data.py", "stream_deux_streams_ecrivent_cible.py", "validate_deux_streams_ecrivent_cible.sql") }
    @{ Number = "038"; Folder = "scenario_038_fraicheur_silver_depasse_5_minutes"; Files = @("generate_fraicheur_silver_depasse_5_minutes_data.py", "stream_fraicheur_silver_depasse_5_minutes.py", "monitor_fraicheur_silver_depasse_5_minutes.sql") }
    @{ Number = "039"; Folder = "scenario_039_deux_jobs_partagent_accidentellement_le_checkpoint"; Files = @("generate_deux_jobs_partagent_accidentellement_c_data.py", "stream_deux_jobs_partagent_accidentellement_c.py", "monitor_deux_jobs_partagent_accidentellement_c.sql") }
    @{ Number = "040"; Folder = "scenario_040_micro_batches_trop_frequents_pour_faible_volume"; Files = @("generate_micro_batches_trop_frequents_faible_vo_data.py", "stream_micro_batches_trop_frequents_faible_vo.py", "monitor_micro_batches_trop_frequents_faible_vo.sql") }
    @{ Number = "041"; Folder = "scenario_041_compacter_une_bronze_tres_fragmentee"; Files = @("prepare_compacter_bronze_tres_fragmentee.sql", "benchmark_compacter_bronze_tres_fragmentee.py", "apply_compacter_bronze_tres_fragmentee.sql", "validate_compacter_bronze_tres_fragmentee.sql") }
    @{ Number = "042"; Folder = "scenario_042_clustering_adapte_aux_acces_gold"; Files = @("prepare_clustering_adapte_acces_gold.sql", "benchmark_clustering_adapte_acces_gold.py", "apply_clustering_adapte_acces_gold.sql", "validate_clustering_adapte_acces_gold.sql") }
    @{ Number = "043"; Folder = "scenario_043_mauvaise_cle_de_clustering"; Files = @("prepare_mauvaise_cle_clustering.sql", "benchmark_mauvaise_cle_clustering.py", "apply_mauvaise_cle_clustering.sql", "validate_mauvaise_cle_clustering.sql") }
    @{ Number = "044"; Folder = "scenario_044_verifier_l_efficacite_du_skipping"; Files = @("prepare_verifier_efficacite_skipping.sql", "benchmark_verifier_efficacite_skipping.py", "apply_verifier_efficacite_skipping.sql", "validate_verifier_efficacite_skipping.sql") }
    @{ Number = "045"; Folder = "scenario_045_merge_qui_scanne_trop_de_donnees"; Files = @("prepare_merge_scanne_trop_donnees.sql", "benchmark_merge_scanne_trop_donnees.py", "apply_merge_scanne_trop_donnees.sql", "validate_merge_scanne_trop_donnees.sql") }
    @{ Number = "046"; Folder = "scenario_046_merge_optimise_par_pruning"; Files = @("prepare_merge_optimise_pruning.sql", "benchmark_merge_optimise_pruning.py", "apply_merge_optimise_pruning.sql", "validate_merge_optimise_pruning.sql") }
    @{ Number = "047"; Folder = "scenario_047_update_large_scope_involontaire"; Files = @("prepare_update_large_scope_involontaire.sql", "benchmark_update_large_scope_involontaire.py", "apply_update_large_scope_involontaire.sql", "validate_update_large_scope_involontaire.sql") }
    @{ Number = "048"; Folder = "scenario_048_delete_massif_et_impact_stockage"; Files = @("prepare_delete_massif_impact_stockage.sql", "benchmark_delete_massif_impact_stockage.py", "apply_delete_massif_impact_stockage.sql", "validate_delete_massif_impact_stockage.sql") }
    @{ Number = "049"; Folder = "scenario_049_comparer_suppression_avec_dv_lorsque_disponible"; Files = @("prepare_comparer_suppression_dv_lorsque_dispon.sql", "apply_comparer_suppression_dv_lorsque_dispon.sql", "validate_comparer_suppression_dv_lorsque_dispon.sql") }
    @{ Number = "050"; Folder = "scenario_050_propager_seulement_les_changements"; Files = @("prepare_propager_seulement_changements.sql", "apply_propager_seulement_changements.sql", "validate_propager_seulement_changements.sql") }
    @{ Number = "051"; Folder = "scenario_051_comparer_gold_avant_apres_incident"; Files = @("prepare_comparer_gold_incident.sql", "apply_comparer_gold_incident.sql", "validate_comparer_gold_incident.sql") }
    @{ Number = "052"; Folder = "scenario_052_restaurer_une_version_saine"; Files = @("prepare_restaurer_version_saine.sql", "apply_restaurer_version_saine.sql", "validate_restaurer_version_saine.sql") }
    @{ Number = "053"; Folder = "scenario_053_retention_et_time_travel"; Files = @("prepare_retention_time_travel.sql", "benchmark_retention_time_travel.py", "apply_retention_time_travel.sql", "validate_retention_time_travel.sql") }
    @{ Number = "054"; Folder = "scenario_054_ecriture_incompatible_bloquee"; Files = @("prepare_ecriture_incompatible_bloquee.sql", "apply_ecriture_incompatible_bloquee.sql", "validate_ecriture_incompatible_bloquee.sql") }
    @{ Number = "055"; Folder = "scenario_055_evolution_controlee_d_une_table"; Files = @("prepare_evolution_controlee_table.sql", "apply_evolution_controlee_table.sql", "validate_evolution_controlee_table.sql") }
    @{ Number = "056"; Folder = "scenario_056_overwrite_accidentel_d_une_cible"; Files = @("prepare_overwrite_accidentel_cible.sql", "apply_overwrite_accidentel_cible.sql", "validate_overwrite_accidentel_cible.sql") }
    @{ Number = "057"; Folder = "scenario_057_conflit_d_ecriture_optimiste"; Files = @("prepare_conflit_ecriture_optimiste.sql", "apply_conflit_ecriture_optimiste.sql", "validate_conflit_ecriture_optimiste.sql") }
    @{ Number = "058"; Folder = "scenario_058_cycle_de_vie_des_donnees"; Files = @("prepare_cycle_vie_donnees.sql", "apply_cycle_vie_donnees.sql", "validate_cycle_vie_donnees.sql") }
    @{ Number = "059"; Folder = "scenario_059_audit_d_une_table_apres_plusieurs_mutations"; Files = @("prepare_audit_table_plusieurs_mutations.sql", "apply_audit_table_plusieurs_mutations.sql", "validate_audit_table_plusieurs_mutations.sql") }
    @{ Number = "060"; Folder = "scenario_060_plan_de_maintenance_differencie_bronze_silver_gold"; Files = @("prepare_plan_maintenance_differencie_bronze_si.sql", "apply_plan_maintenance_differencie_bronze_si.sql", "validate_plan_maintenance_differencie_bronze_si.sql") }
    @{ Number = "061"; Folder = "scenario_061_agregation_avec_shuffle_excessif"; Files = @("generate_agregation_shuffle_excessif_dataset.py", "benchmark_agregation_shuffle_excessif.py", "optimize_agregation_shuffle_excessif.py") }
    @{ Number = "062"; Folder = "scenario_062_pas_assez_de_partitions"; Files = @("generate_assez_partitions_dataset.py", "benchmark_assez_partitions.py", "optimize_assez_partitions.py") }
    @{ Number = "063"; Folder = "scenario_063_trop_de_partitions"; Files = @("generate_trop_partitions_dataset.py", "benchmark_trop_partitions.py", "optimize_trop_partitions.py") }
    @{ Number = "064"; Folder = "scenario_064_repartition_inutile_avant_ecriture"; Files = @("generate_repartition_inutile_ecriture_dataset.py", "benchmark_repartition_inutile_ecriture.py", "optimize_repartition_inutile_ecriture.py") }
    @{ Number = "065"; Folder = "scenario_065_coalesce_trop_agressif"; Files = @("generate_coalesce_trop_agressif_dataset.py", "benchmark_coalesce_trop_agressif.py", "optimize_coalesce_trop_agressif.py") }
    @{ Number = "066"; Folder = "scenario_066_join_fortement_skewe"; Files = @("generate_join_fortement_skewe_dataset.py", "benchmark_join_fortement_skewe.py", "optimize_join_fortement_skewe.py") }
    @{ Number = "067"; Folder = "scenario_067_agregation_skewee"; Files = @("generate_agregation_skewee_dataset.py", "benchmark_agregation_skewee.py", "optimize_agregation_skewee.py") }
    @{ Number = "068"; Folder = "scenario_068_broadcast_pertinent"; Files = @("generate_broadcast_pertinent_dataset.py", "benchmark_broadcast_pertinent.py", "optimize_broadcast_pertinent.py") }
    @{ Number = "069"; Folder = "scenario_069_broadcast_trop_volumineux"; Files = @("generate_broadcast_trop_volumineux_dataset.py", "benchmark_broadcast_trop_volumineux.py", "optimize_broadcast_trop_volumineux.py") }
    @{ Number = "070"; Folder = "scenario_070_smj_couteux"; Files = @("generate_smj_couteux_dataset.py", "benchmark_smj_couteux.py", "optimize_smj_couteux.py") }
    @{ Number = "071"; Folder = "scenario_071_tester_un_candidat_shj"; Files = @("generate_tester_candidat_shj_dataset.py", "benchmark_tester_candidat_shj.py", "optimize_tester_candidat_shj.py") }
    @{ Number = "072"; Folder = "scenario_072_mauvais_ordre_creant_un_intermediaire_enorme"; Files = @("generate_mauvais_ordre_creant_intermediaire_eno_dataset.py", "benchmark_mauvais_ordre_creant_intermediaire_eno.py", "optimize_mauvais_ordre_creant_intermediaire_eno.py") }
    @{ Number = "073"; Folder = "scenario_073_explosion_many_to_many_non_voulue"; Files = @("generate_explosion_many_to_many_non_voulue_dataset.py", "benchmark_explosion_many_to_many_non_voulue.py", "optimize_explosion_many_to_many_non_voulue.py") }
    @{ Number = "074"; Folder = "scenario_074_lecture_de_colonnes_inutiles"; Files = @("generate_lecture_colonnes_inutiles_dataset.py", "benchmark_lecture_colonnes_inutiles.py", "optimize_lecture_colonnes_inutiles.py") }
    @{ Number = "075"; Folder = "scenario_075_filtre_applique_trop_tard"; Files = @("generate_filtre_applique_trop_tard_dataset.py", "benchmark_filtre_applique_trop_tard.py", "optimize_filtre_applique_trop_tard.py") }
    @{ Number = "076"; Folder = "scenario_076_predicat_non_exploitable"; Files = @("generate_predicat_non_exploitable_dataset.py", "benchmark_predicat_non_exploitable.py", "optimize_predicat_non_exploitable.py") }
    @{ Number = "077"; Folder = "scenario_077_dpp_sur_schema_etoile"; Files = @("generate_dpp_schema_etoile_dataset.py", "benchmark_dpp_schema_etoile.py", "optimize_dpp_schema_etoile.py") }
    @{ Number = "078"; Folder = "scenario_078_coalescing_des_partitions_shuffle"; Files = @("generate_coalescing_partitions_shuffle_dataset.py", "benchmark_coalescing_partitions_shuffle.py", "optimize_coalescing_partitions_shuffle.py") }
    @{ Number = "079"; Folder = "scenario_079_skew_join_optimization"; Files = @("generate_skew_join_optimization_dataset.py", "benchmark_skew_join_optimization.py", "optimize_skew_join_optimization.py") }
    @{ Number = "080"; Folder = "scenario_080_memory_bytes_spilled_eleve"; Files = @("generate_memory_bytes_spilled_eleve_dataset.py", "benchmark_memory_bytes_spilled_eleve.py", "optimize_memory_bytes_spilled_eleve.py") }
    @{ Number = "081"; Folder = "scenario_081_disk_bytes_spilled_eleve"; Files = @("generate_disk_bytes_spilled_eleve_dataset.py", "benchmark_disk_bytes_spilled_eleve.py", "optimize_disk_bytes_spilled_eleve.py") }
    @{ Number = "082"; Folder = "scenario_082_executor_oom_sur_partition_geante"; Files = @("generate_executor_oom_partition_geante_dataset.py", "benchmark_executor_oom_partition_geante.py", "optimize_executor_oom_partition_geante.py") }
    @{ Number = "083"; Folder = "scenario_083_driver_oom_via_collect"; Files = @("generate_driver_oom_via_collect_dataset.py", "benchmark_driver_oom_via_collect.py", "optimize_driver_oom_via_collect.py") }
    @{ Number = "084"; Folder = "scenario_084_udf_python_ligne_a_ligne_lente"; Files = @("generate_udf_python_ligne_ligne_lente_dataset.py", "benchmark_udf_python_ligne_ligne_lente.py", "optimize_udf_python_ligne_ligne_lente.py") }
    @{ Number = "085"; Folder = "scenario_085_vectoriser_un_traitement_python"; Files = @("generate_vectoriser_traitement_python_dataset.py", "benchmark_vectoriser_traitement_python.py", "optimize_vectoriser_traitement_python.py") }
    @{ Number = "086"; Folder = "scenario_086_cache_utile_pour_reutilisation_multiple"; Files = @("generate_cache_utile_reutilisation_multiple_dataset.py", "benchmark_cache_utile_reutilisation_multiple.py", "optimize_cache_utile_reutilisation_multiple.py") }
    @{ Number = "087"; Folder = "scenario_087_cache_inutile_d_un_dataset_consomme_une_fois"; Files = @("generate_cache_inutile_dataset_consomme_fois_dataset.py", "benchmark_cache_inutile_dataset_consomme_fois.py", "optimize_cache_inutile_dataset_consomme_fois.py") }
    @{ Number = "088"; Folder = "scenario_088_objets_colonnes_wide_couteux"; Files = @("generate_objets_colonnes_wide_couteux_dataset.py", "benchmark_objets_colonnes_wide_couteux.py", "optimize_objets_colonnes_wide_couteux.py") }
    @{ Number = "089"; Folder = "scenario_089_transformation_cpu_saturee"; Files = @("generate_transformation_cpu_saturee_dataset.py", "benchmark_transformation_cpu_saturee.py", "optimize_transformation_cpu_saturee.py") }
    @{ Number = "090"; Folder = "scenario_090_workload_limite_par_memoire"; Files = @("generate_workload_limite_memoire_dataset.py", "benchmark_workload_limite_memoire.py", "optimize_workload_limite_memoire.py") }
    @{ Number = "091"; Folder = "scenario_091_workload_domine_par_reseau_shuffle"; Files = @("generate_workload_domine_reseau_shuffle_dataset.py", "benchmark_workload_domine_reseau_shuffle.py", "optimize_workload_domine_reseau_shuffle.py") }
    @{ Number = "092"; Folder = "scenario_092_cluster_sous_dimensionne"; Files = @("generate_cluster_sous_dimensionne_dataset.py", "benchmark_cluster_sous_dimensionne.py", "optimize_cluster_sous_dimensionne.py") }
    @{ Number = "093"; Folder = "scenario_093_cluster_surdimensionne"; Files = @("generate_cluster_surdimensionne_dataset.py", "benchmark_cluster_surdimensionne.py", "optimize_cluster_surdimensionne.py") }
    @{ Number = "094"; Folder = "scenario_094_scale_up_vs_scale_out"; Files = @("generate_scale_up_vs_scale_out_dataset.py", "benchmark_scale_up_vs_scale_out.py", "optimize_scale_up_vs_scale_out.py") }
    @{ Number = "095"; Folder = "scenario_095_statistiques_cardinalites_trompeuses"; Files = @("generate_statistiques_cardinalites_trompeuses_dataset.py", "benchmark_statistiques_cardinalites_trompeuses.py", "optimize_statistiques_cardinalites_trompeuses.py") }
    @{ Number = "096"; Folder = "scenario_096_comparer_cluster_fixe_et_autoscaling"; Files = @("benchmark_comparer_cluster_fixe_autoscaling.py", "configure_comparer_cluster_fixe_autoscaling.ps1", "compare_comparer_cluster_fixe_autoscaling_metrics.sql") }
    @{ Number = "097"; Folder = "scenario_097_min_workers_trop_eleve"; Files = @("benchmark_min_workers_trop_eleve.py", "configure_min_workers_trop_eleve.ps1", "compare_min_workers_trop_eleve_metrics.sql") }
    @{ Number = "098"; Folder = "scenario_098_max_workers_trop_faible"; Files = @("benchmark_max_workers_trop_faible.py", "configure_max_workers_trop_faible.ps1", "compare_max_workers_trop_faible_metrics.sql") }
    @{ Number = "099"; Folder = "scenario_099_worker_trop_petit"; Files = @("benchmark_worker_trop_petit.py", "configure_worker_trop_petit.ps1", "compare_worker_trop_petit_metrics.sql") }
    @{ Number = "100"; Folder = "scenario_100_worker_trop_gros"; Files = @("benchmark_worker_trop_gros.py", "configure_worker_trop_gros.ps1", "compare_worker_trop_gros_metrics.sql") }
    @{ Number = "101"; Folder = "scenario_101_driver_trop_petit"; Files = @("benchmark_driver_trop_petit.py", "configure_driver_trop_petit.ps1", "compare_driver_trop_petit_metrics.sql") }
    @{ Number = "102"; Folder = "scenario_102_cluster_oublie_actif"; Files = @("benchmark_cluster_oublie_actif.py", "configure_cluster_oublie_actif.ps1", "compare_cluster_oublie_actif_metrics.sql") }
    @{ Number = "103"; Folder = "scenario_103_migration_de_databricks_runtime"; Files = @("benchmark_migration_databricks_runtime.py", "configure_migration_databricks_runtime.ps1", "compare_migration_databricks_runtime_metrics.sql") }
    @{ Number = "104"; Folder = "scenario_104_benchmark_photon_lorsque_le_runtime_compute_le_permet"; Files = @("benchmark_benchmark_photon_lorsque_runtime_compu.py", "configure_benchmark_photon_lorsque_runtime_compu.ps1", "compare_benchmark_photon_lorsque_runtime_compu_metrics.sql") }
    @{ Number = "105"; Folder = "scenario_105_compute_dedie_au_job_vs_interactif"; Files = @("benchmark_compute_dedie_job_vs_interactif.py", "configure_compute_dedie_job_vs_interactif.ps1", "compare_compute_dedie_job_vs_interactif_metrics.sql") }
    @{ Number = "106"; Folder = "scenario_106_dag_bronze_to_silver_to_gold"; Files = @("resources/job_dag_bronze_to_silver_to_gold.yml", "tasks/dag_bronze_to_silver_to_gold_task.py", "validate_dag_bronze_to_silver_to_gold.sql") }
    @{ Number = "107"; Folder = "scenario_107_parametrer_date_et_environnement"; Files = @("resources/job_parametrer_date_environnement.yml", "tasks/parametrer_date_environnement_task.py", "validate_parametrer_date_environnement.sql") }
    @{ Number = "108"; Folder = "scenario_108_retry_utile_sur_erreur_transitoire"; Files = @("resources/job_retry_utile_erreur_transitoire.yml", "tasks/retry_utile_erreur_transitoire_task.py", "validate_retry_utile_erreur_transitoire.sql") }
    @{ Number = "109"; Folder = "scenario_109_mauvais_retry_sur_erreur_deterministe"; Files = @("resources/job_mauvais_retry_erreur_deterministe.yml", "tasks/mauvais_retry_erreur_deterministe_task.py", "validate_mauvais_retry_erreur_deterministe.sql") }
    @{ Number = "110"; Folder = "scenario_110_task_depasse_son_sla"; Files = @("resources/job_task_depasse_son_sla.yml", "tasks/task_depasse_son_sla_task.py", "validate_task_depasse_son_sla.sql") }
    @{ Number = "111"; Folder = "scenario_111_run_partiellement_reussi"; Files = @("resources/job_run_partiellement_reussi.yml", "tasks/run_partiellement_reussi_task.py", "validate_run_partiellement_reussi.sql") }
    @{ Number = "112"; Folder = "scenario_112_task_conditionnelle_de_publication"; Files = @("resources/job_task_conditionnelle_publication.yml", "tasks/task_conditionnelle_publication_task.py", "validate_task_conditionnelle_publication.sql") }
    @{ Number = "113"; Folder = "scenario_113_alerter_sur_echec_et_duree_anormale"; Files = @("resources/job_alerter_echec_duree_anormale.yml", "tasks/alerter_echec_duree_anormale_task.py", "validate_alerter_echec_duree_anormale.sql") }
    @{ Number = "114"; Folder = "scenario_114_overlapping_runs"; Files = @("resources/job_overlapping_runs.yml", "tasks/overlapping_runs_task.py", "validate_overlapping_runs.sql") }
    @{ Number = "115"; Folder = "scenario_115_job_rejoue_qui_double_gold"; Files = @("resources/job_job_rejoue_double_gold.yml", "tasks/job_rejoue_double_gold_task.py", "validate_job_rejoue_double_gold.sql") }
    @{ Number = "116"; Folder = "scenario_116_nouvelle_version_du_job_degrade_les_resultats"; Files = @("resources/job_nouvelle_version_job_degrade_resultats.yml", "tasks/nouvelle_version_job_degrade_resultats_task.py", "validate_nouvelle_version_job_degrade_resultats.sql") }
    @{ Number = "117"; Folder = "scenario_117_choix_du_compute_par_task"; Files = @("resources/job_choix_compute_task.yml", "tasks/choix_compute_task_task.py", "validate_choix_compute_task.sql") }
    @{ Number = "118"; Folder = "scenario_118_null_inattendus_dans_cles_metier"; Files = @("generate_null_inattendus_cles_metier_data.py", "check_null_inattendus_cles_metier.sql", "test_null_inattendus_cles_metier.py") }
    @{ Number = "119"; Folder = "scenario_119_cle_logique_non_unique"; Files = @("generate_cle_logique_non_unique_data.py", "check_cle_logique_non_unique.sql", "test_cle_logique_non_unique.py") }
    @{ Number = "120"; Folder = "scenario_120_reference_customer_absente"; Files = @("generate_reference_customer_absente_data.py", "check_reference_customer_absente.sql", "test_reference_customer_absente.py") }
    @{ Number = "121"; Folder = "scenario_121_statut_de_commande_inconnu"; Files = @("generate_statut_commande_inconnu_data.py", "check_statut_commande_inconnu.sql", "test_statut_commande_inconnu.py") }
    @{ Number = "122"; Folder = "scenario_122_contrainte_logique_violee"; Files = @("generate_contrainte_logique_violee_data.py", "check_contrainte_logique_violee.sql", "test_contrainte_logique_violee.py") }
    @{ Number = "123"; Folder = "scenario_123_pipeline_de_donnees_rejetees_exploitable"; Files = @("generate_pipeline_donnees_rejetees_exploitable_data.py", "check_pipeline_donnees_rejetees_exploitable.sql", "test_pipeline_donnees_rejetees_exploitable.py") }
    @{ Number = "124"; Folder = "scenario_124_tester_une_transformation_pyspark"; Files = @("generate_tester_transformation_pyspark_data.py", "check_tester_transformation_pyspark.sql", "test_tester_transformation_pyspark.py") }
    @{ Number = "125"; Folder = "scenario_125_assertions_sur_gold"; Files = @("generate_assertions_gold_data.py", "check_assertions_gold.sql", "test_assertions_gold.py") }
    @{ Number = "126"; Folder = "scenario_126_test_bronze_to_silver_to_gold"; Files = @("generate_test_bronze_to_silver_to_gold_data.py", "check_test_bronze_to_silver_to_gold.sql", "test_test_bronze_to_silver_to_gold.py") }
    @{ Number = "127"; Folder = "scenario_127_volume_correct_mais_kpi_faux_apres_changement"; Files = @("generate_volume_correct_mais_kpi_faux_changemen_data.py", "check_volume_correct_mais_kpi_faux_changemen.sql", "test_volume_correct_mais_kpi_faux_changemen.py") }
    @{ Number = "128"; Folder = "scenario_128_analyste_limite_a_gold"; Files = @("setup_analyste_limite_gold.sql", "validate_analyste_limite_gold_access.sql") }
    @{ Number = "129"; Folder = "scenario_129_data_engineer_bronze_silver_sans_admin_global"; Files = @("setup_data_engineer_bronze_silver_admin_glob.sql", "validate_data_engineer_bronze_silver_admin_glob_access.sql") }
    @{ Number = "130"; Folder = "scenario_130_diagnostiquer_un_acces_refuse"; Files = @("setup_diagnostiquer_acces_refuse.sql", "validate_diagnostiquer_acces_refuse_access.sql") }
    @{ Number = "131"; Folder = "scenario_131_permission_heritee_trop_large"; Files = @("setup_permission_heritee_trop_large.sql", "validate_permission_heritee_trop_large_access.sql") }
    @{ Number = "132"; Folder = "scenario_132_objet_possede_par_la_mauvaise_identite"; Files = @("setup_objet_possede_mauvaise_identite.sql", "validate_objet_possede_mauvaise_identite_access.sql") }
    @{ Number = "133"; Folder = "scenario_133_acces_gcs_refuse_via_uc"; Files = @("setup_acces_gcs_refuse_via_uc.sql", "reproduce_acces_gcs_refuse_via_uc.py", "validate_acces_gcs_refuse_via_uc_access.sql") }
    @{ Number = "134"; Folder = "scenario_134_identite_de_job_en_least_privilege"; Files = @("setup_identite_job_least_privilege.sql", "reproduce_identite_job_least_privilege.py", "validate_identite_job_least_privilege_access.sql") }
    @{ Number = "135"; Folder = "scenario_135_bronze_exposee_a_un_analyste"; Files = @("setup_bronze_exposee_analyste.sql", "validate_bronze_exposee_analyste_access.sql") }
    @{ Number = "136"; Folder = "scenario_136_tracer_un_kpi_gold_jusqu_aux_sources"; Files = @("setup_tracer_kpi_gold_jusqu_sources.sql", "validate_tracer_kpi_gold_jusqu_sources_access.sql") }
    @{ Number = "137"; Folder = "scenario_137_secret_manquant_ou_expose_au_mauvais_workload"; Files = @("setup_secret_manquant_ou_expose_mauvais_work.sql", "reproduce_secret_manquant_ou_expose_mauvais_work.py", "validate_secret_manquant_ou_expose_mauvais_work_access.sql") }
    @{ Number = "138"; Folder = "scenario_138_pipeline_operations_dashboard"; Files = @("create_pipeline_operations_dashboard_views.sql", "dashboard_pipeline_operations_dashboard_queries.sql") }
    @{ Number = "139"; Folder = "scenario_139_streaming_control_center"; Files = @("create_streaming_control_center_views.sql", "dashboard_streaming_control_center_queries.sql") }
    @{ Number = "140"; Folder = "scenario_140_spark_performance_cockpit"; Files = @("create_spark_performance_cockpit_views.sql", "dashboard_spark_performance_cockpit_queries.sql") }
    @{ Number = "141"; Folder = "scenario_141_delta_health_dashboard"; Files = @("create_delta_health_dashboard_views.sql", "dashboard_delta_health_dashboard_queries.sql") }
    @{ Number = "142"; Folder = "scenario_142_data_quality_dashboard"; Files = @("create_data_quality_dashboard_views.sql", "dashboard_data_quality_dashboard_queries.sql") }
    @{ Number = "143"; Folder = "scenario_143_security_governance_dashboard"; Files = @("create_security_governance_dashboard_views.sql", "dashboard_security_governance_dashboard_queries.sql") }
    @{ Number = "144"; Folder = "scenario_144_platform_capacity_dashboard"; Files = @("create_platform_capacity_dashboard_views.sql", "dashboard_platform_capacity_dashboard_queries.sql") }
    @{ Number = "145"; Folder = "scenario_145_executive_platform_cockpit"; Files = @("create_executive_platform_cockpit_views.sql", "dashboard_executive_platform_cockpit_queries.sql") }
    @{ Number = "146"; Folder = "scenario_146_finops_cout_par_job_workload"; Files = @("finops_finops_cout_job_workload.sql", "benchmark_finops_cout_job_workload.py", "validate_finops_cout_job_workload_cost.sql") }
    @{ Number = "147"; Folder = "scenario_147_finops_cout_quotidien_x2_sans_hausse_de_volume"; Files = @("finops_finops_cout_quotidien_x2_hausse_volume.sql", "benchmark_finops_cout_quotidien_x2_hausse_volume.py", "validate_finops_cout_quotidien_x2_hausse_volume_cost.sql") }
    @{ Number = "148"; Folder = "scenario_148_cluster_oublie_ou_surdimensionne"; Files = @("finops_cluster_oublie_ou_surdimensionne.sql", "benchmark_cluster_oublie_ou_surdimensionne.py", "validate_cluster_oublie_ou_surdimensionne_cost.sql") }
    @{ Number = "149"; Folder = "scenario_149_merge_scans_couteux"; Files = @("finops_merge_scans_couteux.sql", "benchmark_merge_scans_couteux.py", "validate_merge_scans_couteux_cost.sql") }
    @{ Number = "150"; Folder = "scenario_150_performance_vs_cout"; Files = @("finops_performance_vs_cout.sql", "benchmark_performance_vs_cout.py", "validate_performance_vs_cout_cost.sql") }
    @{ Number = "151"; Folder = "scenario_151_configuration_dev_test_prod"; Files = @("terraform/configuration_dev_test_prod.tf", "bundle/configuration_dev_test_prod.yml", "tests/test_configuration_dev_test_prod.py", "validate_configuration_dev_test_prod.ps1") }
    @{ Number = "152"; Folder = "scenario_152_secret_manquant_au_deploiement"; Files = @("bundle/secret_manquant_deploiement.yml", "tests/test_secret_manquant_deploiement.py", "validate_secret_manquant_deploiement.ps1") }
    @{ Number = "153"; Folder = "scenario_153_pipeline_bloquee_par_test"; Files = @("tests/test_pipeline_bloquee_test.py", "validate_pipeline_bloquee_test.ps1") }
    @{ Number = "154"; Folder = "scenario_154_ressource_modifiee_manuellement"; Files = @("terraform/ressource_modifiee_manuellement.tf", "validate_ressource_modifiee_manuellement.ps1") }
    @{ Number = "155"; Folder = "scenario_155_changement_destructif_inattendu"; Files = @("terraform/changement_destructif_inattendu.tf", "validate_changement_destructif_inattendu.ps1") }
    @{ Number = "156"; Folder = "scenario_156_automation_asset_bundle_invalide"; Files = @("bundle/automation_asset_bundle_invalide.yml", "validate_automation_asset_bundle_invalide.ps1") }
    @{ Number = "157"; Folder = "scenario_157_job_mis_a_jour_mais_dependance_absente"; Files = @("bundle/job_mis_jour_mais_dependance_absente.yml", "validate_job_mis_jour_mais_dependance_absente.ps1") }
    @{ Number = "158"; Folder = "scenario_158_dev_to_test_to_prod_simule"; Files = @("terraform/dev_to_test_to_prod_simule.tf", "bundle/dev_to_test_to_prod_simule.yml", "tests/test_dev_to_test_to_prod_simule.py", "validate_dev_to_test_to_prod_simule.ps1") }
    @{ Number = "159"; Folder = "scenario_159_retour_a_la_version_precedente"; Files = @("bundle/retour_version_precedente.yml", "tests/test_retour_version_precedente.py", "validate_retour_version_precedente.ps1") }
    @{ Number = "160"; Folder = "scenario_160_service_principal_de_deploiement"; Files = @("terraform/service_principal_deploiement.tf", "bundle/service_principal_deploiement.yml", "tests/test_service_principal_deploiement.py", "validate_service_principal_deploiement.ps1") }
    @{ Number = "161"; Folder = "scenario_161_sla_passe_de_2_h_a_10_min"; Files = @("simulate_sla_passe_2_h_10_min.py", "measure_sla_passe_2_h_10_min.sql", "validate_sla_passe_2_h_10_min.sql") }
    @{ Number = "162"; Folder = "scenario_162_volume_quotidien_x10"; Files = @("simulate_volume_quotidien_x10.py", "measure_volume_quotidien_x10.sql", "validate_volume_quotidien_x10.sql") }
    @{ Number = "163"; Folder = "scenario_163_simuler_une_architecture_5_to_jour_sans_generer_5_to"; Files = @("simulate_simuler_architecture_5_to_jour_generer.py", "measure_simuler_architecture_5_to_jour_generer.sql", "validate_simuler_architecture_5_to_jour_generer.sql") }
    @{ Number = "164"; Folder = "scenario_164_backlog_depuis_3_heures_en_production_simulee"; Files = @("simulate_backlog_depuis_3_heures_production_sim.py", "measure_backlog_depuis_3_heures_production_sim.sql", "validate_backlog_depuis_3_heures_production_sim.sql") }
    @{ Number = "165"; Folder = "scenario_165_merge_passe_de_5_a_40_minutes"; Files = @("simulate_merge_passe_5_40_minutes.py", "measure_merge_passe_5_40_minutes.sql", "validate_merge_passe_5_40_minutes.sql") }
    @{ Number = "166"; Folder = "scenario_166_integrer_un_domaine_refunds_sans_casser_la_plateforme"; Files = @("simulate_integrer_domaine_refunds_casser_platef.py", "measure_integrer_domaine_refunds_casser_platef.sql", "validate_integrer_domaine_refunds_casser_platef.sql") }
    @{ Number = "167"; Folder = "scenario_167_20_equipes_utilisent_la_plateforme"; Files = @("simulate_20_equipes_utilisent_plateforme.py", "measure_20_equipes_utilisent_plateforme.sql", "validate_20_equipes_utilisent_plateforme.sql") }
    @{ Number = "168"; Folder = "scenario_168_le_schema_source_change_chaque_semaine"; Files = @("simulate_schema_source_change_chaque_semaine.py", "measure_schema_source_change_chaque_semaine.sql", "validate_schema_source_change_chaque_semaine.sql") }
    @{ Number = "169"; Folder = "scenario_169_incident_de_permissions_bloque_le_pipeline"; Files = @("simulate_incident_permissions_bloque_pipeline.py", "measure_incident_permissions_bloque_pipeline.sql", "validate_incident_permissions_bloque_pipeline.sql") }
    @{ Number = "170"; Folder = "scenario_170_tous_les_jobs_sont_verts_mais_le_kpi_gold_est_faux"; Files = @("simulate_tous_jobs_verts_mais_kpi_gold_faux.py", "measure_tous_jobs_verts_mais_kpi_gold_faux.sql", "validate_tous_jobs_verts_mais_kpi_gold_faux.sql") }
    @{ Number = "171"; Folder = "scenario_171_perte_checkpoint_replay_massif"; Files = @("simulate_perte_checkpoint_replay_massif.py", "measure_perte_checkpoint_replay_massif.sql", "validate_perte_checkpoint_replay_massif.sql") }
    @{ Number = "172"; Folder = "scenario_172_modification_compute_reseau_runtime_puis_rollback_urgent"; Files = @("simulate_modification_compute_reseau_runtime_pu.py", "measure_modification_compute_reseau_runtime_pu.sql", "validate_modification_compute_reseau_runtime_pu.sql") }
    @{ Number = "173"; Folder = "scenario_173_modele_classique_mlflow_tracking"; Files = @("prepare_training_data.py", "train_mlflow_model.py", "compare_mlflow_runs.py") }
    @{ Number = "174"; Folder = "scenario_174_comparer_runs_et_hyperparametres"; Files = @("train_hyperparameter_runs.py", "compare_hyperparameters.py", "select_best_run.py") }
    @{ Number = "175"; Folder = "scenario_175_creer_et_reutiliser_des_features_gouvernees"; Files = @("build_governed_features.py", "register_feature_table.py", "reuse_features.py") }
    @{ Number = "176"; Folder = "scenario_176_promotion_version_batch_inference"; Files = @("train_model_version.py", "promote_model_version.py", "batch_inference.py", "validate_predictions.sql") }
    @{ Number = "177"; Folder = "scenario_177_drift_simple_decision_de_retraining"; Files = @("generate_drift_data.py", "detect_data_drift.py", "evaluate_retraining_decision.py") }
    @{ Number = "178"; Folder = "scenario_178_sla_gold_de_2_h_a_10_min"; Files = @("simulate_sla_gold_2_h_10_min.py", "measure_sla_gold_2_h_10_min.sql", "validate_sla_gold_2_h_10_min.sql") }
    @{ Number = "179"; Folder = "scenario_179_volume_quotidien_multiplie_par_10"; Files = @("simulate_volume_quotidien_multiplie_10.py", "measure_volume_quotidien_multiplie_10.sql", "validate_volume_quotidien_multiplie_10.sql") }
    @{ Number = "180"; Folder = "scenario_180_preparer_5_to_jour_sans_posseder_5_to"; Files = @("simulate_preparer_5_to_jour_posseder_5_to.py", "measure_preparer_5_to_jour_posseder_5_to.sql", "validate_preparer_5_to_jour_posseder_5_to.sql") }
    @{ Number = "181"; Folder = "scenario_181_un_workload_secondaire_degrade_le_gold_critique"; Files = @("simulate_workload_secondaire_degrade_gold_criti.py", "measure_workload_secondaire_degrade_gold_criti.sql", "validate_workload_secondaire_degrade_gold_criti.sql") }
    @{ Number = "182"; Folder = "scenario_182_20_equipes_publient_des_pipelines_sur_la_plateforme"; Files = @("simulate_20_equipes_publient_pipelines_platefor.py", "measure_20_equipes_publient_pipelines_platefor.sql", "validate_20_equipes_publient_pipelines_platefor.sql") }
    @{ Number = "183"; Folder = "scenario_183_nouveau_domaine_livre_en_48_h"; Files = @("simulate_nouveau_domaine_livre_48_h.py", "measure_nouveau_domaine_livre_48_h.sql", "validate_nouveau_domaine_livre_48_h.sql") }
    @{ Number = "184"; Folder = "scenario_184_le_schema_source_change_chaque_semaine"; Files = @("simulate_schema_source_change_chaque_semaine.py", "measure_schema_source_change_chaque_semaine.sql", "validate_schema_source_change_chaque_semaine.sql") }
    @{ Number = "185"; Folder = "scenario_185_une_optimisation_globale_casse_trois_jobs"; Files = @("simulate_optimisation_globale_casse_trois_jobs.py", "measure_optimisation_globale_casse_trois_jobs.sql", "validate_optimisation_globale_casse_trois_jobs.sql") }
    @{ Number = "186"; Folder = "scenario_186_upgrade_databricks_runtime_avec_regression"; Files = @("simulate_upgrade_databricks_runtime_regression.py", "measure_upgrade_databricks_runtime_regression.sql", "validate_upgrade_databricks_runtime_regression.sql") }
    @{ Number = "187"; Folder = "scenario_187_modification_infrastructure_et_rollback_urgent"; Files = @("simulate_modification_infrastructure_rollback_u.py", "measure_modification_infrastructure_rollback_u.sql", "validate_modification_infrastructure_rollback_u.sql") }
    @{ Number = "188"; Folder = "scenario_188_dependance_externe_lente_bloque_tout_le_dag"; Files = @("simulate_dependance_externe_lente_bloque_tout_d.py", "measure_dependance_externe_lente_bloque_tout_d.sql", "validate_dependance_externe_lente_bloque_tout_d.sql") }
    @{ Number = "189"; Folder = "scenario_189_replay_de_30_jours_sans_perturber_le_quotidien"; Files = @("simulate_replay_30_jours_perturber_quotidien.py", "measure_replay_30_jours_perturber_quotidien.sql", "validate_replay_30_jours_perturber_quotidien.sql") }
    @{ Number = "190"; Folder = "scenario_190_definir_et_tester_rto_rpo_de_la_pipeline"; Files = @("simulate_definir_tester_rto_rpo_pipeline.py", "measure_definir_tester_rto_rpo_pipeline.sql", "validate_definir_tester_rto_rpo_pipeline.sql") }
    @{ Number = "191"; Folder = "scenario_191_batch_ou_streaming_pour_un_nouveau_besoin_5_min"; Files = @("simulate_batch_ou_streaming_nouveau_besoin_5_mi.py", "measure_batch_ou_streaming_nouveau_besoin_5_mi.sql", "validate_batch_ou_streaming_nouveau_besoin_5_mi.sql") }
    @{ Number = "192"; Folder = "scenario_192_construire_un_slo_plateforme_multi_pipelines"; Files = @("simulate_construire_slo_plateforme_multi_pipeli.py", "measure_construire_slo_plateforme_multi_pipeli.sql", "validate_construire_slo_plateforme_multi_pipeli.sql") }
    @{ Number = "193"; Folder = "scenario_193_plan_physique_change_et_double_le_runtime"; Files = @("generate_plan_physique_change_double_runtime_dataset.py", "benchmark_plan_physique_change_double_runtime.py", "optimize_plan_physique_change_double_runtime.py") }
    @{ Number = "194"; Folder = "scenario_194_broadcast_borderline_et_executor_sous_pression"; Files = @("generate_broadcast_borderline_executor_sous_pre_dataset.py", "benchmark_broadcast_borderline_executor_sous_pre.py", "optimize_broadcast_borderline_executor_sous_pre.py") }
    @{ Number = "195"; Folder = "scenario_195_shuffle_hash_join_reellement_meilleur_que_sort_merge"; Files = @("generate_shuffle_hash_join_reellement_meilleur_dataset.py", "benchmark_shuffle_hash_join_reellement_meilleur.py", "optimize_shuffle_hash_join_reellement_meilleur.py") }
    @{ Number = "196"; Folder = "scenario_196_skew_multi_cles_masque_par_la_moyenne"; Files = @("generate_skew_multi_cles_masque_moyenne_dataset.py", "benchmark_skew_multi_cles_masque_moyenne.py", "optimize_skew_multi_cles_masque_moyenne.py") }
    @{ Number = "197"; Folder = "scenario_197_skew_apparait_apres_une_agregation_intermediaire"; Files = @("generate_skew_apparait_agregation_intermediaire_dataset.py", "benchmark_skew_apparait_agregation_intermediaire.py", "optimize_skew_apparait_agregation_intermediaire.py") }
    @{ Number = "198"; Folder = "scenario_198_deux_shuffles_evitables_dans_la_meme_transformation"; Files = @("generate_deux_shuffles_evitables_transformation_dataset.py", "benchmark_deux_shuffles_evitables_transformation.py", "optimize_deux_shuffles_evitables_transformation.py") }
    @{ Number = "199"; Folder = "scenario_199_une_partition_unique_de_plusieurs_go"; Files = @("generate_partition_unique_plusieurs_go_dataset.py", "benchmark_partition_unique_plusieurs_go.py", "optimize_partition_unique_plusieurs_go.py") }
    @{ Number = "200"; Folder = "scenario_200_2000_partitions_pour_un_dataset_minuscule"; Files = @("generate_2000_partitions_dataset_minuscule_dataset.py", "benchmark_2000_partitions_dataset_minuscule.py", "optimize_2000_partitions_dataset_minuscule.py") }
    @{ Number = "201"; Folder = "scenario_201_coalesce_cree_un_nouveau_hotspot"; Files = @("generate_coalesce_cree_nouveau_hotspot_dataset.py", "benchmark_coalesce_cree_nouveau_hotspot.py", "optimize_coalesce_cree_nouveau_hotspot.py") }
    @{ Number = "202"; Folder = "scenario_202_spill_disque_massif_sans_oom"; Files = @("generate_spill_disque_massif_oom_dataset.py", "benchmark_spill_disque_massif_oom.py", "optimize_spill_disque_massif_oom.py") }
    @{ Number = "203"; Folder = "scenario_203_executor_oom_intermittent_seulement_sur_certaines_cles"; Files = @("generate_executor_oom_intermittent_seulement_ce_dataset.py", "benchmark_executor_oom_intermittent_seulement_ce.py", "optimize_executor_oom_intermittent_seulement_ce.py") }
    @{ Number = "204"; Folder = "scenario_204_driver_oom_cause_par_metadata_files"; Files = @("generate_driver_oom_cause_metadata_files_dataset.py", "benchmark_driver_oom_cause_metadata_files.py", "optimize_driver_oom_cause_metadata_files.py") }
    @{ Number = "205"; Folder = "scenario_205_python_udf_domine_le_temps_cpu"; Files = @("generate_python_udf_domine_temps_cpu_dataset.py", "benchmark_python_udf_domine_temps_cpu.py", "optimize_python_udf_domine_temps_cpu.py") }
    @{ Number = "206"; Folder = "scenario_206_arrow_active_mais_aucun_gain_mesurable"; Files = @("generate_arrow_active_mais_aucun_gain_mesurable_dataset.py", "benchmark_arrow_active_mais_aucun_gain_mesurable.py", "optimize_arrow_active_mais_aucun_gain_mesurable.py") }
    @{ Number = "207"; Folder = "scenario_207_cache_utile_en_notebook_mais_mauvais_en_job"; Files = @("generate_cache_utile_notebook_mais_mauvais_job_dataset.py", "benchmark_cache_utile_notebook_mais_mauvais_job.py", "optimize_cache_utile_notebook_mais_mauvais_job.py") }
    @{ Number = "208"; Folder = "scenario_208_distinct_explose_apres_mauvaise_projection"; Files = @("generate_distinct_explose_mauvaise_projection_dataset.py", "benchmark_distinct_explose_mauvaise_projection.py", "optimize_distinct_explose_mauvaise_projection.py") }
    @{ Number = "209"; Folder = "scenario_209_resultat_intermediaire_20x_plus_gros_que_l_entree"; Files = @("generate_resultat_intermediaire_20x_gros_entree_dataset.py", "benchmark_resultat_intermediaire_20x_gros_entree.py", "optimize_resultat_intermediaire_20x_gros_entree.py") }
    @{ Number = "210"; Folder = "scenario_210_order_by_global_cache_dans_un_pipeline"; Files = @("generate_order_by_global_cache_pipeline_dataset.py", "benchmark_order_by_global_cache_pipeline.py", "optimize_order_by_global_cache_pipeline.py") }
    @{ Number = "211"; Folder = "scenario_211_fenetre_non_partitionnee_ecrase_le_parallelisme"; Files = @("generate_fenetre_non_partitionnee_ecrase_parall_dataset.py", "benchmark_fenetre_non_partitionnee_ecrase_parall.py", "optimize_fenetre_non_partitionnee_ecrase_parall.py") }
    @{ Number = "212"; Folder = "scenario_212_dynamic_partition_pruning_absent_malgre_une_dimension_fi"; Files = @("generate_dynamic_partition_pruning_absent_malgr_dataset.py", "benchmark_dynamic_partition_pruning_absent_malgr.py", "optimize_dynamic_partition_pruning_absent_malgr.py") }
    @{ Number = "213"; Folder = "scenario_213_input_rate_depasse_processing_rate_pendant_45_min"; Files = @("generate_input_rate_depasse_processing_rate_pen_data.py", "stream_input_rate_depasse_processing_rate_pen.py", "monitor_input_rate_depasse_processing_rate_pen.sql") }
    @{ Number = "214"; Folder = "scenario_214_state_croit_sans_borne_malgre_watermark"; Files = @("generate_state_croit_borne_malgre_watermark_data.py", "stream_state_croit_borne_malgre_watermark.py", "monitor_state_croit_borne_malgre_watermark.sql") }
    @{ Number = "215"; Folder = "scenario_215_late_data_valide_rejetee_par_watermark_trop_agressif"; Files = @("generate_late_data_valide_rejetee_watermark_tro_data.py", "stream_late_data_valide_rejetee_watermark_tro.py", "monitor_late_data_valide_rejetee_watermark_tro.sql") }
    @{ Number = "216"; Folder = "scenario_216_correction_j_2_d_une_agregation_streaming"; Files = @("generate_correction_j_2_agregation_streaming_data.py", "stream_correction_j_2_agregation_streaming.py", "validate_correction_j_2_agregation_streaming.sql") }
    @{ Number = "217"; Folder = "scenario_217_checkpoint_supprime_puis_source_rejouee"; Files = @("generate_checkpoint_supprime_puis_source_rejoue_data.py", "stream_checkpoint_supprime_puis_source_rejoue.py", "monitor_checkpoint_supprime_puis_source_rejoue.sql") }
    @{ Number = "218"; Folder = "scenario_218_deux_streams_partagent_accidentellement_un_checkpoint"; Files = @("generate_deux_streams_partagent_accidentellemen_data.py", "stream_deux_streams_partagent_accidentellemen.py", "monitor_deux_streams_partagent_accidentellemen.sql") }
    @{ Number = "219"; Folder = "scenario_219_evolution_de_schema_pendant_un_stream_actif"; Files = @("generate_evolution_schema_pendant_stream_actif_data.py", "stream_evolution_schema_pendant_stream_actif.py", "validate_evolution_schema_pendant_stream_actif.sql") }
    @{ Number = "220"; Folder = "scenario_220_foreachbatch_reussi_a_moitie_puis_retry"; Files = @("generate_foreachbatch_reussi_moitie_puis_retry_data.py", "stream_foreachbatch_reussi_moitie_puis_retry.py", "validate_foreachbatch_reussi_moitie_puis_retry.sql") }
    @{ Number = "221"; Folder = "scenario_221_gcs_delta_sink_temporairement_indisponible"; Files = @("generate_gcs_delta_sink_temporairement_indispon_data.py", "stream_gcs_delta_sink_temporairement_indispon.py", "validate_gcs_delta_sink_temporairement_indispon.sql") }
    @{ Number = "222"; Folder = "scenario_222_trigger_trop_frequent_coute_plus_sans_gagner_de_fraicheu"; Files = @("generate_trigger_trop_frequent_coute_gagner_fra_data.py", "stream_trigger_trop_frequent_coute_gagner_fra.py", "monitor_trigger_trop_frequent_coute_gagner_fra.sql") }
    @{ Number = "223"; Folder = "scenario_223_un_seul_micro_batch_pathologique_cree_1_h_de_retard"; Files = @("generate_seul_micro_batch_pathologique_cree_1_h_data.py", "stream_seul_micro_batch_pathologique_cree_1_h.py", "monitor_seul_micro_batch_pathologique_cree_1_h.sql") }
    @{ Number = "224"; Folder = "scenario_224_cle_de_deduplication_trop_large_gonfle_le_state"; Files = @("generate_cle_deduplication_trop_large_gonfle_st_data.py", "stream_cle_deduplication_trop_large_gonfle_st.py", "monitor_cle_deduplication_trop_large_gonfle_st.sql") }
    @{ Number = "225"; Folder = "scenario_225_fenetres_chevauchantes_multiplient_le_state"; Files = @("generate_fenetres_chevauchantes_multiplient_sta_data.py", "stream_fenetres_chevauchantes_multiplient_sta.py", "monitor_fenetres_chevauchantes_multiplient_sta.sql") }
    @{ Number = "226"; Folder = "scenario_226_fenetres_temporelles_asymetriques_orders_payments"; Files = @("generate_fenetres_temporelles_asymetriques_orde_data.py", "stream_fenetres_temporelles_asymetriques_orde.py", "monitor_fenetres_temporelles_asymetriques_orde.sql") }
    @{ Number = "227"; Folder = "scenario_227_restart_toutes_les_nuits_masque_une_fuite_d_etat"; Files = @("generate_restart_toutes_nuits_masque_fuite_etat_data.py", "stream_restart_toutes_nuits_masque_fuite_etat.py", "monitor_restart_toutes_nuits_masque_fuite_etat.sql") }
    @{ Number = "228"; Folder = "scenario_228_streaming_ecrit_trop_de_petits_fichiers_delta"; Files = @("generate_streaming_ecrit_trop_petits_fichiers_d_data.py", "stream_streaming_ecrit_trop_petits_fichiers_d.py", "validate_streaming_ecrit_trop_petits_fichiers_d.sql") }
    @{ Number = "229"; Folder = "scenario_229_deux_consommateurs_streaming_se_disputent_le_compute"; Files = @("generate_deux_consommateurs_streaming_se_disput_data.py", "stream_deux_consommateurs_streaming_se_disput.py", "validate_deux_consommateurs_streaming_se_disput.sql") }
    @{ Number = "230"; Folder = "scenario_230_millions_de_petits_objets_gcs_ralentissent_la_decouverte"; Files = @("generate_millions_petits_objets_gcs_ralentissen_data.py", "stream_millions_petits_objets_gcs_ralentissen.py", "validate_millions_petits_objets_gcs_ralentissen.sql") }
    @{ Number = "231"; Folder = "scenario_231_backlog_critique_sans_job_failure"; Files = @("generate_backlog_critique_job_failure_data.py", "stream_backlog_critique_job_failure.py", "monitor_backlog_critique_job_failure.sql") }
    @{ Number = "232"; Folder = "scenario_232_side_effect_externe_casse_l_idempotence"; Files = @("generate_side_effect_externe_casse_idempotence_data.py", "stream_side_effect_externe_casse_idempotence.py", "validate_side_effect_externe_casse_idempotence.sql") }
    @{ Number = "233"; Folder = "scenario_233_deux_merge_concurrents_sur_la_meme_table_silver"; Files = @("prepare_deux_merge_concurrents_table_silver.sql", "benchmark_deux_merge_concurrents_table_silver.py", "apply_deux_merge_concurrents_table_silver.sql", "validate_deux_merge_concurrents_table_silver.sql") }
    @{ Number = "234"; Folder = "scenario_234_update_massif_concurrence_un_stream"; Files = @("prepare_update_massif_concurrence_stream.sql", "benchmark_update_massif_concurrence_stream.py", "apply_update_massif_concurrence_stream.sql", "validate_update_massif_concurrence_stream.sql") }
    @{ Number = "235"; Folder = "scenario_235_merge_passe_de_5_a_40_min_apres_croissance"; Files = @("prepare_merge_passe_5_40_min_croissance.sql", "benchmark_merge_passe_5_40_min_croissance.py", "apply_merge_passe_5_40_min_croissance.sql", "validate_merge_passe_5_40_min_croissance.sql") }
    @{ Number = "236"; Folder = "scenario_236_clustering_choisi_sur_une_mauvaise_colonne"; Files = @("prepare_clustering_choisi_mauvaise_colonne.sql", "benchmark_clustering_choisi_mauvaise_colonne.py", "apply_clustering_choisi_mauvaise_colonne.sql", "validate_clustering_choisi_mauvaise_colonne.sql") }
    @{ Number = "237"; Folder = "scenario_237_data_skipping_inefficace_malgre_filtre_selectif"; Files = @("prepare_data_skipping_inefficace_malgre_filtre.sql", "benchmark_data_skipping_inefficace_malgre_filtre.py", "apply_data_skipping_inefficace_malgre_filtre.sql", "validate_data_skipping_inefficace_malgre_filtre.sql") }
    @{ Number = "238"; Folder = "scenario_238_optimize_ameliore_les_lectures_mais_degrade_le_budget"; Files = @("prepare_optimize_ameliore_lectures_mais_degrad.sql", "benchmark_optimize_ameliore_lectures_mais_degrad.py", "apply_optimize_ameliore_lectures_mais_degrad.sql", "validate_optimize_ameliore_lectures_mais_degrad.sql") }
    @{ Number = "239"; Folder = "scenario_239_maintenance_manuelle_et_predictive_optimization_se_cheva"; Files = @("prepare_maintenance_manuelle_predictive_optimi.sql", "apply_maintenance_manuelle_predictive_optimi.sql", "validate_maintenance_manuelle_predictive_optimi.sql") }
    @{ Number = "240"; Folder = "scenario_240_delete_rapide_mais_lectures_ensuite_plus_couteuses"; Files = @("prepare_delete_rapide_mais_lectures_ensuite_co.sql", "benchmark_delete_rapide_mais_lectures_ensuite_co.py", "apply_delete_rapide_mais_lectures_ensuite_co.sql", "validate_delete_rapide_mais_lectures_ensuite_co.sql") }
    @{ Number = "241"; Folder = "scenario_241_consumer_cdf_en_retard_apres_vacuum_retention"; Files = @("prepare_consumer_cdf_retard_vacuum_retention.sql", "benchmark_consumer_cdf_retard_vacuum_retention.py", "apply_consumer_cdf_retard_vacuum_retention.sql", "validate_consumer_cdf_retard_vacuum_retention.sql") }
    @{ Number = "242"; Folder = "scenario_242_restore_corrige_gold_mais_casse_un_consumer_incremental"; Files = @("prepare_restore_corrige_gold_mais_casse_consum.sql", "apply_restore_corrige_gold_mais_casse_consum.sql", "validate_restore_corrige_gold_mais_casse_consum.sql") }
    @{ Number = "243"; Folder = "scenario_243_renommage_metier_compatible_en_bronze_mais_cassant_en_go"; Files = @("prepare_renommage_metier_compatible_bronze_mai.sql", "apply_renommage_metier_compatible_bronze_mai.sql", "validate_renommage_metier_compatible_bronze_mai.sql") }
    @{ Number = "244"; Folder = "scenario_244_vacuum_agressif_supprime_la_capacite_de_rollback"; Files = @("prepare_vacuum_agressif_supprime_capacite_roll.sql", "benchmark_vacuum_agressif_supprime_capacite_roll.py", "apply_vacuum_agressif_supprime_capacite_roll.sql", "validate_vacuum_agressif_supprime_capacite_roll.sql") }
    @{ Number = "245"; Folder = "scenario_245_external_table_supprime_des_attentes_de_lifecycle"; Files = @("prepare_external_table_supprime_attentes_lifec.sql", "apply_external_table_supprime_attentes_lifec.sql", "validate_external_table_supprime_attentes_lifec.sql") }
    @{ Number = "246"; Folder = "scenario_246_trouver_le_commit_qui_a_introduit_la_corruption"; Files = @("prepare_trouver_commit_introduit_corruption.sql", "apply_trouver_commit_introduit_corruption.sql", "validate_trouver_commit_introduit_corruption.sql") }
    @{ Number = "247"; Folder = "scenario_247_stockage_gcs_augmente_alors_que_la_table_change_peu"; Files = @("prepare_stockage_gcs_augmente_alors_table_chan.sql", "benchmark_stockage_gcs_augmente_alors_table_chan.py", "apply_stockage_gcs_augmente_alors_table_chan.sql", "validate_stockage_gcs_augmente_alors_table_chan.sql") }
    @{ Number = "248"; Folder = "scenario_248_dag_partiellement_parallele_mais_serialise_par_erreur"; Files = @("resources/job_dag_partiellement_parallele_mais_seria.yml", "tasks/dag_partiellement_parallele_mais_seria_task.py", "validate_dag_partiellement_parallele_mais_seria.sql") }
    @{ Number = "249"; Folder = "scenario_249_retry_global_masque_une_task_non_idempotente"; Files = @("resources/job_retry_global_masque_task_non_idempoten.yml", "tasks/retry_global_masque_task_non_idempoten_task.py", "validate_retry_global_masque_task_non_idempoten.sql") }
    @{ Number = "250"; Folder = "scenario_250_reparer_seulement_la_branche_echouee"; Files = @("resources/job_reparer_seulement_branche_echouee.yml", "tasks/reparer_seulement_branche_echouee_task.py", "validate_reparer_seulement_branche_echouee.sql") }
    @{ Number = "251"; Folder = "scenario_251_overlapping_runs_ecrivent_la_meme_partition_gold"; Files = @("resources/job_overlapping_runs_ecrivent_partition_go.yml", "tasks/overlapping_runs_ecrivent_partition_go_task.py", "validate_overlapping_runs_ecrivent_partition_go.sql") }
    @{ Number = "252"; Folder = "scenario_252_sla_rate_a_cause_de_queue_time_pas_du_code"; Files = @("resources/job_sla_rate_cause_queue_time_code.yml", "tasks/sla_rate_cause_queue_time_code_task.py", "validate_sla_rate_cause_queue_time_code.sql") }
    @{ Number = "253"; Folder = "scenario_253_timeout_trop_court_provoque_des_retries_couteux"; Files = @("resources/job_timeout_trop_court_provoque_retries_co.yml", "tasks/timeout_trop_court_provoque_retries_co_task.py", "validate_timeout_trop_court_provoque_retries_co.sql") }
    @{ Number = "254"; Folder = "scenario_254_mauvais_parametre_date_relit_tout_l_historique"; Files = @("resources/job_mauvais_parametre_date_relit_tout_hist.yml", "tasks/mauvais_parametre_date_relit_tout_hist_task.py", "validate_mauvais_parametre_date_relit_tout_hist.sql") }
    @{ Number = "255"; Folder = "scenario_255_100_alertes_pour_un_seul_incident"; Files = @("resources/job_100_alertes_seul_incident.yml", "tasks/100_alertes_seul_incident_task.py", "validate_100_alertes_seul_incident.sql") }
    @{ Number = "256"; Folder = "scenario_256_job_prod_modifie_manuellement_malgre_le_bundle"; Files = @("resources/job_job_prod_modifie_manuellement_malgre_b.yml", "tasks/job_prod_modifie_manuellement_malgre_b_task.py", "validate_job_prod_modifie_manuellement_malgre_b.sql") }
    @{ Number = "257"; Folder = "scenario_257_gold_demarre_avant_la_vraie_fin_de_silver"; Files = @("resources/job_gold_demarre_vraie_fin_silver.yml", "tasks/gold_demarre_vraie_fin_silver_task.py", "validate_gold_demarre_vraie_fin_silver.sql") }
    @{ Number = "258"; Folder = "scenario_258_cockpit_unique_sante_cout_sla_qualite"; Files = @("create_cockpit_unique_sante_cout_sla_qualite_views.sql", "dashboard_cockpit_unique_sante_cout_sla_qualite_queries.sql") }
    @{ Number = "259"; Folder = "scenario_259_detecter_une_regression_progressive_sur_30_runs"; Files = @("create_detecter_regression_progressive_30_run_views.sql", "dashboard_detecter_regression_progressive_30_run_queries.sql") }
    @{ Number = "260"; Folder = "scenario_260_cpu_faible_mais_cout_compute_eleve"; Files = @("create_cpu_faible_mais_cout_compute_eleve_views.sql", "dashboard_cpu_faible_mais_cout_compute_eleve_queries.sql") }
    @{ Number = "261"; Folder = "scenario_261_vue_multi_stream_backlog_fraicheur"; Files = @("create_vue_multi_stream_backlog_fraicheur_views.sql", "dashboard_vue_multi_stream_backlog_fraicheur_queries.sql") }
    @{ Number = "262"; Folder = "scenario_262_top_requetes_sql_couteuses_et_lentes"; Files = @("create_top_requetes_sql_couteuses_lentes_views.sql", "dashboard_top_requetes_sql_couteuses_lentes_queries.sql") }
    @{ Number = "263"; Folder = "scenario_263_table_health_se_degrade_avant_le_sla"; Files = @("create_table_health_se_degrade_sla_views.sql", "dashboard_table_health_se_degrade_sla_queries.sql") }
    @{ Number = "264"; Folder = "scenario_264_changement_de_permissions_inattendu"; Files = @("create_changement_permissions_inattendu_views.sql", "dashboard_changement_permissions_inattendu_queries.sql") }
    @{ Number = "265"; Folder = "scenario_265_impact_analysis_avant_modification_silver"; Files = @("create_impact_analysis_modification_silver_views.sql", "dashboard_impact_analysis_modification_silver_queries.sql") }
    @{ Number = "266"; Folder = "scenario_266_seuil_fixe_genere_des_faux_positifs_chaque_matin"; Files = @("create_seuil_fixe_genere_faux_positifs_chaque_views.sql", "dashboard_seuil_fixe_genere_faux_positifs_chaque_queries.sql") }
    @{ Number = "267"; Folder = "scenario_267_reconstruire_un_incident_uniquement_depuis_les_traces"; Files = @("create_reconstruire_incident_uniquement_depui_views.sql", "dashboard_reconstruire_incident_uniquement_depui_queries.sql") }
    @{ Number = "268"; Folder = "scenario_268_cout_databricks_double_sans_hausse_de_volume"; Files = @("finops_cout_databricks_double_hausse_volume.sql", "benchmark_cout_databricks_double_hausse_volume.py", "validate_cout_databricks_double_hausse_volume_cost.sql") }
    @{ Number = "269"; Folder = "scenario_269_cout_par_million_de_commandes_traite"; Files = @("finops_cout_million_commandes_traite.sql", "benchmark_cout_million_commandes_traite.py", "validate_cout_million_commandes_traite_cost.sql") }
    @{ Number = "270"; Folder = "scenario_270_impossible_d_attribuer_30_du_cout"; Files = @("finops_impossible_attribuer_30_cout.sql", "validate_impossible_attribuer_30_cout_cost.sql") }
    @{ Number = "271"; Folder = "scenario_271_job_30_plus_rapide_mais_2x_plus_cher"; Files = @("finops_job_30_rapide_mais_2x_cher.sql", "benchmark_job_30_rapide_mais_2x_cher.py", "validate_job_30_rapide_mais_2x_cher_cost.sql") }
    @{ Number = "272"; Folder = "scenario_272_job_toutes_les_5_min_alors_que_le_metier_accepte_30_min"; Files = @("finops_job_toutes_5_min_alors_metier_accepte.sql", "benchmark_job_toutes_5_min_alors_metier_accepte.py", "validate_job_toutes_5_min_alors_metier_accepte_cost.sql") }
    @{ Number = "273"; Folder = "scenario_273_cluster_oublie_apres_incident"; Files = @("finops_cluster_oublie_incident.sql", "benchmark_cluster_oublie_incident.py", "validate_cluster_oublie_incident_cost.sql") }
    @{ Number = "274"; Folder = "scenario_274_merge_incremental_coute_presque_un_full_refresh"; Files = @("finops_merge_incremental_coute_presque_full_r.sql", "benchmark_merge_incremental_coute_presque_full_r.py", "validate_merge_incremental_coute_presque_full_r_cost.sql") }
    @{ Number = "275"; Folder = "scenario_275_historique_delta_represente_la_majorite_du_cout_stockage"; Files = @("finops_historique_delta_represente_majorite_c.sql", "validate_historique_delta_represente_majorite_c_cost.sql") }
    @{ Number = "276"; Folder = "scenario_276_dashboard_cout_par_domaine_et_environnement"; Files = @("finops_dashboard_cout_domaine_environnement.sql", "validate_dashboard_cout_domaine_environnement_cost.sql") }
    @{ Number = "277"; Folder = "scenario_277_deploiement_ferait_exploser_le_cout_x5"; Files = @("finops_deploiement_ferait_exploser_cout_x5.sql", "benchmark_deploiement_ferait_exploser_cout_x5.py", "validate_deploiement_ferait_exploser_cout_x5_cost.sql") }
    @{ Number = "278"; Folder = "scenario_278_data_engineer_peut_select_gold_mais_aussi_modify_par_err"; Files = @("setup_data_engineer_peut_select_gold_mais_au.sql", "validate_data_engineer_peut_select_gold_mais_au_access.sql") }
    @{ Number = "279"; Folder = "scenario_279_revocation_table_inefficace_a_cause_d_un_droit_superieur"; Files = @("setup_revocation_table_inefficace_cause_droi.sql", "validate_revocation_table_inefficace_cause_droi_access.sql") }
    @{ Number = "280"; Folder = "scenario_280_depart_d_un_owner_bloque_l_exploitation"; Files = @("setup_depart_owner_bloque_exploitation.sql", "validate_depart_owner_bloque_exploitation_access.sql") }
    @{ Number = "281"; Folder = "scenario_281_job_prod_fonctionne_avec_droits_personnels"; Files = @("setup_job_prod_fonctionne_droits_personnels.sql", "reproduce_job_prod_fonctionne_droits_personnels.py", "validate_job_prod_fonctionne_droits_personnels_access.sql") }
    @{ Number = "282"; Folder = "scenario_282_external_location_trop_large_expose_plusieurs_zones_gcs"; Files = @("setup_external_location_trop_large_expose_pl.sql", "reproduce_external_location_trop_large_expose_pl.py", "validate_external_location_trop_large_expose_pl_access.sql") }
    @{ Number = "283"; Folder = "scenario_283_secret_expose_dans_logs_notebook"; Files = @("setup_secret_expose_logs_notebook.sql", "reproduce_secret_expose_logs_notebook.py", "validate_secret_expose_logs_notebook_access.sql") }
    @{ Number = "284"; Folder = "scenario_284_qui_a_lu_la_table_sensible_avant_l_incident"; Files = @("setup_lu_table_sensible_incident.sql", "validate_lu_table_sensible_incident_access.sql") }
    @{ Number = "285"; Folder = "scenario_285_dev_peut_ecrire_dans_prod"; Files = @("setup_dev_peut_ecrire_prod.sql", "reproduce_dev_peut_ecrire_prod.py", "validate_dev_peut_ecrire_prod_access.sql") }
    @{ Number = "286"; Folder = "scenario_286_changement_d_une_colonne_sensible_propage_en_gold"; Files = @("setup_changement_colonne_sensible_propage_go.sql", "validate_changement_colonne_sensible_propage_go_access.sql") }
    @{ Number = "287"; Folder = "scenario_287_acces_d_urgence_prod_tracable_et_reversible"; Files = @("setup_acces_urgence_prod_tracable_reversible.sql", "reproduce_acces_urgence_prod_tracable_reversible.py", "validate_acces_urgence_prod_tracable_reversible_access.sql") }
    @{ Number = "288"; Folder = "scenario_288_terraform_plan_revele_une_modification_manuelle_prod"; Files = @("terraform/terraform_plan_revele_modification_man.tf", "validate_terraform_plan_revele_modification_man.ps1") }
    @{ Number = "289"; Folder = "scenario_289_plan_veut_recreer_une_ressource_critique"; Files = @("terraform/plan_veut_recreer_ressource_critique.tf", "tests/test_plan_veut_recreer_ressource_critique.py", "validate_plan_veut_recreer_ressource_critique.ps1") }
    @{ Number = "290"; Folder = "scenario_290_variable_dev_deployee_en_prod"; Files = @("terraform/variable_dev_deployee_prod.tf", "bundle/variable_dev_deployee_prod.yml", "tests/test_variable_dev_deployee_prod.py", "validate_variable_dev_deployee_prod.ps1") }
    @{ Number = "291"; Folder = "scenario_291_infrastructure_appliquee_mais_bundle_echoue"; Files = @("terraform/infrastructure_appliquee_mais_bundle_e.tf", "bundle/infrastructure_appliquee_mais_bundle_e.yml", "tests/test_infrastructure_appliquee_mais_bundle_e.py", "validate_infrastructure_appliquee_mais_bundle_e.ps1") }
    @{ Number = "292"; Folder = "scenario_292_tests_unitaires_verts_mais_pipeline_d_integration_cassee"; Files = @("tests/test_tests_unitaires_verts_mais_pipeline_in.py", "validate_tests_unitaires_verts_mais_pipeline_in.ps1") }
    @{ Number = "293"; Folder = "scenario_293_artefact_test_different_de_celui_promu_en_prod"; Files = @("bundle/artefact_test_different_celui_promu_pr.yml", "tests/test_artefact_test_different_celui_promu_pr.py", "validate_artefact_test_different_celui_promu_pr.ps1") }
    @{ Number = "294"; Folder = "scenario_294_rollback_code_ne_restaure_pas_le_schema_de_donnees"; Files = @("tests/test_rollback_code_restaure_schema_donnees.py", "validate_rollback_code_restaure_schema_donnees.ps1") }
    @{ Number = "295"; Folder = "scenario_295_grant_manuel_disparait_au_prochain_deploiement"; Files = @("terraform/grant_manuel_disparait_prochain_deploi.tf", "bundle/grant_manuel_disparait_prochain_deploi.yml", "tests/test_grant_manuel_disparait_prochain_deploi.py", "validate_grant_manuel_disparait_prochain_deploi.ps1") }
    @{ Number = "296"; Folder = "scenario_296_pr_augmente_workers_sans_justification"; Files = @("terraform/pr_augmente_workers_justification.tf", "tests/test_pr_augmente_workers_justification.py", "validate_pr_augmente_workers_justification.ps1") }
    @{ Number = "297"; Folder = "scenario_297_canary_pipeline_avant_promotion_globale"; Files = @("bundle/canary_pipeline_promotion_globale.yml", "tests/test_canary_pipeline_promotion_globale.py", "validate_canary_pipeline_promotion_globale.ps1") }
    @{ Number = "298"; Folder = "scenario_298_features_batch_perimees_au_moment_du_scoring"; Files = @("build_point_in_time_features.py", "score_with_fresh_features.py", "validate_feature_freshness.sql") }
    @{ Number = "299"; Folder = "scenario_299_transformation_differente_entre_train_et_batch_inference"; Files = @("train_feature_pipeline.py", "batch_inference_pipeline.py", "compare_train_serve_features.py") }
    @{ Number = "300"; Folder = "scenario_300_nouvelle_version_modele_degrade_le_kpi"; Files = @("train_candidate_model.py", "evaluate_candidate_model.py", "compare_model_versions.py") }
    @{ Number = "301"; Folder = "scenario_301_challenger_meilleur_mais_3x_plus_couteux"; Files = @("train_challenger_model.py", "benchmark_model_cost.py", "compare_champion_challenger.py") }
    @{ Number = "302"; Folder = "scenario_302_drift_donnees_sans_baisse_de_performance_immediate"; Files = @("generate_drift_dataset.py", "detect_data_drift.py", "monitor_model_performance.py") }
)

$createdFolders = 0
$createdFiles = 0
$preservedFiles = 0
$documentedScenarios = 0

foreach ($definition in $definitions) {
    $number = $definition.Number

    # Reuse an existing scenario folder even if its current name differs
    # from the canonical name generated from the Excel roadmap.
    $existingFolder = Get-ChildItem -Path $ScenariosRoot -Directory -Filter "scenario_${number}_*" -ErrorAction SilentlyContinue | Select-Object -First 1

    if ($null -ne $existingFolder) {
        $scenarioDir = $existingFolder.FullName
    }
    else {
        $scenarioDir = Join-Path $ScenariosRoot $definition.Folder
        New-Item -ItemType Directory -Path $scenarioDir -Force | Out-Null
        $createdFolders++
        Write-Host "[FOLDER] $($definition.Folder)"
    }

    $readmePath = Join-Path $scenarioDir 'README.md'

    # If a scenario already has a non-empty README, consider it already
    # documented/worked and keep its current file structure untouched.
    if ((Test-Path $readmePath) -and ((Get-Item $readmePath).Length -gt 0)) {
        Write-Host "[KEEP]   Scenario $number already documented: $(Split-Path $scenarioDir -Leaf)"
        $documentedScenarios++
        continue
    }

    if (-not (Test-Path $readmePath)) {
        New-Item -ItemType File -Path $readmePath | Out-Null
        $createdFiles++
    }

    # Folder reserved for the architecture image added after the scenario.
    $imagesDir = Join-Path $scenarioDir 'images'
    New-Item -ItemType Directory -Path $imagesDir -Force | Out-Null
    $gitkeep = Join-Path $imagesDir '.gitkeep'
    if (-not (Test-Path $gitkeep)) {
        New-Item -ItemType File -Path $gitkeep | Out-Null
        $createdFiles++
    }

    foreach ($relativeFile in $definition.Files) {
        $fullPath = Join-Path $scenarioDir $relativeFile
        $parent = Split-Path -Parent $fullPath

        if (-not (Test-Path $parent)) {
            New-Item -ItemType Directory -Path $parent -Force | Out-Null
        }

        if (-not (Test-Path $fullPath)) {
            New-Item -ItemType File -Path $fullPath | Out-Null
            $createdFiles++
            Write-Host "[CREATE] $number -> $relativeFile"
        }
        else {
            $preservedFiles++
        }
    }
}

Write-Host ""
Write-Host "============================================================"
Write-Host "302 scenario workspaces prepared"
Write-Host "============================================================"
Write-Host "New folders          : $createdFolders"
Write-Host "New files            : $createdFiles"
Write-Host "Existing files kept  : $preservedFiles"
Write-Host "Documented untouched : $documentedScenarios"
Write-Host "Root                 : $ScenariosRoot"
