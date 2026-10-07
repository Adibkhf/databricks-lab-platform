# databricks-lab-platform

Exercices Databricks autour de l’ingestion, du CDC et du streaming. Chaque scénario documente un problème, sa correction et les contrôles réalisés.

## Ingestion et qualité des fichiers

- [001 — Auto Loader : ingestion incrémentale](scenarios/scenario_001_auto_loader_incremental/README.md)
- [006 — Évolution du schéma](scenarios/scenario_006_schema_evolution/README.md)
- [007 — Contrat de fichier et quarantaine](scenarios/scenario_007_file_contract/README.md)
- [008 — Impact des petits fichiers](scenarios/scenario_008_generate_small_files/README.md)

## Reprise et CDC

- [010 — Suppression du checkpoint Auto Loader](scenarios/scenario_010_suppression_du_checkpoint_auto_loader/README.md)
- [011 — Appliquer uniquement les nouveaux enregistrements](scenarios/scenario_011_appliquer_uniquement_les_nouveaux_enregistrements/README.md)
- [012 — Propagation d’une mise à jour](scenarios/scenario_012_propagation_d_une_mise_a_jour/README.md)
- [013 — Propagation d’une suppression](scenarios/scenario_013_propagation_d_une_suppression/README.md)
- [014 — INSERT, UPDATE et DELETE dans le même lot](scenarios/scenario_014_insert_update_delete_dans_le_meme_lot/README.md)

## État et jointures en streaming

- [025 — État qui augmente sans borne](scenarios/scenario_025_etat_qui_augmente_sans_borne/README.md)
- [026 — Jointure sans contrainte temporelle](scenarios/scenario_026_jointure_sans_contrainte_temporelle/README.md)

## Performance des transformations

- [028 — Transformation trop lente](scenarios/scenario_028_transformation_trop_lente/README.md)
