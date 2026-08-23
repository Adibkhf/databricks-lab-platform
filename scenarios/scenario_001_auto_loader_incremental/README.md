# Scenario 001 — Auto Loader : ingestion incrémentale

## Objectif

Vérifier qu'un pipeline Databricks Auto Loader ingère uniquement les nouveaux fichiers déposés dans GCS sans retraiter les fichiers déjà connus.

## Architecture

GCS RAW → Auto Loader → Bronze Delta → Silver → Gold

Le checkpoint Auto Loader conserve l'état des fichiers déjà découverts.

## Baseline

Avant le test :

- lignes Bronze : 50 003
- fichiers sources : 3

Fichiers déjà traités :

- batch_001/orders.csv : 50 000 lignes
- batch_002/orders_bad.csv : 2 lignes
- batch_003/orders_duplicate.csv : 1 ligne

## Reproduction

Un nouveau fichier a été ajouté :

`batch_004/orders.csv`

Il contient 2 nouvelles commandes.

## Résultat

Après exécution du pipeline :

- lignes Bronze : 50 005
- fichiers sources : 4
- batch_004/orders.csv : 2 lignes

Les anciens fichiers n'ont pas été réingérés.

## Vérification du checkpoint

La fonction Databricks `cloud_files_state()` montre les quatre fichiers dans l'état Auto Loader.

Le checkpoint permet donc à Auto Loader de distinguer :

- fichiers déjà traités
- nouveaux fichiers à ingérer

## Diagnostic

Le comportement est conforme.

Aucun doublon n'a été créé par une relecture des anciens fichiers.

## Conclusion

Auto Loader assure ici une ingestion incrémentale basée sur son état de checkpoint.

Le checkpoint est donc un composant critique de la fiabilité du pipeline.

## Point Tech Lead

Un checkpoint doit :

- être durable ;
- être propre à la query ;
- être conservé entre les exécutions ;
- ne pas être supprimé sans stratégie de replay.

## Script

[Voir le script du scénario](./scenario_001.ps1)