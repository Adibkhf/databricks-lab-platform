# Scenario 006 Auto Loader & Delta Schema Evolution

![Auto Loader and Delta Schema Evolution](./scenario_006.png)

## Objectif

Comprendre et et securier l'arrivée d'une nouvelle colonne dans les fichiers RAW ingeres avec Databricks Auto Loader.

Le scÃ©nario distingue deux mÃ©canismes diffÃ©rents :

1. l'Ã©volution du schÃ©ma d'entrÃ©e gÃ©rÃ©e par Auto Loader ;
2. l'Ã©volution du schÃ©ma des tables Delta cibles.

## Situation initiale

SchÃ©ma initial de `orders.csv` :

```text
order_id
customer_id
order_date
status
```

Un nouveau fichier arrive avec :

```text
order_id
customer_id
order_date
status
channel
```

Nouvelle colonne : `channel STRING`.

## Architecture

```mermaid
flowchart LR
    A[GCS / Unity Catalog Volume RAW] --> B[Auto Loader]
    B --> C[schemaLocation]
    C --> D[foreachBatch]
    D --> E[bronze.orders_raw]
    D --> F[bronze.orders_quarantine]
```

## Incident observÃ©

Lors de l'arrivÃ©e de `channel`, Auto Loader a d'abord retournÃ© :

`UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_FILE`

Le nouveau schÃ©ma a ensuite Ã©tÃ© enregistrÃ© dans :

`/Volumes/dbx_lab_dev/landing/raw/_schemas/bronze_orders`

avec une nouvelle version contenant `channel STRING`.

## Correction Auto Loader

Le lecteur utilise :

```python
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
.option("cloudFiles.schemaHints", "channel STRING")
```

`schemaLocation` conserve les versions du schÃ©ma dÃ©couvert.

`schemaHints` permet ici de prÃ©ciser explicitement le type attendu de `channel`.

## Correction Delta Lake

Auto Loader peut connaÃ®tre `channel` alors que les tables Delta cibles ne la connaissent pas encore.

Les Ã©critures vers :

- `dbx_lab_dev.bronze.orders_raw`
- `dbx_lab_dev.bronze.orders_quarantine`

utilisent donc :

```python
.option("mergeSchema", "true")
```

Cela permet aux deux tables Delta d'accepter la nouvelle colonne.

## PiÃ¨ge foreachBatch

Le `foreachBatch` Ã©crit dans plusieurs tables indÃ©pendantes.

Une situation possible est :

```text
orders_raw        -> commit OK
orders_quarantine -> erreur
micro-batch       -> FAILED
```

Les deux Ã©critures ne constituent donc pas une transaction globale unique.

Un retry doit Ãªtre conÃ§u de maniÃ¨re idempotente.

## Idempotence des retries

Les Ã©critures peuvent utiliser :

- `txnAppId`
- `txnVersion`

Le `batch_id` peut servir de version de transaction afin d'Ã©viter qu'un mÃªme micro-batch soit appliquÃ© plusieurs fois.

## Validation attendue

AprÃ¨s correction :

- Auto Loader reconnaÃ®t `channel` ;
- `orders_raw` contient `channel` ;
- `orders_quarantine` contient aussi `channel` ;
- le stream ne casse plus sur cette Ã©volution de schÃ©ma ;
- les retries ne doivent pas crÃ©er de doublons.

## Points Ã  retenir

- Auto Loader Schema Evolution et Delta Schema Evolution sont deux mÃ©canismes diffÃ©rents.
- `schemaLocation` conserve les versions du schÃ©ma Auto Loader.
- `schemaHints` contrÃ´le le type attendu d'une colonne connue.
- `mergeSchema=true` fait Ã©voluer explicitement une table Delta.
- plusieurs Ã©critures dans `foreachBatch` ne forment pas une transaction globale.
- les retries doivent Ãªtre idempotents.

## Code

[Voir le code Bronze](../../src/ingestion/bronze_order.py)

[Reproduire le scÃ©nario](./reproduce.ps1)

[Valider le scÃ©nario](./validate.sql)
