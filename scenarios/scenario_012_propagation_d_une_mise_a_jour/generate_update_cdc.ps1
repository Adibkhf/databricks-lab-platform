param([string]$Profile = 'DEV')

$ErrorActionPreference = 'Stop'
$scenarioFolder = $PSScriptRoot
$projectFolder = Split-Path (Split-Path $scenarioFolder -Parent) -Parent
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$runFolder = Join-Path $projectFolder "tmp/scenario_012_$stamp"
New-Item -ItemType Directory -Path $runFolder -Force | Out-Null

# Le notebook reprend le même code que le fichier Python du scénario.
$userJson = databricks current-user me -p $Profile --output json
if ($LASTEXITCODE -ne 0) { throw 'Connexion Databricks impossible.' }
$userName = ($userJson | ConvertFrom-Json).userName
$notebookPath = "/Workspace/Users/$userName/scenario_012_cdc_update_$stamp"
$pythonPath = Join-Path $scenarioFolder 'apply_update_cdc.py'
$notebookFile = Join-Path $runFolder 'notebook.py'
$notebookSource = "# Databricks notebook source`n" + [IO.File]::ReadAllText($pythonPath) + "`ndbutils.notebook.exit(json.dumps(results, default=str, ensure_ascii=False))`n"
[IO.File]::WriteAllText($notebookFile, $notebookSource, [Text.UTF8Encoding]::new($false))

databricks workspace import $notebookPath --file $notebookFile --language PYTHON --format SOURCE -p $Profile
if ($LASTEXITCODE -ne 0) { throw 'Import du notebook impossible.' }

# Exécution serverless isolée, sans modifier le job ecommerce existant.
$request = @{
    run_name = 'scenario_012_cdc_update'
    timeout_seconds = 600
    tasks = @(@{
        task_key = 'cdc_update'
        timeout_seconds = 600
        notebook_task = @{ source = 'WORKSPACE'; notebook_path = $notebookPath }
    })
}
$requestFile = Join-Path $runFolder 'submit.json'
[IO.File]::WriteAllText($requestFile, ($request | ConvertTo-Json -Depth 8), [Text.UTF8Encoding]::new($false))
$runJson = databricks jobs submit --json "@$requestFile" -p $Profile --output json
if ($LASTEXITCODE -ne 0) { throw 'Exécution du scénario échouée. Consulter le run Databricks.' }
$run = $runJson | ConvertFrom-Json
if ($run.state.result_state -ne 'SUCCESS') { throw "Résultat du run : $($run.state.result_state)" }
[IO.File]::WriteAllText((Join-Path $runFolder 'run.json'), $runJson, [Text.UTF8Encoding]::new($false))

$outputJson = databricks jobs get-run-output $run.tasks[0].run_id -p $Profile --output json
if ($LASTEXITCODE -ne 0) { throw 'Lecture des résultats impossible.' }
$output = $outputJson | ConvertFrom-Json
if ($output.notebook_output.truncated) { throw 'Les résultats du notebook sont tronqués.' }
$results = $output.notebook_output.result | ConvertFrom-Json
if ($results.scenario -ne '012' -or -not $results.checks.replay_unchanged) { throw 'Résultats du scénario incomplets.' }

# La preuve conserve les métriques réelles et la version du code exécuté.
$results | Add-Member -NotePropertyName run_url -NotePropertyValue $run.run_page_url
$results | Add-Member -NotePropertyName executed_at_utc -NotePropertyValue ([DateTime]::UtcNow.ToString('o'))
$results | Add-Member -NotePropertyName code_sha256 -NotePropertyValue ((Get-FileHash -LiteralPath $pythonPath -Algorithm SHA256).Hash.ToLower())
$resultFile = Join-Path $scenarioFolder 'execution_update.json'
[IO.File]::WriteAllText($resultFile, ($results | ConvertTo-Json -Depth 8), [Text.UTF8Encoding]::new($false))

Write-Output "Résultats : $resultFile"
Write-Output "Run Databricks : $($run.run_page_url)"
Write-Output "UPDATE : $($results.first_run.updated_rows) / Replay : $($results.replay.updated_rows)"
