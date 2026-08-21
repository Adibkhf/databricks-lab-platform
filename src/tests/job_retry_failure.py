import datetime  # Permet d'afficher l'heure exacte de chaque tentative.

print(f"Retry test started at {datetime.datetime.now()}")  # Permet d'identifier chaque nouvelle tentative dans les logs.

raise RuntimeError("Intentional failure to test Lakeflow Jobs retries.")  # Force volontairement l'échec de la task.