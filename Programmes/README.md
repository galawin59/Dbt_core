## Sécurité des accès et gestion des credentials

Tous les accès Snowflake (user, password, account, etc.) sont centralisés dans un fichier `.env` à la racine du projet. Ce fichier n'est jamais versionné (voir `.gitignore`).

- Les scripts Python utilisent la librairie `python-dotenv` pour charger automatiquement ces variables d'environnement.
- Le fichier `dbt_project/profiles.yml` référence également ces variables via la syntaxe `env_var`.
- Exemple de contenu du `.env` (à adapter avec tes propres accès) :

```
SNOWFLAKE_USER=ton_user
SNOWFLAKE_PASSWORD=ton_mot_de_passe
SNOWFLAKE_ACCOUNT=ton_account
SNOWFLAKE_DATABASE=ta_database
SNOWFLAKE_SCHEMA=ton_schema
SNOWFLAKE_WAREHOUSE=ton_warehouse
SNOWFLAKE_ROLE=ton_role
```

**Ne jamais versionner ce fichier !**

Pour toute nouvelle machine ou collaborateur, il suffit de copier `.env.example` en `.env` et de renseigner les accès.

# Pipeline NYC Yellow Taxi - Organisation des scripts et usage dbt


## Structure des dossiers

- `scripts_ingest/` : Scripts pour télécharger, uploader et créer les tables RAW
- `scripts_clean/` : Scripts pour nettoyer, transformer et créer les tables STAGGING
- `scripts_final/` : Script pour créer les tables agrégées dans FINAL
- `scripts_analyse/` : Scripts d'analyse et de contrôle qualité
- `dbt_project/models/` : Modèles dbt pour pipeline analytique SQL
  - `staging/` : Nettoyage et filtrage (stg_yellow_taxi_trips.sql)
  - `intermediate/` : Enrichissement, calculs (int_trip_metrics.sql)
  - `marts/` : Tables finales et analyses (fact_trips.sql, daily_summary.sql, zone_analysis.sql, hourly_patterns.sql)
- `archive/` : (optionnel) Pour archiver les anciens scripts ou essais


## Pipeline complet (exécution manuelle)

### 1. Pipeline Python

**Ingestion**
```powershell
python scripts_ingest/download_parquet_files.py
python scripts_ingest/upload_to_snowflake.py
python scripts_ingest/create_raw_tables_from_stage.py
```

**Nettoyage & enrichissement**
```powershell
python scripts_clean/clean_and_create_stagging_tables_all.py
```

**Création des tables finales**
```powershell
python scripts_final/create_final_tables.py
```

**Analyse/contrôle qualité**
```powershell
python scripts_analyse/analyze_stagging_clean_trips.py
```

### 2. Pipeline dbt (optionnel)

**Structure des modèles**
- `staging/stg_yellow_taxi_trips.sql` : Nettoyage, filtrage, standardisation
- `intermediate/int_trip_metrics.sql` : Enrichissement, calculs, dimensions business
- `marts/fact_trips.sql` : Table de faits principale
- `marts/daily_summary.sql` : Résumés quotidiens
- `marts/zone_analysis.sql` : Analyses par zone géographique
- `marts/hourly_patterns.sql` : Patterns de demande horaire

**Exécution dbt**
```powershell
cd dbt_project
dbt run
```
Cela créera toutes les vues/tables selon la logique SQL définie dans les modèles.

**Tests de qualité**
Ajoute des fichiers `.yml` dans chaque dossier pour définir des tests dbt (non-null, unique, accepted values, etc.).


## Conseils
- Adapter les scripts ou modèles si tu ajoutes de nouveaux mois ou de nouveaux fichiers.
- Utiliser le dossier `archive/` pour stocker les anciens scripts ou essais non utilisés.
- Modifier les scripts Python ou les modèles dbt selon tes besoins d'évolution du pipeline.
- Tu peux utiliser l'un ou l'autre pipeline, ou les deux en parallèle pour comparer les résultats.

---


---

Pour toute automatisation complète (pipeline unique), demande un script d'orchestration !
