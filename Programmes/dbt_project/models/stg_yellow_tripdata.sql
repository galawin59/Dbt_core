-- Exemple de modèle de staging pour charger un fichier Parquet
-- À adapter selon le nom de votre table et le schéma cible

{{
  config(
    materialized='table'
  )
}}

select *
from external_stage.yellow_tripdata_2024_01
