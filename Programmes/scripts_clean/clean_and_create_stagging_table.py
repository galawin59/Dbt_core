import snowflake.connector
import yaml
import os

def get_snowflake_credentials():
    profiles_path = os.path.expanduser('~/.dbt/profiles.yml')
    with open(profiles_path, 'r') as f:
        profiles = yaml.safe_load(f)
    profile = profiles[list(profiles.keys())[0]]['outputs']['dev']
    return profile

def main():
    creds = get_snowflake_credentials()
    ctx = snowflake.connector.connect(
        user=creds['user'],
        password=creds['password'],
        account=creds['account'],
        warehouse=creds['warehouse'],
        database=creds['database'],
        schema='RAW',
        role=creds.get('role', None)
    )
    cs = ctx.cursor()
    try:
        src_table = 'YELLOW_TAXI_TRIPS_2024_01'
        tgt_schema = 'STAGGING'
        tgt_table = 'CLEAN_TRIPS_2024_01'
        # Créer le schéma cible si besoin
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {tgt_schema};")
        # Nettoyage et création de la table propre
        sql = f'''
CREATE OR REPLACE TABLE {tgt_schema}.{tgt_table} AS
SELECT
  -- Champs d'origine nettoyés
  IFF(AIRPORT_FEE < 0, NULL, AIRPORT_FEE) AS AIRPORT_FEE,
  DOLocationID,
  PULocationID,
  RatecodeID,
  VendorID,
  IFF(CONGESTION_SURCHARGE < 0, NULL, CONGESTION_SURCHARGE) AS CONGESTION_SURCHARGE,
  IFF(EXTRA < 0, NULL, EXTRA) AS EXTRA,
  IFF(FARE_AMOUNT < 0, NULL, FARE_AMOUNT) AS FARE_AMOUNT,
  IFF(IMPROVEMENT_SURCHARGE < 0, NULL, IMPROVEMENT_SURCHARGE) AS IMPROVEMENT_SURCHARGE,
  IFF(MTA_TAX < 0, NULL, MTA_TAX) AS MTA_TAX,
  IFF(PASSENGER_COUNT < 0, NULL, PASSENGER_COUNT) AS PASSENGER_COUNT,
  PAYMENT_TYPE,
  STORE_AND_FWD_FLAG,
  IFF(TIP_AMOUNT < 0, NULL, TIP_AMOUNT) AS TIP_AMOUNT,
  IFF(TOLLS_AMOUNT < 0, NULL, TOLLS_AMOUNT) AS TOLLS_AMOUNT,
  IFF(TOTAL_AMOUNT < 0, NULL, TOTAL_AMOUNT) AS TOTAL_AMOUNT,
  IFF(TRIP_DISTANCE < 0, NULL, TRIP_DISTANCE) AS TRIP_DISTANCE,
  CASE WHEN TRY_TO_TIMESTAMP(TPEP_PICKUP_DATETIME) >= '2009-01-01' THEN TPEP_PICKUP_DATETIME ELSE NULL END AS TPEP_PICKUP_DATETIME,
  CASE WHEN TRY_TO_TIMESTAMP(TPEP_DROPOFF_DATETIME) >= '2009-01-01' THEN TPEP_DROPOFF_DATETIME ELSE NULL END AS TPEP_DROPOFF_DATETIME,
  -- Enrichissements
  DATEDIFF('minute', TRY_TO_TIMESTAMP(TPEP_PICKUP_DATETIME), TRY_TO_TIMESTAMP(TPEP_DROPOFF_DATETIME)) AS TRIP_DURATION_MINUTES,
  EXTRACT(hour FROM TRY_TO_TIMESTAMP(TPEP_PICKUP_DATETIME)) AS PICKUP_HOUR,
  EXTRACT(day FROM TRY_TO_TIMESTAMP(TPEP_PICKUP_DATETIME)) AS PICKUP_DAY,
  EXTRACT(month FROM TRY_TO_TIMESTAMP(TPEP_PICKUP_DATETIME)) AS PICKUP_MONTH,
  CASE WHEN DATEDIFF('second', TRY_TO_TIMESTAMP(TPEP_PICKUP_DATETIME), TRY_TO_TIMESTAMP(TPEP_DROPOFF_DATETIME)) > 0
    THEN TRIP_DISTANCE * 3600.0 / DATEDIFF('second', TRY_TO_TIMESTAMP(TPEP_PICKUP_DATETIME), TRY_TO_TIMESTAMP(TPEP_DROPOFF_DATETIME))
    ELSE NULL END AS AVG_SPEED_MPH,
  CASE WHEN FARE_AMOUNT > 0 THEN TIP_AMOUNT / FARE_AMOUNT ELSE NULL END AS TIP_PCT
FROM RAW.{src_table}
WHERE
  TRIP_DISTANCE BETWEEN 0.1 AND 100
  AND TOTAL_AMOUNT > 0
  AND FARE_AMOUNT > 0
  AND TPEP_PICKUP_DATETIME IS NOT NULL
  AND TPEP_DROPOFF_DATETIME IS NOT NULL
  AND TRY_TO_TIMESTAMP(TPEP_PICKUP_DATETIME) < TRY_TO_TIMESTAMP(TPEP_DROPOFF_DATETIME)
  AND PULocationID IS NOT NULL
  AND DOLocationID IS NOT NULL;
'''
        cs.execute(sql)
        print(f"Table nettoyée créée : {tgt_schema}.{tgt_table}")
    finally:
        cs.close()
        ctx.close()

if __name__ == '__main__':
    main()
