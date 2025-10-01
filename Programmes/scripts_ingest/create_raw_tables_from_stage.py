
import os
import snowflake.connector
import re
from dotenv import load_dotenv

load_dotenv()

STAGE_NAME = 'nyc_stage'
RAW_SCHEMA = 'RAW'

def get_snowflake_credentials():
    return {
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'role': os.getenv('SNOWFLAKE_ROLE')
    }

def extract_year_month(filename):
    match = re.search(r'(\d{4})-(\d{2})', filename)
    if match:
        return match.group(1), match.group(2)
    return None, None

def main():
    creds = get_snowflake_credentials()
    ctx = snowflake.connector.connect(
        user=creds['user'],
        password=creds['password'],
        account=creds['account'],
        warehouse=creds['warehouse'],
        database=creds['database'],
        schema=creds['schema']
    )
    cs = ctx.cursor()
    try:
        # S'assurer que le schéma RAW existe
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
        # S'assurer que le file format Parquet existe
        cs.execute("CREATE OR REPLACE FILE FORMAT parquet_format TYPE = 'PARQUET'")
        # Lister les fichiers sur le stage
        cs.execute(f"LIST @{STAGE_NAME}")
        files = [row[0] for row in cs.fetchall() if row[0].endswith('.parquet')]
        print("Fichiers trouvés sur le stage :")
        for f in files:
            print(f"- {f}")
        for file_path in files:
            file_name = os.path.basename(file_path)
            year, month = extract_year_month(file_name)
            if not year or not month:
                print(f"Nom de fichier non reconnu : {file_name}")
                continue
            table_name = f"{RAW_SCHEMA}.yellow_taxi_trips_{year}_{month}"
            print(f"Création de la table {table_name} depuis {file_name} ...")
            # 1. Créer la table temporaire (schéma Parquet, dates en NUMBER)
            temp_table = f"{table_name}_tmp"
            sql_create_tmp = f'''
CREATE OR REPLACE TABLE {temp_table} (
  Airport_fee FLOAT,
  DOLocationID INTEGER,
  PULocationID INTEGER,
  RatecodeID INTEGER,
  VendorID INTEGER,
  congestion_surcharge FLOAT,
  extra FLOAT,
  fare_amount FLOAT,
  improvement_surcharge FLOAT,
  mta_tax FLOAT,
  passenger_count INTEGER,
  payment_type INTEGER,
  store_and_fwd_flag STRING,
  tip_amount FLOAT,
  tolls_amount FLOAT,
  total_amount FLOAT,
  trip_distance FLOAT,
  tpep_pickup_datetime NUMBER,
  tpep_dropoff_datetime NUMBER
);
'''
            cs.execute(sql_create_tmp)
            print(f"Table temporaire {temp_table} créée. Chargement des données ...")
            # 2. COPY INTO table temporaire
            sql_copy_tmp = f"""
COPY INTO {temp_table} FROM @{STAGE_NAME}/{file_name}
FILE_FORMAT = (TYPE = 'PARQUET') MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
"""
            cs.execute(sql_copy_tmp)
            print(f"Table temporaire {temp_table} alimentée.")
            # 3. Créer la table RAW finale (dates en STRING)
            sql_create = f'''
CREATE OR REPLACE TABLE {table_name} (
  Airport_fee FLOAT,
  DOLocationID INTEGER,
  PULocationID INTEGER,
  RatecodeID INTEGER,
  VendorID INTEGER,
  congestion_surcharge FLOAT,
  extra FLOAT,
  fare_amount FLOAT,
  improvement_surcharge FLOAT,
  mta_tax FLOAT,
  passenger_count INTEGER,
  payment_type INTEGER,
  store_and_fwd_flag STRING,
  tip_amount FLOAT,
  tolls_amount FLOAT,
  total_amount FLOAT,
  trip_distance FLOAT,
  tpep_pickup_datetime STRING,
  tpep_dropoff_datetime STRING
);
'''
            cs.execute(sql_create)
            print(f"Table {table_name} créée. Conversion et insertion des données ...")
            # 4. INSERT INTO finale en convertissant les dates en STRING lisible
            sql_insert = f'''
INSERT INTO {table_name}
SELECT
  Airport_fee,
  DOLocationID,
  PULocationID,
  RatecodeID,
  VendorID,
  congestion_surcharge,
  extra,
  fare_amount,
  improvement_surcharge,
  mta_tax,
  passenger_count,
  payment_type,
  store_and_fwd_flag,
  tip_amount,
  tolls_amount,
  total_amount,
  trip_distance,
  TO_VARCHAR(TO_TIMESTAMP(tpep_pickup_datetime / 1000000)),
  TO_VARCHAR(TO_TIMESTAMP(tpep_dropoff_datetime / 1000000))
FROM {temp_table};
'''
            cs.execute(sql_insert)
            print(f"Table {table_name} alimentée.")
            # 5. Drop table temporaire
            cs.execute(f"DROP TABLE IF EXISTS {temp_table};")
            print(f"Table temporaire {temp_table} supprimée.")
    finally:
        cs.close()
        ctx.close()

if __name__ == "__main__":
    main()