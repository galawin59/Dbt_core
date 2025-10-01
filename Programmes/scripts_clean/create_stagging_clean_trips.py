import snowflake.connector
import yaml
import os

# Charger les credentials depuis profiles.yml
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
        # Table source et cible
        src_table = 'YELLOW_TAXI_TRIPS_2024_01'
        tgt_schema = 'STAGGING'
        tgt_table = 'CLEAN_TRIPS_2024_01'
        # Créer le schéma cible si besoin
        cs.execute(f"CREATE SCHEMA IF NOT EXISTS {tgt_schema};")
        # Récupérer le schéma de la table source
        cs.execute(f"SHOW COLUMNS IN RAW.{src_table};")
        columns = [(row[2], row[3]) for row in cs.fetchall()]
        # Créer la table cible avec le même schéma que la source
        cs.execute(f"CREATE OR REPLACE TABLE {tgt_schema}.{tgt_table} LIKE RAW.{src_table};")
        print(f"Table {tgt_schema}.{tgt_table} créée.")
        # Copier les données
        cs.execute(f"INSERT INTO {tgt_schema}.{tgt_table} SELECT * FROM RAW.{src_table};")
        print(f"Données copiées dans {tgt_schema}.{tgt_table}.")
    finally:
        cs.close()
        ctx.close()

if __name__ == '__main__':
    main()
