import snowflake.connector
import yaml
import os


from dotenv import load_dotenv
import os

load_dotenv()

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

def main():
    creds = get_snowflake_credentials()
    ctx = snowflake.connector.connect(
        user=creds['user'],
        password=creds['password'],
        account=creds['account'],
        warehouse=creds['warehouse'],
        database=creds['database'],
        schema='STAGGING',
        role=creds.get('role', None)
    )
    cs = ctx.cursor()
    try:
        table = 'YELLOW_TAXI_TRIPS_2024_01'
        schema = 'RAW'
        print(f"Analyse de la table {schema}.{table}\n")
        # Afficher les colonnes et types
        cs.execute(f"SHOW COLUMNS IN {schema}.{table};")
        columns = cs.fetchall()
        print("Colonnes :")
        for col in columns:
            print(f"- {col[2]} : {col[3]}")
        print("\nAperçu des 10 premières lignes :")
        cs.execute(f"SELECT * FROM {schema}.{table} LIMIT 10;")
        rows = cs.fetchall()
        col_names = [col[2] for col in columns]
        print(col_names)
        for row in rows:
            print(row)
        print("\nStatistiques par colonne :")
        for col in col_names:
            cs.execute(f"SELECT COUNT(DISTINCT {col}), COUNT(*) - COUNT({col}), MIN({col}), MAX({col}) FROM {schema}.{table};")
            stats = cs.fetchone()
            print(f"{col} : {stats[0]} distincts, {stats[1]} nulls, min={stats[2]}, max={stats[3]}")
    finally:
        cs.close()
        ctx.close()

if __name__ == '__main__':
    main()
