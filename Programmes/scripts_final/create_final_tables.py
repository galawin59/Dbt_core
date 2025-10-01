
import snowflake.connector
import os
import re
from dotenv import load_dotenv

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
        # Lister toutes les tables CLEAN_TRIPS_YYYY_MM
        cs.execute("SHOW TABLES IN SCHEMA STAGGING;")
        tables = [row[1] for row in cs.fetchall() if re.match(r'^CLEAN_TRIPS_\d{4}_\d{2}$', row[1])]
        print(f"Tables trouvées : {tables}")
        # Construire la requête UNION ALL
        union_query = "\nUNION ALL\n".join([f"SELECT * FROM STAGGING.{t}" for t in tables])
        # Créer le schéma FINAL si besoin
        cs.execute("CREATE SCHEMA IF NOT EXISTS FINAL;")
        # Table 1 : daily_summary
        sql_daily = f'''
CREATE OR REPLACE TABLE FINAL.daily_summary AS
SELECT
  TO_DATE(TPEP_PICKUP_DATETIME) AS pickup_date,
  COUNT(*) AS trip_count,
  AVG(TRIP_DISTANCE) AS avg_distance,
  SUM(TOTAL_AMOUNT) AS total_revenue
FROM (
  {union_query}
)
GROUP BY pickup_date
ORDER BY pickup_date;
'''
        cs.execute(sql_daily)
        print("Table FINAL.daily_summary créée.")
        # Table 2 : zone_analysis
        sql_zone = f'''
CREATE OR REPLACE TABLE FINAL.zone_analysis AS
SELECT
  PULocationID,
  COUNT(*) AS trip_count,
  AVG(TOTAL_AMOUNT) AS avg_revenue,
  SUM(TOTAL_AMOUNT) AS total_revenue
FROM (
  {union_query}
)
GROUP BY PULocationID
ORDER BY trip_count DESC;
'''
        cs.execute(sql_zone)
        print("Table FINAL.zone_analysis créée.")
        # Table 3 : hourly_patterns
        sql_hourly = f'''
CREATE OR REPLACE TABLE FINAL.hourly_patterns AS
SELECT
  PICKUP_HOUR,
  COUNT(*) AS trip_count,
  SUM(TOTAL_AMOUNT) AS total_revenue,
  AVG(AVG_SPEED_MPH) AS avg_speed
FROM (
  {union_query}
)
GROUP BY PICKUP_HOUR
ORDER BY PICKUP_HOUR;
'''
        cs.execute(sql_hourly)
        print("Table FINAL.hourly_patterns créée.")
    finally:
        cs.close()
        ctx.close()

if __name__ == '__main__':
    main()
