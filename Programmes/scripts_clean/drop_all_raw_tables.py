
import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
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
		cs.execute(f"SHOW TABLES IN SCHEMA {RAW_SCHEMA}")
		tables = [row[1] for row in cs.fetchall() if row[1].startswith('YELLOW_TAXI_TRIPS_') or row[1].startswith('yellow_taxi_trips_')]
		for table in tables:
			print(f"Suppression de la table {RAW_SCHEMA}.{table} ...")
			cs.execute(f"DROP TABLE IF EXISTS {RAW_SCHEMA}.{table}")
		print("Toutes les tables RAW supprimées.")
	finally:
		cs.close()
		ctx.close()

if __name__ == "__main__":
	main()