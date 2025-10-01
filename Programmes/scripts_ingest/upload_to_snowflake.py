
import os
import snowflake.connector
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

creds = get_snowflake_credentials()

STAGE_NAME = 'nyc_stage'  # Stage interne
TABLE_NAME = 'stg_yellow_tripdata'
DATA_DIR = 'data'

# Connexion à Snowflake
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
	# Créer le stage interne si besoin
	cs.execute(f"CREATE STAGE IF NOT EXISTS {STAGE_NAME}")

	# Uploader les fichiers Parquet dans le stage
	for file in os.listdir(DATA_DIR):
		if file.endswith('.parquet'):
			local_path = os.path.join(DATA_DIR, file)
			print(f"Uploading {file} to stage {STAGE_NAME}...")
			cs.execute(f"PUT file://{local_path} @{STAGE_NAME} OVERWRITE = TRUE")


	# La table de staging doit être créée manuellement dans Snowflake avec le schéma adapté aux fichiers Parquet.
	# Exemple de requête SQL à exécuter dans Snowflake Web UI :
	# CREATE OR REPLACE TABLE stg_yellow_tripdata (
	#     -- Ajoutez ici les colonnes selon le schéma de vos fichiers Parquet
	# );

	# Les fichiers sont maintenant uploadés sur le stage. Vous pourrez charger les données dans une table plus tard avec COPY INTO.
	print("Upload terminé ! Les fichiers sont sur le stage Snowflake.")
finally:
	cs.close()
	ctx.close()