import os
import requests
from datetime import datetime

# Dossier de destination
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Générer les URLs de 2024-01 à 2025-08
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}.parquet"

years_months = []
for year in [2024, 2025]:
	if year == 2024:
		months = range(1, 13)
	else:
		months = range(1, 9)
	for month in months:
		ym = f"{year}-{month:02d}"
		years_months.append(ym)

for ym in years_months:
	url = base_url.format(ym)
	local_path = os.path.join(DATA_DIR, f"yellow_tripdata_{ym}.parquet")
	print(f"Téléchargement de {url} ...")
	response = requests.get(url, stream=True)
	if response.status_code == 200:
		with open(local_path, 'wb') as f:
			for chunk in response.iter_content(chunk_size=8192):
				f.write(chunk)
		print(f"Fichier sauvegardé : {local_path}")
	else:
		print(f"Erreur lors du téléchargement de {url} : {response.status_code}")