# Pipeline ELT - MongoDB + API + Dashboard

Ce dépôt contient un pipeline ELT (MinIO -> Gold) et ajoute une intégration MongoDB, une API FastAPI et un Dashboard Streamlit qui interroge l'API.

Prerequis:

- Docker & docker-compose
- Python 3.10+

Démarrage des services (MinIO, Postgres, Prefect, MongoDB, Metabase):

```bash
# depuis la racine du projet
docker compose up -d
```

Installer l'environnement Python et dépendances:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Exécuter le flow Gold -> MongoDB (local):

```bash
# lance le flow qui lit le bucket gold et écrit dans MongoDB
python -m flows.gold_to_mongo
```

Lancer l'API FastAPI:

```bash
# par défaut écoute http://localhost:8000
uvicorn app.api:app --host 0.0.0.0 --port 8000
```

Lancer le Dashboard Streamlit (consomme l'API):

```bash
export API_URL=http://localhost:8000
streamlit run app/dashboard.py --server.port 8502
```

Variables d'environnement importantes:

- `MONGODB_URI` : URI MongoDB (Atlas) — ex: `mongodb+srv://...` (prioritaire)
- `MONGO_URI` : alternative locale (ex: `mongodb://localhost:27017`)
- `MONGO_DB` / `MONGODB_DB` : nom de la base (par défaut `analytics`)

Considérations:

- Le pipeline `flows/gold_to_mongo.py` détecte les fichiers Parquet ou CSV dans le bucket gold.
- Les collections Mongo portent le nom du fichier sans extension (ex: `monthly_revenue.csv` -> collection `monthly_revenue`).
- Le flow écrit une collection `ingest_metadata` contenant les timestamps d'ingestion et `source_info.last_modified` si disponible. L'API expose ces métadonnées via `/metadata/{collection}`.

Bonus - Metabase:

- Metabase est exposé sur le port 3000, vous pouvez l'utiliser pour créer des tableaux de bord pointant vers MongoDB (hôte `mongo`, port `27017`).

Si vous voulez que je:

- ajoute une route d'API supplémentaire (ex: recherche, filtres),
- empaquette tout dans un service docker (API + dashboard),
- ou configure Metabase automatiquement,
  faites-moi signe.
