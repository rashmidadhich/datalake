# ingest_projects

Data-lake ingestion work, organised by medallion layer.

| Folder | What it is |
|---|---|
| `bronze_loader/` | Oracle (SZ/WZ/NZ/EZ zones) → PostgreSQL bronze sync, with dashboard and zone mappings. See its `README.md` and `TEAM_GUIDE.md`. |
| `bronze_layer_submission/` | Bronze-layer assignment: starter code, sample data, submission artifacts. |
| `silver/` | Silver-layer pipeline (PySpark), tests and evidence artifacts. See `SETUP_AND_RUN.md`. History preserved from its original repo. |

## Setup
Credentials live in `bronze_loader/.env` (git-ignored). Copy `bronze_loader/.env.example` to `.env` and fill it in locally.
