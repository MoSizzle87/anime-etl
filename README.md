# Anime & Manga Intelligence — Pipeline ETL

Pipeline ETL complet pour agréger, normaliser et analyser des données anime/manga
depuis 3 sources : Kaggle CSV, Jikan API (MyAnimeList) et AniList GraphQL.

---

## 📐 Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                          │
│                                                              │
│   Kaggle CSV          Jikan REST API      AniList GraphQL    │
│  (17,562 animes)    (enrichissement)      (trending/scores)  │
│   data/raw/          3 req/sec            90 req/min         │
└───────┬──────────────────┬────────────────────┬─────────────┘
        │                  │                    │
        ▼                  ▼                    ▼
┌──────────────────────────────────────────────────────────────┐
│                      EXTRACT (src/extract.py)                │
│  - Lecture CSV Pandas                                        │
│  - Requêtes HTTP avec retry + rate limiting                  │
│  - Requêtes GraphQL AniList                                  │
│  - Sauvegarde data/raw/ (JSON/CSV bruts)                     │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                    TRANSFORM (src/transform.py)              │
│  - Normalisation titres (minuscules, accents, caractères)    │
│  - Fuzzy matching RapidFuzz (seuil 85%) pour dédup          │
│  - Join sur MAL_ID quand disponible                         │
│  - Calcul scores agrégés (weighted average MAL + AniList)   │
│  - Great Expectations : validation qualité données           │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                      LOAD (src/load.py)                      │
│                                                              │
│         SCHÉMA EN ÉTOILE PostgreSQL 17                       │
│                                                              │
│    f_anime_ratings (faits)                                   │
│    ├── d_anime (dimension)                                   │
│    ├── d_genre (dimension)                                   │
│    ├── d_studio (dimension)                                   │
│    ├── anime_genre (linking)                                 │
│    └── anime_studio (linking)                                │
└──────────────────────────────────────────────────────────────┘
```

---

## Schéma Base de Données

```sql
-- Table de faits : scores agrégés
f_anime_ratings
├── anime_id     INTEGER  PK
├── mal_score    FLOAT
├── anilist_score FLOAT
├── avg_score    FLOAT
└── updated_at   TIMESTAMP

-- Dimension principale
d_anime
├── anime_id     INTEGER  PK
├── title        VARCHAR  (normalisé)
├── type         VARCHAR  (TV, Movie, OVA, ONA, Special)
└── episodes     INTEGER

-- Dimension genres
d_genre
├── genre_id     SERIAL   PK
└── genre_name   VARCHAR  UNIQUE

-- Dimension studios
d_studio
├── studio_id    SERIAL   PK
└── studio_name  VARCHAR  UNIQUE

-- Tables de liaison (many-to-many)
anime_genre  (anime_id FK, genre_id FK)
anime_studio (anime_id FK, studio_id FK)
```

---

## Quick Start

### Prérequis
- Docker + Docker Compose
- Python 3.13 + `uv`
- Dataset Kaggle : [anime-recommendations-database](https://www.kaggle.com/datasets/CooperUnion/anime-recommendations-database)

### 1. Cloner et configurer

```bash
git clone <repo-url>
cd anime-etl

# Configurer les variables d'environnement
cp .env.template .env
# Éditer .env si nécessaire (les valeurs par défaut fonctionnent avec Docker)
```

### 2. Placer le dataset Kaggle

```bash
# Télécharger depuis Kaggle UI ou via CLI :
kaggle datasets download -d CooperUnion/anime-recommendations-database
unzip anime-recommendations-database.zip -d data/raw/
```

### 3. Démarrer la stack Docker

```bash
docker-compose up --build -d

# Vérifier que PostgreSQL est healthy
docker-compose ps
# Attendre ~30s pour le healthcheck PostgreSQL
```

### 4. Exécuter le pipeline

```bash
# Option A : dans le container
docker-compose exec app python pipeline.py

# Option B : en local (avec venv)
uv sync --extra dev
python pipeline.py
```

### 5. Vérifier les données

```bash
# Se connecter à PostgreSQL
docker-compose exec postgres psql -U anime_user -d anime_db

# Requête de vérification
SELECT COUNT(*) FROM d_anime;
SELECT a.title, r.avg_score
FROM d_anime a
JOIN f_anime_ratings r ON a.anime_id = r.anime_id
ORDER BY r.avg_score DESC
LIMIT 10;
```

---

## Tests

```bash
# Installer les dépendances dev
uv sync --extra dev

# Lancer tous les tests avec coverage
pytest

# Tests unitaires uniquement
pytest tests/unit/ -v

# Tests d'intégration (nécessite PostgreSQL running)
pytest tests/integration/ -v

# Coverage HTML
pytest --cov-report=html
open htmlcov/index.html
```

---

## Structure du Projet

```
anime-etl/
├── README.md
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── .env.template
├── .gitignore
├── pipeline.py              # Orchestration E→T→L
│
├── src/
│   ├── config.py            # Config centralisée (env vars, DB)
│   ├── extract.py           # Extraction 3 sources
│   ├── transform.py         # Normalisation + fuzzy matching
│   └── load.py              # Chargement schéma en étoile
│
├── tests/
│   ├── unit/
│   │   ├── test_extract.py
│   │   ├── test_transform.py
│   │   └── test_load.py
│   ├── integration/
│   │   └── test_end_to_end.py
│   └── fixtures/
│       └── sample_anime.csv
│
├── data/
│   ├── raw/                 # ⚠️ gitignored - données brutes
│   └── processed/           # ⚠️ gitignored - données transformées
│
└── great_expectations/      # Validation qualité données
    └── expectations/
```

---

## Sources de Données

| Source | Type | Rate Limit | Données |
|--------|------|-----------|---------|
| Kaggle Anime Dataset | CSV local | Aucune | 17,562 animes, scores MAL, genres |
| [Jikan API v4](https://jikan.moe/) | REST JSON | 3 req/sec | Synopsis, studios, relations |
| [AniList GraphQL](https://anilist.co/graphiql) | GraphQL | 90 req/min | Trending, scores AniList |

---

## Critères d'Acceptation

- Pipeline complet en < 3 min (CPU 4 cœurs)
- Tests unitaires + intégration : coverage ≥ 80%
- Linting : flake8 + mypy sans erreurs
- Zéro doublons dans `d_anime` après déduplication
- Fuzzy matching ≥ 85% accuracy (validé sur 20 samples manuels)
- Logs structurés + gestion d'erreurs complète

---

## Stack Technique

| Outil | Version | Usage |
|-------|---------|-------|
| Python | 3.13 | Runtime |
| pandas | 2.3.3 | DataFrames + transformations |
| requests | 2.32.4 | Jikan REST API |
| graphql-core | 3.2.6 | AniList GraphQL |
| RapidFuzz | 3.13.0 | Fuzzy matching déduplication |
| SQLAlchemy | 2.0.41 | ORM + connexions DB |
| psycopg2-binary | 2.9.10 | Driver PostgreSQL |
| great-expectations | 1.4.5 | Data quality checks |
| pytest | 8.4.2 | Tests + coverage |
| PostgreSQL | 17-alpine | Base de données (Docker) |

---
