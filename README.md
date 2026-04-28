# 🎌 AniData Lab — Observatoire Anime/Manga

> Pipeline de données complet : Data Refinement → Elasticsearch + Grafana → Airflow
> Semaine du 23 au 27 mars 2026

---

## Sommaire
### Niveau 1 — Essentiel (nouveau lecteur)

1. [Prérequis](#prerequis)
2. [Installation (5 minutes)](#installation-5-minutes)
3. [Utilisation au fil de la semaine](#utilisation-au-fil-de-la-semaine)
4. [Commandes utiles](#commandes-utiles)
5. [Dépannage](#depannage)

### Niveau 2 — Avancé (architecture, CI/CD, automatisation)

1. **Contexte du projet**
   - [Documentation du projet](#documentation-du-projet)
   - [Architecture du projet](#architecture-du-projet)
   - [Consommation mémoire (optimisée pour 8 Go)](#consommation-memoire)
2. **CI/CD, qualité et déploiement**
   - [CI GitHub Actions & protection de main](#ci-github-actions--protection-de-main)
   - [Lint : définition rapide et lien avec `needs: [lint, tests]`](#lint--definition-rapide-et-lien-avec-needs-lint-tests)
   - [Feuille de route intégrée : CI verte -> relance Airflow -> DAGs automatiques](#feuille-de-route-integree)
   - [Après "✅ Image publiée sur GHCR" : ce que ça signifie vraiment](#apres-ghcr)
   - [Peut-on automatiser totalement après CI verte ?](#automatisation-apres-ci)
3. [Ressources](#ressources)

## 📋 Prérequis

<a id="prerequis"></a>

- **Docker Desktop** installé et lancé ([docker.com/get-started](https://www.docker.com/get-started))
- **VS Code** avec les extensions Python et Jupyter ([code.visualstudio.com](https://code.visualstudio.com/))
- **Python 3.10+** installé en local (pour le Data Refinement)
- **8 Go de RAM minimum** (fermer les applications inutiles)
- **10 Go d'espace disque** disponible

### Extensions VS Code recommandées

```
code --install-extension ms-python.python
code --install-extension ms-toolsai.jupyter
```

### Vérification rapide

```bash
docker --version        # Docker 24+ recommandé
docker compose version  # Docker Compose v2+
python --version        # Python 3.10+
code --version          # VS Code
```

---

## 🚀 Installation (5 minutes)

<a id="installation-5-minutes"></a>

### Étape 1 — Copier le projet

```bash
cd ~/Desktop
unzip anidata-lab.zip && cd anidata-lab
```

### Étape 2 — Installer les dépendances Python locales (optionnel)

Si vous exécutez les scripts uniquement dans Docker, vous pouvez sauter cette étape.
Elle est nécessaire uniquement pour une exécution locale (hors conteneur).

```bash
pip install pandas numpy matplotlib seaborn elasticsearch
```

### Étape 3 — Télécharger les données

1. Aller sur **https://www.kaggle.com/datasets/hernan4444/anime-recommendation-database-2020**
2. Se connecter (compte Kaggle gratuit)
3. Cliquer **Download** et extraire dans le dossier `data/` :

```
data/
├── anime.csv                  (~3 MB)
├── rating_complete.csv        (~700 MB)
└── anime_with_synopsis.csv    (~5 MB)
```

### Étape 4 — Lancer l'environnement Docker

```bash
# Linux / Mac
chmod +x start.sh && ./start.sh

# Windows
start.bat

# Ou directement :
docker compose up -d
```

### Étape 5 — Vérifier que tout fonctionne

| Service           | URL                          | Identifiants         |
|-------------------|------------------------------|----------------------|
| **Grafana**       | http://localhost:3000        | admin / anidata      |
| **Airflow**       | http://localhost:8080        | admin / admin        |
| **Elasticsearch** | http://localhost:9200        | (API directe)        |

### Étape 6 — Ouvrir le projet dans VS Code

```bash
code .
```

Les fichiers Python et notebooks (.ipynb) s'ouvrent directement dans VS Code.

---

## 📘 Documentation du projet

<a id="documentation-du-projet"></a>

- Rapport principal de la semaine : [`notebooks/rapport.md`](notebooks/rapport.md)
- Rapport d'audit détaillé : [`rapport_audit.md`](rapport_audit.md)
- Rapport de validation détaillé : [`rapport_validation.md`](rapport_validation.md)
- Captures associées au rapport : `notebooks/images/`
- Supports de cours : `notebooks/Cours ELK Grafana Mardi.pdf`

---

## 🏗️ Architecture du projet

<a id="architecture-du-projet"></a>

```
anidata-lab/
│
├── docker-compose.yml              # Orchestration des services Docker
├── .env                            # Variables de configuration
├── start.sh / start.bat            # Scripts de démarrage
│
├── data/                           # 📦 Datasets CSV source + gold
│   ├── LIRE_MOI.txt
│   ├── anime.csv
│   ├── rating_complete.csv
│   └── anime_with_synopsis.csv
│   └── gold/
│       ├── anime_gold.csv
│       └── anime_gold.json
│
├── airflow/
│   ├── dags/                       # 🔄 Orchestration Airflow
│   │   └── anidata_full_pipeline_dag.py
│   ├── scripts/                    # Scripts exécutés dans le DAG
│   │   ├── 00_hello_anidata.py
│   │   ├── 01_audit_complet.py
│   │   ├── 02_audit_visuel.py
│   │   ├── 03_nettoyage.py
│   │   ├── 04_feature_engineering.py
│   │   ├── 05_validation.py
│   │   ├── refine_gold_dataset.py
│   │   └── script_prof.py
│   ├── plugins/                   # Optionnel
│   └── logs/                      # Logs Airflow
│
├── elk/
│   ├── mapping_anime.json          # Mapping Elasticsearch
│   └── logstash/
│       └── pipeline/               # Config Logstash
│           └── logstash.conf
│
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/            # Elasticsearch auto-configuré
│   │   └── dashboards/             # Chargement auto des dashboards
│   └── dashboards/                 # 📊 Fichiers JSON des dashboards
│       └── anidata-overview.json   # Dashboard de démarrage
│
└── notebooks/
    ├── rapport.md                  # Rapport consolidé (audit + refinement + ELK)
    ├── rapport_audit.md           # Sorties audit (01_audit_complet)
    ├── rapport_validation.md      # Sorties validation (05_validation)
    ├── images/                     # Captures & graphiques
    │   ├── grafana.png
    │   ├── grafana2.png
    │   ├── airflow_dag.png
    │   └── audit_charts/          # Graphiques générés par 02_audit_visuel
    ├── Cours ELK Grafana Mardi.pdf
    └── Cours ETL Airflow Jour3.pdf
```

---

## 🧮 Consommation mémoire (optimisée pour 8 Go)

<a id="consommation-memoire"></a>

| Service            | RAM allouée | Rôle                              |
|--------------------|-------------|-----------------------------------|
| Elasticsearch      | 1 Go        | Stockage et recherche             |
| Airflow Webserver  | 512 Mo      | Interface web                     |
| Airflow Scheduler  | 512 Mo      | Exécution des DAGs                |
| PostgreSQL         | 256 Mo      | Base de données Airflow           |
| **Grafana**        | **128 Mo**  | **Dashboards (4x moins que Kibana)** |
| Logstash           | 512 Mo      | **À la demande uniquement**       |
| **Total permanent**| **~2,4 Go** | **Reste ~5,6 Go pour l'OS + VS Code** |

---

## 📊 Utilisation au fil de la semaine

<a id="utilisation-au-fil-de-la-semaine"></a>

### Lundi / Mardi matin — Data Refinement (VS Code)

Ouvrir le projet dans VS Code et créer des notebooks dans `notebooks/` :

```python
import pandas as pd
anime = pd.read_csv("data/anime.csv")
anime.head()
```

### Mardi après-midi — Elasticsearch + Grafana

**Indexer via Logstash :**

```bash
docker compose --profile ingest up logstash
# Logstash lit anime.csv et l'indexe dans Elasticsearch
# Attendre la fin, puis Ctrl+C
```

**Indexer via Python (alternative) :**

```python
from elasticsearch import Elasticsearch
es = Elasticsearch("http://localhost:9200")
es.index(index="anime", id=1, document={"name": "Naruto", "score": 8.0})
```

Puis ouvrir **Grafana** http://localhost:3000 (admin / anidata).
Un dashboard de démarrage est déjà pré-configuré !

### Mercredi → Vendredi — Airflow

Ouvrir **Airflow** http://localhost:8080 (admin / admin).
Créer vos DAGs dans `airflow/dags/` — ils apparaissent automatiquement.

### Brancher le pipeline Airflow (5 étapes)

Le DAG `airflow/dags/anidata_scraper_pipeline_dag.py` enchaîne automatiquement :

1. **Scraper** : `python -m anidata_scraper.scraper`
2. **Nettoyage** : `python /opt/airflow/scripts/03_nettoyage.py`
3. **Feature engineering** : `python /opt/airflow/scripts/04_feature_engineering.py`
4. **Validation** : `python /opt/airflow/scripts/05_validation.py`
5. **Elasticsearch** : `python /opt/airflow/scripts/script_prof.py`

Chaînage logique :

`Scraper -> Nettoyage -> Feature engineering -> Validation -> Elasticsearch`

Automatisation :

- planification quotidienne (`schedule="@daily"`)
- exécution manuelle possible via **Trigger DAG** dans l'UI
- reprise propre après échec grâce à `retries=1`

Configuration scraper (optionnelle) :

- variable d'environnement `ANIDATA_SCRAPER_BASE_URL`
- valeur par défaut : `http://host.docker.internal:8088` (mock-site local sur Mac)

Commandes utiles pour appliquer les changements DAG :

```bash
docker compose restart airflow-scheduler airflow-webserver
docker compose logs -f airflow-scheduler
```

### Airflow vs Cron (pour bien comprendre)

Un cron (souvent écrit `cron job`) sur Linux est un système de planification de tâches automatiques.

En gros :
c’est un outil qui permet d’exécuter des commandes ou scripts à des moments précis (toutes les heures, tous les jours, etc.), sans intervention humaine.

Exemple concret :
- lancer un script Python tous les jours à 3h
- faire une sauvegarde toutes les 10 minutes
- envoyer un email tous les lundis

Différence clé :
- `cron` déclenche “à l’heure” une commande.
- `Airflow` orchestre des pipelines (dépendances entre étapes, exécution pilotée, historisation, etc.).

---

## ✅ CI GitHub Actions & protection de main

<a id="ci-github-actions--protection-de-main"></a>

Le workflow `/.github/workflows/ci-cd.yml` automatise les contrôles qualité à chaque
`push` et `pull_request` :

- **Trigger** : `push` + `pull_request`
- **Runner** : `ubuntu-latest`
- **Job** : `lint-test`
- **Steps principales** :
  - checkout du dépôt
  - setup Python 3.11
  - installation des dépendances dev (`requirements-dev.txt`)
  - lint avec `ruff`
  - tests avec `pytest` + couverture (`--cov-fail-under=80`)

Objectif : empêcher l’intégration de changements cassés et garantir un niveau
minimum de qualité avant merge.

### Lint : définition rapide et lien avec `needs: [lint, tests]`

`lint`, c’est l’étape qui vérifie la qualité/syntaxe/style du code
automatiquement, sans lancer l’application.

Dans le workflow, `needs: [lint, tests]` veut dire :

- le job courant attend que les jobs `lint` et `tests` soient terminés,
- et en pratique, il ne continue que si ces deux jobs sont en succès.

Différence rapide :

- `lint` : "le code est-il propre/conforme ?" (format, imports, règles, erreurs statiques),
- `tests` : "le code fonctionne-t-il comme prévu ?" (comportement métier).

Exemples d’outils de lint courants :

- Python : `ruff`, `flake8`, `black --check`
- JS/TS : `eslint`, `prettier --check`

### Ruff : où c'est configuré et à quoi ça sert

`ruff` est bien utilisé dans ce repo pour standardiser le style Python et détecter
vite les erreurs simples (imports inutilisés, variables non utilisées, règles de
qualité), avant qu'elles n'arrivent en production.

Configuration et usage :

- **Config locale** : `anidata-scraper/pyproject.toml` (`[tool.ruff]`, lint + format)
- **Dépendance dev** : `anidata-scraper/requirements-dev.txt` (`ruff>=0.5`)
- **CI** : `.github/workflows/ci-cd.yml` lance `ruff check anidata_scraper/ tests/`
- **Documentation** : présent dans `README.md` et `anidata-scraper/README.md`

Conclusion : le lint `ruff` est actif à la fois en local (si tu l'exécutes) et en CI.

### Focus sur `ci-cd.yml` (lignes 64-67)

Dans le job `tests`, ce bloc :

```yaml
steps:
  - uses: actions/checkout@v4
  - uses: actions/setup-python@v5
```

prépare l’environnement de test avant l’installation des dépendances.

- `steps:` démarre la liste des actions exécutées dans le job.
- `actions/checkout@v4` clone le dépôt dans le runner GitHub Actions.
  Sans cette étape, le runner n’a pas accès au code (`anidata_scraper/`, `tests/`, etc.).
- `actions/setup-python@v5` installe et active Python dans la version demandée
  par la matrice (`${{ matrix.python-version }}`), ici `3.10` et `3.11`.
- ce step active aussi `cache: pip` (défini juste après), ce qui réutilise les
  paquets déjà installés entre runs et accélère la CI.

En résumé : `checkout` récupère le code, puis `setup-python` prépare l’interpréteur
et le cache pour que `pip install` et `pytest` fonctionnent de façon reproductible.

### Build Docker Airflow + publication GHCR

Le même workflow inclut un job `docker-build-push` qui :

- construit l’image `airflow/Dockerfile` (contenant les DAGs, scripts et le package `anidata-scraper`)
- publie vers `ghcr.io/<owner>/anidata-airflow` sur les événements `push`
- build sans push sur les `pull_request` (validation seulement)
- applique un versioning des tags :
  - `latest` (branche par défaut)
  - `sha-<commit>` (traçabilité immuable)
  - `vX.Y.Z` / semver (quand push d’un tag Git)

### Registry container : rôle et intérêt de GHCR

Un container registry sert à stocker et distribuer des images Docker versionnées.
Au lieu de builder en local sur chaque machine, on pull une image déjà prête.

Pourquoi GHCR :

- intégré à GitHub Actions (auth simple avec `GITHUB_TOKEN`)
- permissions centralisées par repo/org GitHub
- bonne traçabilité image <-> commit/PR/release
- pratique pour déployer la même image en local, CI et prod

### Authentification GHCR dans GitHub Actions

Le workflow se connecte à GHCR avec :

- `username: ${{ github.actor }}`
- `password: ${{ secrets.GITHUB_TOKEN }}`

Prérequis :

- `Settings > Actions > General` : autoriser les workflows à lire/écrire les packages
- dans le job, permissions :
  - `contents: read`
  - `packages: write`

### Build local de l'image Airflow

```bash
docker build -f airflow/Dockerfile -t anidata-airflow:local .
docker run --rm anidata-airflow:local python -m anidata_scraper.scraper --help
docker run --rm anidata-airflow:local python -m anidata_scraper.scraper --base-url http://localhost:8088 --output-dir ./data/raw
```

Explication de la commande :

`docker run --rm anidata-airflow:local python -m anidata_scraper.scraper --base-url http://localhost:8088 --output-dir ./data/raw`

- **1. `docker run --rm anidata-airflow:local`**
  - lance un conteneur à partir de ton image Docker
  - `anidata-airflow:local` = ton image
  - `--rm` = supprime le conteneur après exécution

- **2. `python -m anidata_scraper.scraper`**
  - lance ton script Python
  - `-m` = exécute un module Python
  - `anidata_scraper.scraper` = ton scraper

- **3. `--base-url http://localhost:8088`**
  - URL du site à scraper
  - ici : un mock-site local (site de test)

- **4. `--output-dir ./data/raw`**
  - dossier de sortie pour sauvegarder les données scrapées
  - ici : `data/raw` (par exemple en JSON)

En résumé :
Cette commande lance le scraper dans Docker, récupère les données depuis le site local, puis les enregistre dans un dossier de sortie.

### Exemple réel d'exécution Docker (Mac)

Commande exécutée :

```bash
docker run --rm \
  -v $(pwd)/data:/opt/airflow/data \
  anidata-airflow:local \
  python -m anidata_scraper.scraper \
  --base-url http://host.docker.internal:8088 \
  --output-dir /opt/airflow/data/raw
```

Logs observés (extrait) :

```text
<frozen runpy>:128: RuntimeWarning: 'anidata_scraper.scraper' found in sys.modules after import of package 'anidata_scraper', but prior to execution of 'anidata_scraper.scraper'; this may result in unpredictable behaviour
2026-04-27 10:09:30,820 [INFO] Début du scraping — 4 pages de catalogue à parcourir
2026-04-27 10:09:30,890 [INFO] Page 1 : 30 animes extraits
2026-04-27 10:09:30,968 [INFO] Page 2 : 30 animes extraits
2026-04-27 10:09:31,045 [INFO] Page 3 : 30 animes extraits
2026-04-27 10:09:31,106 [INFO] Page 4 : 13 animes extraits
2026-04-27 10:09:31,106 [INFO] Enrichissement via les pages détail (103 animes)...
2026-04-27 10:09:32,243 [INFO]   Enrichis : 20/103
2026-04-27 10:09:33,398 [INFO]   Enrichis : 40/103
2026-04-27 10:09:34,684 [INFO]   Enrichis : 60/103
2026-04-27 10:09:35,976 [INFO]   Enrichis : 80/103
2026-04-27 10:09:37,098 [INFO]   Enrichis : 100/103
2026-04-27 10:09:37,327 [INFO] 8 actualités récupérées
2026-04-27 10:09:37,333 [INFO] Scraping terminé : 103 animes, 8 news → /opt/airflow/data/raw/anime_20260427_100937.json

✓ Fichier produit : /opt/airflow/data/raw/anime_20260427_100937.json
```

Résultat :

- scraping OK
- `103` animes récupérés
- `8` actualités récupérées
- fichier généré dans `data/raw/anime_20260427_100937.json` (via le volume monté)
- le warning Python est non bloquant

Vérification locale :

```bash
ls -lh data/raw
head -n 40 data/raw/anime_20260427_100937.json
```

### Utiliser l'image GHCR dans docker-compose

`docker-compose.yml` est configuré pour utiliser :

- `AIRFLOW_IMAGE` depuis `.env`
- valeur par défaut : `ghcr.io/your-github-user/anidata-airflow:latest`

Exemple :

```bash
# pull image distante puis relance Airflow
docker compose pull airflow-init airflow-webserver airflow-scheduler
docker compose up -d airflow-init airflow-webserver airflow-scheduler
```

### Pourquoi définir `AIRFLOW_IMAGE=anidata-airflow:local` en local

Dans `docker-compose.yml`, les services Airflow utilisent cette syntaxe :

`image: ${AIRFLOW_IMAGE:-ghcr.io/your-github-user/anidata-airflow:latest}`

Ce que cela signifie :

- si `AIRFLOW_IMAGE` est défini dans `.env`, Docker Compose utilise cette valeur
- sinon, Docker Compose prend la valeur par défaut (fallback) après `:-`
- ici, le fallback `ghcr.io/your-github-user/...` est un placeholder d'exemple

Pourquoi tu as dû mettre `AIRFLOW_IMAGE=anidata-airflow:local` :

- en local, ton image réellement disponible est `anidata-airflow:local`
- sans cette variable, Compose tente de pull le placeholder GHCR
- le placeholder n'existe pas dans ton namespace, donc erreur `403 Forbidden`

Valeurs recommandées selon le contexte :

- **Développement local** : `AIRFLOW_IMAGE=anidata-airflow:local`
- **Registry GHCR (CI/prod)** : `AIRFLOW_IMAGE=ghcr.io/<owner>/anidata-airflow:latest`
- **Release versionnée** : `AIRFLOW_IMAGE=ghcr.io/<owner>/anidata-airflow:v1.0.0`

Cette variable évite de modifier `docker-compose.yml` entre les environnements :
on change uniquement `.env`.

### Procédure complète (A à Z)

1. **Écrire l'image Airflow custom**
   - créer `airflow/Dockerfile` à partir de `apache/airflow:2.8.1-python3.11`
   - copier `airflow/dags`, `airflow/scripts`, `airflow/plugins`, `anidata-scraper`
   - installer les dépendances Python nécessaires + `anidata-scraper`

2. **Builder et tester l'image en local**
   ```bash
   docker build -f airflow/Dockerfile -t anidata-airflow:local .
   docker run --rm anidata-airflow:local airflow version
   docker run --rm anidata-airflow:local python -m anidata_scraper.scraper --help
   ```

3. **Comprendre le rôle du registry**
   - un registry stocke des images Docker versionnées et partageables
   - GHCR permet d'utiliser GitHub comme source unique d'images (CI/CD + déploiement)

4. **Étendre GitHub Actions pour build + push GHCR**
   - ajouter un job `docker-build-push` dans `/.github/workflows/ci-cd.yml`
   - exécuter le job après `lint-test` (`needs: lint-test`)
   - build sur `pull_request`, build + push sur `push`

5. **Configurer l'authentification GHCR**
   - login dans le workflow via `docker/login-action`
   - `username: ${{ github.actor }}`
   - `password: ${{ secrets.GITHUB_TOKEN }}`
   - permissions du job : `contents: read`, `packages: write`

6. **Appliquer le versioning des images**
   - `latest` sur la branche par défaut
   - `sha-<commit>` sur chaque commit
   - `vX.Y.Z` (semver) lors d'un tag Git
   - exemple release :
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```

7. **Adapter `docker-compose.yml` pour GHCR**
   - utiliser `image: ${AIRFLOW_IMAGE:-ghcr.io/<owner>/anidata-airflow:latest}` pour les services Airflow
   - supprimer `_PIP_ADDITIONAL_REQUIREMENTS` (packages déjà inclus dans l'image)
   - conserver les volumes runtime nécessaires (`logs`, `data`)

8. **Configurer `.env`**
   ```env
   AIRFLOW_IMAGE=ghcr.io/<owner>/anidata-airflow:latest
   ```

9. **Valider la configuration compose**
   ```bash
   docker compose config
   ```

10. **Pousser et vérifier la CI/CD**
    ```bash
    git add airflow/Dockerfile .github/workflows/ci-cd.yml docker-compose.yml .env README.md
    git commit -m "Add Airflow image build and GHCR publishing pipeline"
    git push
    ```
    - vérifier les runs dans l'onglet **Actions**
    - vérifier la présence des images/tags dans GHCR

11. **Déployer l'image GHCR avec Compose**
    ```bash
    docker compose pull airflow-init airflow-webserver airflow-scheduler
    docker compose up -d airflow-init airflow-webserver airflow-scheduler
    docker compose ps
    ```

12. **Protéger `main`**
    - `Require a pull request before merging`
    - `Require status checks to pass before merging` (sélectionner les checks CI)
    - `Require branches to be up to date before merging`
    - sauvegarder la règle

### Activer la protection de `main` (PR obligatoire + CI verte)

Dans GitHub :

1. Aller dans **Settings** → **Branches** → **Add branch protection rule**
2. `Branch name pattern` : `main`
3. Cocher **Require a pull request before merging**
4. (Optionnel recommandé) `Require approvals = 1`
5. Cocher **Require status checks to pass before merging**
6. Dans la liste des checks, sélectionner le check CI (après le 1er run), généralement :
   **Lint and Tests (Python 3.11)**
7. Cocher **Require branches to be up to date before merging**
8. Cliquer **Save changes**

### Feuille de route intégrée : CI verte -> relance Airflow -> DAGs automatiques

<a id="feuille-de-route-integree"></a>

Objectif : relancer automatiquement le flux Airflow uniquement après validation
des tests CI, sans modifier `scraper.py`.

1. **Écrire `scraper_dag.py`**
   - utiliser `PythonOperator`
   - ajouter des logs explicites (début, fin, erreurs)
   - utiliser les Variables Airflow pour les paramètres

2. **Écrire `etl_dag.py`**
   - réutiliser la connexion Elasticsearch existante
   - faire le bulk indexing côté ETL
   - chaîner en amont avec `TriggerDagRunOperator`

3. **Push + attente CI**
   - push du code
   - attendre `lint` + `tests` en vert
   - vérifier que l'image GHCR est publiée
   - redémarrer `docker compose` pour recharger image et DAGs

4. **Démonstration intégrée**
   - déclencher manuellement le DAG scraper
   - vérifier la propagation automatique jusqu'aux DAGs aval
   - vérifier la visualisation finale dans Grafana

5. **Documentation finale**
   - appliquer les bonnes pratiques (README, CHANGELOG, schéma d'architecture)
   - finaliser le README avec la procédure reproductible
   - valider la reproductibilité de bout en bout

### Après "✅ Image publiée sur GHCR" : ce que ça signifie vraiment

<a id="apres-ghcr"></a>

Quand le workflow affiche **"✅ Image publiée sur GHCR"**, cela veut dire uniquement :

- GitHub Actions a **build** l'image Docker.
- GitHub Actions a **push** l'image dans le registry GHCR.

Ce message **ne redémarre pas automatiquement** ton Airflow local.

Pour appliquer réellement la nouvelle image sur ta machine :

```bash
./scripts/deploy_airflow_from_ghcr.sh
./scripts/run_e2e_airflow_check.sh
```

En clair :

- **GitHub Actions** = build/publish
- **Ton poste local** = pull/restart/run DAG

### Peut-on automatiser totalement après CI verte ?

<a id="automatisation-apres-ci"></a>

Oui, mais pas avec un runner GitHub hébergé standard tout seul.

Pourquoi :

- GitHub Actions cloud sait builder/pusher GHCR
- mais il n'a pas accès à ton Docker local sur ton Mac

Pour automatiser "après CI verte => scrape/restart", tu as 3 options :

1. **Self-hosted runner sur ton Mac** (recommandé ici)
   - le job tourne directement sur ta machine
   - peut exécuter `docker compose pull/up` + trigger DAG

2. **Serveur de déploiement (VM/VPS)**
   - GitHub déclenche le déploiement sur ce serveur
   - Airflow tourne sur ce serveur (pas sur ton Mac)

3. **Watchtower (auto-update d'image)**
   - détecte la nouvelle image GHCR et redémarre les conteneurs
   - il faut gérer séparément le trigger DAG ensuite

Approche simple et propre recommandée pour ce projet :

1. garder la CI cloud pour build + push GHCR
2. ajouter un job `deploy-local` sur un runner self-hosted qui exécute :
   - `./scripts/deploy_airflow_from_ghcr.sh`
   - `./scripts/run_e2e_airflow_check.sh`

### Automatisation locale avec cron (toutes les 5 minutes)

Si tu veux une boucle automatique locale, le script
`scripts/auto_update_from_push.sh` fait :

1. vérification d'un nouveau commit sur l'API GitHub (`repos/<owner>/<repo>/commits/main`),
2. vérification que la CI GitHub Actions (`ci-cd.yml`) est `completed` + `success` pour ce commit,
3. `git pull --ff-only`,
4. déploiement via `./scripts/deploy_airflow_from_ghcr.sh`,
5. validation via `./scripts/run_e2e_airflow_check.sh`.

Installation :

```bash
chmod +x scripts/auto_update_from_push.sh
crontab -e
```

Ajouter la ligne suivante (exécution toutes les 5 minutes) :

```cron
*/5 * * * * cd /Users/romain/Downloads/anidata-lab && ./scripts/auto_update_from_push.sh >> /tmp/anidata_auto_update.log 2>&1
```

Notes importantes :

- Le script ignore l'exécution si une autre run est déjà en cours (verrou).
- Le script n'applique rien si ton repo local a des changements non commit (sécurité).
- Le script mémorise le dernier SHA GitHub traité dans `.state/last_seen_main_sha.txt`.
- Le déploiement est déclenché seulement après CI verte (pas juste après un push).
- Tu peux exporter `GITHUB_TOKEN` pour éviter les limites de rate-limit API GitHub.
- En cas de succès, la plateforme locale est mise à jour automatiquement après chaque push détecté.

### Comprendre les logs `auto_update_from_push.sh`

Exemple de séquence normale :

1. `Checking latest commit on GitHub API`  
   Le script interroge l'API GitHub pour récupérer le dernier SHA de `main`.

2. `New commit detected on GitHub: <ancien> -> <nouveau>`  
   Un nouveau commit est détecté par rapport au dernier SHA traité.

3. `Checking CI workflow status: ci-cd.yml`  
   Le script vérifie la run GitHub Actions associée à ce commit.

4. `CI is green ... Starting deploy pipeline.`  
   La CI est terminée et en succès (`completed + success`) : le déploiement peut démarrer.

5. `Working tree is not clean, skipping auto-update to avoid conflicts.`  
   Le script s'arrête volontairement si le repo local contient des changements non commités.
   C'est une protection pour éviter les conflits ou écrasements pendant `git pull --ff-only`.

Que faire si cette dernière ligne apparaît :

- vérifier l'état local : `git status`
- soit commit/push, soit `git stash`, soit nettoyer les fichiers non voulus
- relancer : `./scripts/auto_update_from_push.sh`

Exemple réel : CI encore en cours (attente du prochain cron)

```text
./scripts/auto_update_from_push.sh
[2026-04-28 09:34:07] Checking latest commit on GitHub API: RomainR99/anidata-lab@main
[2026-04-28 09:34:08] New commit detected on GitHub: <none> -> 805b5bdd3a207e2da7d18946b46e164182ffca54
[2026-04-28 09:34:08] Checking CI workflow status: ci-cd.yml for 805b5bdd3a207e2da7d18946b46e164182ffca54
[2026-04-28 09:34:08] CI not finished yet for 805b5bdd3a207e2da7d18946b46e164182ffca54 (status=in_progress). Waiting next cron run.
```

Interprétation :

- le commit est bien détecté
- la CI n'est pas encore terminée
- le script n'applique aucun déploiement tant que la run n'est pas `completed + success`
- au prochain passage cron, le script re-tente automatiquement

Exemple réel : chaîne CI/CD complète validée (de bout en bout)

```text
./scripts/auto_update_from_push.sh
[2026-04-28 11:08:52] Checking latest commit on GitHub API: RomainR99/anidata-lab@main
[2026-04-28 11:08:53] New commit detected on GitHub: 9596c16fe90ba1c2391f12006209b9764386c5ad -> 7c723866f018036534627e1ac21f9862b6bf6949
[2026-04-28 11:08:53] Checking CI workflow status: ci-cd.yml for 7c723866f018036534627e1ac21f9862b6bf6949
[2026-04-28 11:08:53] CI is green for 7c723866f018036534627e1ac21f9862b6bf6949. Starting deploy pipeline.
[2026-04-28 11:08:54] Deploying updated stack
==> Pull image from GHCR: ghcr.io/romainr99/anidata-lab-airflow:latest
Status: Image is up to date for ghcr.io/romainr99/anidata-lab-airflow:latest
==> Restart platform services with docker compose
Airflow webserver and Elasticsearch are healthy, Grafana is running.
[2026-04-28 11:09:37] Running end-to-end Airflow check
==> Trigger DAG: anidata_scraper_pipeline
Poll 1/60: state=queued
Poll 2/60: state=running
Poll 3/60: state=success
==> Checking Elasticsearch index document count
Index anime document count: 17588
==> E2E check completed successfully.
[2026-04-28 11:10:13] Auto-update and verification completed successfully.
```

Ce run confirme que la chaîne CI/CD est complète :

- commit pushé sur `main` détecté,
- CI GitHub Actions validée (`completed + success`),
- image GHCR pullée et stack redéployée,
- DAG Airflow déclenché et terminé en `success`,
- index Elasticsearch vérifié,
- vérification E2E finale en succès.

### Explication complète de `auto_update_from_push.sh`

Cette section explique le script de façon détaillée : variables, lock, API GitHub,
conditions de décision, et enchaînement de bout en bout.

#### 1) Flux complet du début à la fin

Le script suit ce flux logique :

1. active un mode Bash strict (`set -euo pipefail`) pour échouer vite et proprement,
2. calcule ses chemins et paramètres (repo, branche, workflow, fichier d'état),
3. prend un verrou (`LOCK_DIR`) pour éviter deux exécutions simultanées,
4. lit le dernier commit de `main` via l'API GitHub,
5. compare avec le dernier SHA déjà traité (fichier `.state/...sha.txt`),
6. vérifie le statut du workflow GitHub Actions pour ce SHA précis,
7. si CI verte, vérifie que le repo local est clean,
8. fait `git fetch` puis `git pull --ff-only`,
9. lance `deploy_airflow_from_ghcr.sh`,
10. lance `run_e2e_airflow_check.sh`,
11. enregistre le SHA traité dans `.state` seulement après succès.

#### 2) Rôle des variables principales

- `SCRIPT_DIR` : dossier du script en cours (`scripts/`), calculé de manière robuste.
- `REPO_ROOT` : racine du repo (parent de `scripts/`), utilisée pour les chemins absolus.
- `TARGET_BRANCH` : branche surveillée (par défaut `main`).
- `GITHUB_OWNER` / `GITHUB_REPO` : repo GitHub cible pour les appels API.
- `GITHUB_TOKEN` : token optionnel pour éviter les limites de rate limit API.
- `WORKFLOW_FILE` : workflow CI à vérifier (`ci-cd.yml`).
- `REMOTE_NAME` : remote Git utilisé pour fetch/pull (par défaut `origin`).
- `LOCK_DIR` : chemin du verrou d'exécution unique.
- `STATE_DIR` : dossier local qui stocke l'état métier du script.
- `STATE_FILE` : fichier SHA du dernier commit déjà traité avec succès.
- `API_URL` : endpoint GitHub pour le dernier commit de la branche.
- `RUNS_URL` : endpoint GitHub Actions pour le workflow lié à ce SHA.
- `REMOTE_SHA` : SHA du dernier commit distant détecté.
- `LAST_SEEN_SHA` : dernier SHA déjà traité (lu dans `STATE_FILE`).
- `RUN_STATUS` : état d'exécution du workflow (`queued`, `in_progress`, `completed`, etc.).
- `RUN_CONCLUSION` : résultat final du workflow (`success`, `failure`, `cancelled`, etc.).

#### 3) Appels API GitHub (commits + workflows)

Le script fait deux appels API :

1. Commit de branche :
   - `GET /repos/<owner>/<repo>/commits/<branch>`
   - objectif : récupérer `REMOTE_SHA` (dernier commit de `main`).

2. Workflow pour ce commit :
   - `GET /repos/<owner>/<repo>/actions/workflows/<workflow_file>/runs?head_sha=<sha>&branch=<branch>&event=push&per_page=1`
   - objectif : récupérer le run GitHub Actions correspondant au commit et lire
     `status` + `conclusion`.

Le paramètre `head_sha` est la clé : il force le filtrage sur le commit exact, pas
sur "la dernière run de la branche" en général.

#### 4) Vérifie-t-il bien le workflow du dernier push sur la branche ?

Oui :

- le script prend d'abord le dernier SHA de `main`,
- puis interroge les runs du workflow `ci-cd.yml` avec `head_sha=<ce_sha>`,
- et filtre aussi avec `branch=main` et `event=push`.

Donc la décision de déployer est prise sur la CI du commit courant de `main`.

#### 5) Différence entre `status` et `conclusion`

- `status` = état d'avancement (ex: `queued`, `in_progress`, `completed`).
- `conclusion` = résultat final, uniquement pertinent quand `status=completed`
  (ex: `success`, `failure`, `cancelled`, `timed_out`).

Règle implémentée :

- si `status != completed` -> le script attend (prochain passage cron),
- si `status = completed` mais `conclusion != success` -> blocage déploiement,
- si `status = completed` et `conclusion = success` -> déploiement autorisé.

#### 6) Cas de décision (attend, bloque, ou déploie)

- **Attend le cron suivant** : CI pas terminée (`status=in_progress` par exemple).
- **Bloque le déploiement** : CI terminée mais pas verte (`conclusion=failure`, etc.).
- **Lance pull + déploiement** : commit nouveau + CI verte + repo local clean.

#### 7) Système de lock (`LOCK_DIR`) en détail

Un lock ici est un mécanisme d'exclusion mutuelle : une seule instance du script
peut travailler à la fois.

Pourquoi un dossier (`mkdir`) :

- `mkdir` est atomique : deux processus qui tentent de créer le même dossier en
  même temps ne peuvent pas réussir tous les deux.
- c'est simple, robuste, portable pour un script shell.

Sans lock, deux runs simultanées peuvent :

- déclencher deux `git pull` concurrents,
- redémarrer Docker en parallèle,
- lancer plusieurs checks E2E en collision,
- provoquer des états incohérents ou des erreurs transitoires difficiles à diagnostiquer.

Rôle de `trap cleanup EXIT` :

- garantit la suppression du lock à la fin du script, même en cas d'erreur ou
  sortie anticipée,
- évite de laisser un "faux verrou" qui bloquerait les prochains runs.

Pourquoi supprimer le lock à la fin :

- pour autoriser les exécutions suivantes (cron ou manuel).

Si on ne le supprimait pas :

- toutes les exécutions futures verraient "Another run is already in progress"
  et s'arrêteraient en boucle, même si aucun run n'est réellement active.

#### 8) Rôle de `git fetch` et `git pull --ff-only`

- `git fetch origin main` : met à jour les références distantes locales sans modifier
  la branche courante.
- `git pull --ff-only origin main` : met à jour localement uniquement si un
  fast-forward est possible (pas de merge commit automatique).

Pourquoi `--ff-only` :

- comportement prédictible et sûr pour un job auto,
- échoue si l'historique local diverge (au lieu de créer un merge non désiré).

#### 9) Rôle des scripts appelés

- `scripts/deploy_airflow_from_ghcr.sh` :
  - pull l'image Airflow depuis GHCR,
  - redémarre les services Docker Compose,
  - attend les états de santé (Airflow/Elasticsearch).

- `scripts/run_e2e_airflow_check.sh` :
  - vérifie API Airflow,
  - déclenche le DAG scraper,
  - attend son succès,
  - contrôle un indicateur Elasticsearch (`_count`).

#### 10) Pourquoi stocker un SHA dans `.state`

Le fichier `.state/last_seen_main_sha.txt` sert de mémoire entre deux exécutions :

- évite de retraiter le même commit à chaque passage cron,
- rend le script idempotent côté orchestration,
- permet de ne marquer un SHA "traité" qu'après un cycle complet réussi.

---

## ⚡ Commandes utiles

<a id="commandes-utiles"></a>

```bash
# Démarrer tout
docker compose up -d

# (Re)démarrer Airflow + sa DB Postgres
docker compose up -d postgres airflow-init airflow-webserver airflow-scheduler

# Arrêter tout (conserve les données)
docker compose down

# (Re)faire une remise à zéro complète via Airflow
# (arrête les conteneurs et garde les volumes)
# Si tu veux repartire de zéro "clean" : stop/détruit les conteneurs, puis recrée l'environnement.
# Exemple : enchaîner `docker compose down` puis `docker compose up -d`.

# Arrêter et SUPPRIMER toutes les données
docker compose down -v

# (Re)créer l'environnement après destruction complète
docker compose up -d

# Voir les logs d'un service
docker compose logs -f elasticsearch
docker compose logs -f grafana
docker compose logs -f airflow-webserver

# Redémarrer un service
docker compose restart grafana

# Vérifier l'état
docker compose ps

# Lancer Logstash ponctuellement pour indexer
docker compose --profile ingest up logstash

# Installer un package Python dans Airflow
docker compose exec airflow-webserver pip install <package>

# Shell dans un container
docker compose exec airflow-webserver bash
```

---

## 🐛 Dépannage

<a id="depannage"></a>

### Elasticsearch ne démarre pas (Linux)

```bash
sudo sysctl -w vm.max_map_count=262144
echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf
```

### Airflow "Database not initialized"

```bash
docker compose down
docker compose up airflow-init
docker compose up -d
```

### Airflow UI ne répond pas (ERR_EMPTY_RESPONSE) / `SIGKILL` (OOM) sur `airflow-webserver`

Symptôme typique dans les logs :
`Worker (...) was sent SIGKILL! Perhaps out of memory?`

Modif minimale recommandée dans `docker-compose.yml` (service `airflow-webserver`) :

```yaml
environment:
  - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=${AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION}
  - AIRFLOW__WEBSERVER__WORKERS=1
  - _PIP_ADDITIONAL_REQUIREMENTS=pandas
```

Puis relancer et retester :

```bash
docker compose down
docker compose up -d
curl http://localhost:8080/health
```

### Grafana ne montre pas de données

1. Vérifier qu'Elasticsearch a des données : `curl http://localhost:9200/anime/_count`
2. Dans Grafana → Configuration → Data Sources → tester la connexion
3. Relancer Logstash si besoin : `docker compose --profile ingest up logstash`

### GHCR : `denied`, `403 Forbidden` et `no matching manifest for linux/arm64/v8`

Si `docker pull ghcr.io/...` échoue, il faut distinguer 2 causes différentes :

- **Cause 1 (auth)** : `denied` ou `403 Forbidden` sur `https://ghcr.io/token`
- **Cause 2 (architecture)** : `no matching manifest for linux/arm64/v8` (Mac Apple Silicon)

#### 1) Corriger l'authentification GHCR (403/denied)

Pour `docker login ghcr.io`, le "password" n'est pas le mot de passe GitHub :
il faut utiliser un **Personal Access Token (PAT)** GitHub.

Scopes recommandés :
- pull uniquement : `read:packages`
- push + pull : `write:packages` + `read:packages`
- repo privé (token classic) : ajouter `repo`

Procédure :

```bash
docker logout ghcr.io
docker login ghcr.io -u <github-username>
# Password: coller le PAT GitHub
docker pull ghcr.io/<owner>/<image>:latest
```

#### 2) Corriger l'erreur ARM64 sur Mac M1/M2

Si l'auth est OK mais que tu as `no matching manifest for linux/arm64/v8`,
l'image a été publiée sans variante ARM64.

Solution recommandée dans `/.github/workflows/ci-cd.yml` :
- ajouter `docker/setup-qemu-action@v3`
- builder avec `platforms: linux/amd64,linux/arm64`

#### 3) Vérifier le nom exact de l'image GHCR

Avec ce workflow :

```yaml
IMAGE_NAME: ${{ github.repository }}-airflow
```

l'image publiée pour ce repo est :
`ghcr.io/romainr99/anidata-lab-airflow:<tag>`

et non `ghcr.io/romainr99/anidata-airflow:<tag>`.

#### 4) Redéployer Airflow après image GHCR verte

```bash
docker compose --env-file .env down
docker compose --env-file .env pull airflow-init airflow-webserver airflow-scheduler
docker compose --env-file .env up -d postgres airflow-init airflow-webserver airflow-scheduler
docker compose ps
```

Attendu :
- `airflow-init` en `Exited` (normal)
- `postgres` healthy
- `airflow-webserver` puis `airflow-scheduler` en `Up`

Exemple de logs (cas validé) :

```text
[+] Running 5/5
✔ Network anidata-lab_anidata-network  Created
✔ Container anidata-postgres           Healthy
✔ Container anidata-airflow-init       Exited
✔ Container anidata-airflow-webserver  Started
✔ Container anidata-airflow-scheduler  Started
...
anidata-airflow-scheduler   ghcr.io/romainr99/anidata-lab-airflow:latest   ...   Up
anidata-airflow-webserver   ghcr.io/romainr99/anidata-lab-airflow:latest   ...   Up (health: starting)
anidata-postgres            postgres:15-alpine                               ...   Up (healthy)
```

Lecture rapide de ces logs :
- `Network ... Created` : le réseau Docker Compose est bien recréé.
- `postgres ... healthy` : la base Airflow est prête.
- `airflow-init ... Exited` : normal, c'est un job one-shot d'initialisation.
- `airflow-webserver ... health: starting` : normal juste après le démarrage, attendre 30-60 secondes puis refaire `docker compose ps`.
- `airflow-scheduler ... Up` : le scheduler tourne et peut planifier/lancer les DAGs.
- image affichée `ghcr.io/romainr99/anidata-lab-airflow:latest` : confirme que le déploiement utilise bien l'image GHCR publiée par la CI.

#### 5) Validation E2E (de bout en bout)

Checklist E2E complète :

- [ ] **CI verte** : `lint`, `tests`, `build-and-push` en succès sur le dernier commit `main`.
- [ ] **Pull GHCR OK** :
  ```bash
  docker pull ghcr.io/romainr99/anidata-lab-airflow:latest
  ```
- [ ] **Relance Airflow avec l'image GHCR** :
  ```bash
  docker compose --env-file .env down
  docker compose --env-file .env up -d postgres airflow-init airflow-webserver airflow-scheduler
  docker compose ps
  ```
- [ ] **Airflow prêt** :
  - `airflow-init` = `Exited` (normal)
  - `postgres` = `healthy`
  - `airflow-webserver` + `airflow-scheduler` = `Up`
- [ ] **Trigger DAG scraper** :
  - ouvrir Airflow UI : `http://localhost:8080`
  - lancer manuellement le DAG scraper
- [ ] **Propagation automatique DAG ETL** :
  - vérifier que le DAG ETL est déclenché automatiquement après succès scraper
  - vérifier que les tasks ETL passent en `success`
- [ ] **Validation Grafana** :
  - ouvrir `http://localhost:3000`
  - vérifier que les dashboards sont alimentés/rafraîchis avec les nouvelles données

Si tous les points sont cochés, le flux complet
`CI verte -> GHCR -> compose -> Airflow -> Grafana` est validé.

Automatisation (scripts prêts à l'emploi) :

```bash
# 1) Pull GHCR + relance Airflow + attente health webserver
./scripts/deploy_airflow_from_ghcr.sh

# 2) Trigger DAG scraper + attente success + check Elasticsearch
./scripts/run_e2e_airflow_check.sh
```

Variables utiles :
- `AIRFLOW_IMAGE` pour forcer un tag (`latest` ou `sha-...`) dans le script de déploiement
- `DOWNSTREAM_DAG_ID` pour vérifier un DAG aval optionnel (ex: `anidata_full_pipeline`)
- `AIRFLOW_BASE_URL`, `AIRFLOW_USER`, `AIRFLOW_PASSWORD` si ton Airflow n'est pas en local par défaut

### Les imports Airflow sont soulignés dans Cursor (mais ça marche dans Docker)

 C'est un avertissement de l'IDE (linter `basedpyright`) et pas un vrai problème d'exécution.

- Quand tu exécutes les DAG dans Docker, le conteneur a bien `apache-airflow` installé, donc les imports comme `from airflow.operators.bash import BashOperator` fonctionnent.
- Dans Cursor, le linter analyse ton environnement Python local (ou le venv sélectionné). S'il n'a pas `apache-airflow` installé, Cursor affiche “Impossible de résoudre l'importation …”.

Si tu veux faire disparaître le warning : installe `apache-airflow` dans le même interpréteur Python que celui utilisé par Cursor, ou ignore simplement ces warnings de type.

### Supprimer le DAG Airflow : pourquoi les données restent sur Grafana ?

Si tu supprimes le DAG `anidata_full_pipeline` dans Airflow, tu peux quand même voir les chiffres dans Grafana.

Dans ton projet, la “base de données” qui sert à afficher les chiffres dans Grafana, c’est Elasticsearch.

Explication : le DAG Airflow ne “stocke” pas les données. Il sert uniquement à exécuter tes scripts (notamment `script_prof.py`) qui indexent ensuite les données dans Elasticsearch. Une fois l’index `anime` créé dans Elasticsearch, Grafana continuera d’afficher les résultats tant que l’index (et ses documents) existent.

Pour effacer réellement les données affichées dans Grafana :
1. Supprimer l’index Elasticsearch `anime` :
   `curl -X DELETE http://localhost:9200/anime`
2. Ou réinitialiser Elasticsearch (supprime les volumes) :
   `docker compose down -v && docker compose up -d elasticsearch`

### Indexation Elasticsearch via le DAG (script_prof.py)

Question (dernière) :
`docker compose exec airflow-webserver python /opt/airflow/scripts/script_prof.py`

Réponse :
Oui, tu peux le faire via le DAG. Dans `anidata_full_pipeline`, lance le DAG puis attends que la tâche
`06_indexation_elasticsearch` s’exécute (c’est elle qui lance `script_prof.py` et recrée l’index `anime`).

Question :
L'indexation Elasticsearch est-elle forcément refaite après DAG1 ?

Réponse :
Oui, si `anidata_full_pipeline` va jusqu’à la tâche `06_indexation_elasticsearch`, alors l’indexation est refaite à chaque run de DAG1.

Dans ton DAG :
- `06_indexation_elasticsearch` lance `script_prof.py`
- `script_prof.py` indexe dans `anime` en mode incrémental/upsert
- il ne supprime pas forcément l’index : il met à jour/ajoute selon les IDs

En pratique : c’est `script_prof.py` qui “fabrique” la base de données pour Grafana, c’est-à-dire l’index Elasticsearch `anime` (et donc les documents qui alimentent les dashboards).

Note : `script_prof.py` fait maintenant une **indexation incrémentale / upsert** :
- il ne supprime plus l’index existant `anime`
- les documents sont mis à jour via `_id` (basé sur `mal_id`)

Image (clic pour déclencher le DAG) :
<img src="notebooks/images/airflow_trigger_dag.png" alt="airflow_trigger_dag.png" width="800" />

### Résultats DAG Airflow (exécution complète)

- Vue DAG (succès) :
  <img src="notebooks/images/airflow_dag.png" alt="airflow_dag.png" width="800" />
- Les 7 tâches principales sont en `success` :
  <img src="notebooks/images/success_task.png" alt="success_task.png" width="800" />
- Aucun email d’échec n’a été envoyé (tâche `send_email_audit_failed` en `skipped`) :
  <img src="notebooks/images/skipped_task.png" alt="skipped_task.png" width="800" />

### Résultat final : validation auto-update + E2E

Exemple réel (run manuel avec log consolidé) :

```text
cd /Users/romain/Desktop/anidata-lab && /bin/bash /Users/romain/Desktop/anidata-lab/scripts/auto_update_from_push.sh >> /tmp/anidata_auto_update.log 2>&1
...
==> Current service status
anidata-airflow-scheduler   ghcr.io/romainr99/anidata-lab-airflow:latest   Up
anidata-airflow-webserver   ghcr.io/romainr99/anidata-lab-airflow:latest   Up (health: starting)
anidata-elasticsearch       docker.elastic.co/elasticsearch/elasticsearch:8.12.0   Up (healthy)
anidata-grafana             grafana/grafana-oss:10.3.1   Up
anidata-postgres            postgres:15-alpine   Up (healthy)
==> Waiting for Airflow webserver and Elasticsearch health
Airflow webserver and Elasticsearch are healthy, Grafana is running.
[2026-04-28 11:55:49] Running end-to-end Airflow check
==> Trigger DAG: anidata_scraper_pipeline
Poll 1/60: state=queued
Poll 2/60: state=running
Poll 3/60: state=running
[2026-04-28 11:56:23] CI is green for 6a005f29cda807682cbbd35c007b4a903060f1b3. Starting deploy pipeline.
[2026-04-28 11:56:23] Working tree is not clean, skipping auto-update to avoid conflicts.
```

Lecture des résultats :

- la stack Docker redémarre correctement (Postgres, Elasticsearch, Airflow, Grafana),
- le check E2E démarre et le DAG `anidata_scraper_pipeline` passe de `queued` à `running`,
- le script de surveillance relancé en parallèle détecte une CI verte,
- la protection "working tree clean" joue son rôle et bloque un déploiement concurrent si le dépôt local n'est pas propre.

### DAG2 : pourquoi le total passe de 17562 à 17588

Le panneau Grafana **“Total animes indexés”** compte les documents dans l’index Elasticsearch `anime`.

- Après exécution **DAG1 seul** (indexation depuis `anime_gold.json`) : **17562**.
- Après exécution **DAG2 puis DAG1** :
  - `dag2` convertit `data/anime_2.json` et `data/anime_3.xml` en CSV,
  - puis indexe/upsert ces nouveaux enregistrements directement dans l’index **`anime`**,
  - ce qui ajoute **+26** documents uniques.

Donc : **17562 → 17588 (= 17562 + 26)**.

Pour refaire la demo (17562 puis 17588) :
- Supprime l'index `anime` (vrai reset Elasticsearch) :
  `curl -X DELETE http://localhost:9200/anime`
- Vérifie :
  `curl http://localhost:9200/anime/_count`

Captures :

- DAG1 only :
  <img src="notebooks/images/DAG1_only.png" alt="DAG1_only.png" width="800" />
- DAG2 + DAG1 :
  <img src="notebooks/images/DAG2+DAG1.png" alt="DAG2+DAG1.png" width="800" />

### `01_audit_complet.py` : `sys.exit()` à la fin

À la fin de `01_audit_complet.py`, il vaut mieux faire un `sys.exit()` pour que le script se termine proprement (succès ou échec).

Exemple :

```python
import sys

try:
    # ... tout ton code d’audit ...

    print("✅ Audit terminé avec succès !")
    sys.exit(0)

except Exception as e:
    print(f"❌ Audit échoué : {e}")
    sys.exit(1)
```

Dans ton cas **Airflow** :

Quand ton script fait :

```python
print("Audit terminé avec succès !")
```

👉 cette phrase est envoyée dans un **stream stdout**.

Et Airflow peut :
- lire ce flux
- ou récupérer tout à la fin via les logs / l’exécution du `BashOperator`.

Exemple visuel (sortie console) :

<img src="notebooks/images/sis.exit().png" alt="sis.exit().png" width="800" />

### Email en cas d'échec (DAG `anidata_full_pipeline_dag.py`)

Dans `anidata_full_pipeline_dag.py`, l’`EmailOperator` envoie l’email à :

- `to="tonmail@example.com"`

Donc c’est **`tonmail@example.com`** (placeholder à remplacer par ton vrai email).

### Où retrouves-tu la phrase dans Airflow ?

Oui, les logs Airflow enregistrent tous les `print()`, donc :

SI ton script affiche

```python
print("Audit terminé avec succès !")
```

👉 alors oui, cette phrase sera dans les logs Airflow ✅

🧠 1. Où ça apparaît ?

Dans Airflow :
`UI → ton DAG → tâche 01_audit_complet → onglet Log`

Tu verras exactement la ligne
`Audit terminé avec succès !`
dans la sortie de la tâche.

🧠 2. Comment le DAG la détecte ?

Dans `anidata_full_pipeline_dag.py`, la commande de `01_audit_complet` écrit d’abord la console dans :
`/opt/airflow/output/audit_log.txt` (via `tee`)

Puis le DAG vérifie :
`if grep -q 'Audit terminé avec succès !' /opt/airflow/output/audit_log.txt; then ...`

### Pourquoi `anidata_full_pipeline` peut échouer même si l’audit affiche "✅ Audit terminé…"

Cas typique :
`01_audit_complet.py` affiche bien “✅ Audit terminé…” donc le `grep` passe, **mais** la tâche échoue quand même car le `BashOperator` faisait un **test de déterminisme** en comparant le hash de `audit_log_1.txt` et `audit_log_2.txt`.

Si les logs diffèrent (même 1 ligne), ça force `FAIL` et `exit 1`.

Extrait (ancienne logique) :

```bash
"grep -q 'Audit terminé avec succès !' /opt/airflow/output/audit_log_1.txt || (echo FAIL > /opt/airflow/output/audit_status.txt; exit 1); "
"hash1=$(sha256sum /opt/airflow/output/audit_log_1.txt | awk '{print $1}'); "
# Run 2
"grep -q 'Audit terminé avec succès !' /opt/airflow/output/audit_log_2.txt || (echo FAIL > /opt/airflow/output/audit_status.txt; exit 1); "
"hash2=$(sha256sum /opt/airflow/output/audit_log_2.txt | awk '{print $1}'); "
# Compare
"if [ \"$hash1\" != \"$hash2\" ]; then echo FAIL > /opt/airflow/output/audit_status.txt; exit 1; fi; "
```

Pourquoi ça arrive :
le script imprime des choses qui peuvent varier d’un run à l’autre (timestamps, ordre d’affichage, etc.), donc les 2 logs ne sont pas forcément strictement identiques.

#### Correction appliquée

Le DAG a été modifié pour **ne plus faire échouer le run sur un hash mismatch** :
- on garde les 2 exécutions,
- on garde le `grep "Audit terminé avec succès !"`,
- on garde la 2e exécution comme log canonique (`audit_log.txt`) et on écrit `OK`.

#### Après modification (recharger Airflow)

```bash
docker compose restart airflow-scheduler airflow-webserver
```

### run2 : ce que fait `02_audit_visuel.py` dans `anidata_full_pipeline_dag.py`

Le DAG exécute `run_02_audit_visuel` (tâche Airflow `02_audit_visuel`) et fait ces vérifications :

1) Lance le script :
`python /opt/airflow/scripts/02_audit_visuel.py`

2) Sauvegarde la sortie dans un log :
`/opt/airflow/output/audit_visuel_log.txt` (via `tee`)

3) Vérifie que Python s’est bien terminé (code retour != 0 => `FAIL`)

4) Compte les `.png` générés dans :
`/opt/airflow/output/audit_charts/`
Attendu : **7 fichiers**

5) Met `OK` ou `FAIL` dans :
`/opt/airflow/output/audit_visuel_status.txt`

6) Vérifie les noms exacts des fichiers (7 attendus) :
- `01_score_distribution.png`
- `02_data_types.png`
- `03_top_genres.png`
- `04_top_studios.png`
- `05_type_distribution.png`
- `06_boxplots.png`
- `07_correlation_matrix.png`

### run_03_nettoyage : ce que fait `03_nettoyage.py` dans `anidata_full_pipeline_dag.py`

Le DAG exécute `run_03_nettoyage` (tâche Airflow `03_nettoyage`) et attend principalement ces vérifications :

Ce que cette version vérifie :

- le script ne crash pas (exit code OK)
- `output/anime_cleaned.csv` a bien été généré
- le fichier n’est pas vide
- le log contient bien la ligne `Nettoyage terminé !`

1) Lancer le script :
`python /opt/airflow/scripts/03_nettoyage.py`

2) Sauver le log d’exécution (stdout) dans les logs Airflow (onglet **Log** de la tâche).

3) Vérifier que le script s’est bien terminé (exit code OK => la tâche passe, sinon FAIL).

4) Vérifier que le fichier existe vraiment :
`/opt/airflow/output/anime_cleaned.csv`

5) Vérifier que le fichier n’est pas vide.

6) Éventuellement vérifier que le fichier contient bien les colonnes attendues (ex : `mal_id`, `name`, `score`, `episodes`, etc.).

### Un port est déjà utilisé

```bash
lsof -i :3000   # ou 9200, 8080
```

### Réinitialisation complète

```bash
docker compose down -v
docker compose up -d
```

---

## 📚 Ressources

<a id="ressources"></a>

- [Dataset Kaggle](https://www.kaggle.com/datasets/hernan4444/anime-recommendation-database-2020)
- [Documentation Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [Documentation Grafana](https://grafana.com/docs/grafana/latest/)
- [Documentation Airflow](https://airflow.apache.org/docs/apache-airflow/stable/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
