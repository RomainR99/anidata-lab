"""
🎌 AniData Lab — Indexation dans Elasticsearch
================================================
Séance 3 — Mardi 24 mars 2026 — Après-midi

Ce script :
  1. Vérifie la connexion à Elasticsearch
  2. Crée l'index "anime" avec un mapping optimisé
  3. Indexe le dataset anime_gold.json en bulk
  4. Vérifie l'indexation avec des requêtes de test
  5. Affiche des statistiques

Usage : python airflow/scripts/script_prof.py
Prérequis : pip install elasticsearch pandas
Entrée : output/anime_gold.json (généré par 05_validation.py)
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    from elasticsearch import Elasticsearch, helpers
except ImportError:
    print("❌ Module 'elasticsearch' non installé.")
    print("   Installez-le avec : pip install elasticsearch")
    sys.exit(1)

# ============================================
# CONFIG
# ============================================
ES_HOST = None  # defaultisé après détection local vs Docker
INDEX_NAME = "anime"
BULK_CHUNK_SIZE = 500

AIRFLOW_BASE_DIR = "/opt/airflow"
if os.path.exists(os.path.join(AIRFLOW_BASE_DIR, "data")):
    BASE_DIR = AIRFLOW_BASE_DIR
else:
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

if ES_HOST is None:
    # Dans Docker Compose, Elasticsearch est joignable via le nom de service.
    # En local (hors docker), on utilise localhost.
    ES_HOST = (
        "http://elasticsearch:9200"
        if BASE_DIR == AIRFLOW_BASE_DIR
        else "http://localhost:9200"
    )

INPUT_FILE = os.path.join(BASE_DIR, "output", "anime_gold.json")
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")

class C:
    H = "\033[95m"; B = "\033[94m"; G = "\033[92m"
    W = "\033[93m"; F = "\033[91m"; BOLD = "\033[1m"
    CYAN = "\033[96m"; END = "\033[0m"

def titre(t):
    print(f"\n{C.BOLD}{C.H}{'='*60}\n  {t}\n{'='*60}{C.END}\n")

def step(t):
    print(f"\n{C.BOLD}{C.B}--- {t} ---{C.END}")

def ok(t):
    print(f"  {C.G}✅ {t}{C.END}")

def warn(t):
    print(f"  {C.W}⚠️  {t}{C.END}")

def fail(t):
    print(f"  {C.F}❌ {t}{C.END}")

def info(t):
    print(f"  {C.B}ℹ️  {t}{C.END}")

def load_json_documents(path):
    """Charge un fichier JSON au format NDJSON ou tableau JSON."""
    with open(path, "r", encoding="utf-8") as f:
        first_non_ws = ""
        while True:
            ch = f.read(1)
            if not ch:
                break
            if not ch.isspace():
                first_non_ws = ch
                break
        f.seek(0)

        # Format JSON standard: [ {...}, {...} ]
        if first_non_ws == "[":
            data = json.load(f)
            if not isinstance(data, list):
                raise ValueError("Le fichier JSON doit contenir une liste de documents.")
            return data

        # Format NDJSON: 1 objet JSON par ligne
        docs = []
        for line in f:
            line = line.strip()
            if not line:
                continue
            docs.append(json.loads(line))
        return docs


def load_latest_scraper_documents(raw_dir):
    """
    Charge le dernier fichier raw/anime_*.json du scraper et le convertit
    vers des documents indexables en complément du dataset gold.
    """
    raw_path = None
    try:
        candidates = sorted(Path(raw_dir).glob("anime_*.json"))
        if candidates:
            raw_path = str(candidates[-1])
    except Exception:
        raw_path = None

    if not raw_path:
        return [], None

    with open(raw_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    animes = payload.get("animes", [])
    scraped_at = payload.get("scraped_at")
    docs = []
    for a in animes:
        raw_id = a.get("id")
        if raw_id is None:
            # Skip entries without stable id to avoid duplicates across runs
            continue

        # Offset IDs to avoid collision with MAL ids from anime_gold.json
        synthetic_id = 10_000_000 + int(raw_id)
        genres = a.get("genres") or []
        main_genre = genres[0] if genres else None

        docs.append(
            {
                "mal_id": synthetic_id,
                "name": a.get("title_en"),
                "japanese_name": a.get("title_jp"),
                "score": a.get("score"),
                "type": a.get("type"),
                "episodes": a.get("episodes"),
                "year": a.get("year"),
                "main_studio": a.get("studio"),
                "studios": a.get("studio"),
                "genres": genres,
                "main_genre": main_genre,
                "detail_url": a.get("detail_url"),
                "status": a.get("status"),
                "synopsis": a.get("synopsis"),
                "source_dataset": "scraper_raw",
                "@timestamp": scraped_at or datetime.now(timezone.utc).isoformat(),
            }
        )

    return docs, raw_path


# ============================================
# MAPPING ELASTICSEARCH
# ============================================
ANIME_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                "anime_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            # --- Identifiants ---
            "mal_id":           { "type": "integer" },

            # --- Noms (recherche full-text) ---
            "name":             { "type": "text", "analyzer": "anime_analyzer",
                                  "fields": { "keyword": { "type": "keyword" } } },
            "english_name":     { "type": "text", "analyzer": "anime_analyzer" },
            "japanese_name":    { "type": "text" },

            # --- Scores ---
            "score":            { "type": "float" },
            "weighted_score":   { "type": "float" },
            "score_category":   { "type": "keyword" },

            # --- Catégories ---
            "type":             { "type": "keyword" },
            "source":           { "type": "keyword" },
            "rating":           { "type": "keyword" },

            # --- Genres ---
            "genres":           { "type": "keyword" },
            "main_genre":       { "type": "keyword" },
            "n_genres":         { "type": "integer" },

            # --- Studios ---
            "studios":          { "type": "keyword" },
            "main_studio":      { "type": "keyword" },
            "studio_tier":      { "type": "keyword" },

            # --- Statistiques numériques ---
            "episodes":         { "type": "integer" },
            "members":          { "type": "long" },
            "favorites":        { "type": "long" },
            "popularity":       { "type": "integer" },
            "ranked":           { "type": "float" },

            # --- Métriques calculées ---
            "drop_ratio":       { "type": "float" },
            "engagement_ratio": { "type": "float" },
            "duration_minutes": { "type": "integer" },

            # --- Compteurs de statut ---
            "watching":         { "type": "long" },
            "completed":        { "type": "long" },
            "on_hold":          { "type": "long" },
            "dropped":          { "type": "long" },
            "plan_to_watch":    { "type": "long" },

            # --- Temporel ---
            "aired":            { "type": "text" },
            "aired_start":      { "type": "date", "format": "yyyy-MM-dd||epoch_millis||strict_date_optional_time", "ignore_malformed": True },
            "aired_end":        { "type": "date", "format": "yyyy-MM-dd||epoch_millis||strict_date_optional_time", "ignore_malformed": True },
            "premiered":        { "type": "keyword" },
            "year":             { "type": "integer" },
            "decade":           { "type": "integer" },
            "decade_start":     { "type": "date" },

            # --- Scores détaillés ---
            "score_10":         { "type": "long" },
            "score_9":          { "type": "long" },
            "score_8":          { "type": "long" },
            "score_7":          { "type": "long" },
            "score_6":          { "type": "long" },
            "score_5":          { "type": "long" },
            "score_4":          { "type": "long" },
            "score_3":          { "type": "long" },
            "score_2":          { "type": "long" },
            "score_1":          { "type": "long" },

            # --- Flags ---
            "is_outlier":       { "type": "boolean" },

            # --- Autres textes ---
            "duration":         { "type": "keyword" },
            "producers":        { "type": "keyword" },
            "licensors":        { "type": "keyword" },
        }
    }
}


# ============================================
# 1. VÉRIFIER LA CONNEXION
# ============================================
titre("INDEXATION ELASTICSEARCH")

step("Étape 1 : Connexion à Elasticsearch")

print(f"  Tentative de connexion à {ES_HOST}...")

es = Elasticsearch(ES_HOST, request_timeout=30)

# Attendre que ES soit prêt (peut prendre du temps au démarrage)
max_retries = 10
for attempt in range(max_retries):
    try:
        health = es.cluster.health()
        cluster_name = health.get("cluster_name", "?")
        status = health.get("status", "?")
        ok(f"Connecté au cluster '{cluster_name}' (status: {status})")
        break
    except Exception as e:
        if attempt < max_retries - 1:
            print(f"  ⏳ ES pas encore prêt (tentative {attempt + 1}/{max_retries})... attente 5s")
            time.sleep(5)
        else:
            fail(f"Impossible de se connecter à {ES_HOST}")
            print(f"  Erreur : {e}")
            print(f"\n  Vérifiez que Docker tourne :")
            print(f"    docker compose ps")
            print(f"    docker compose logs elasticsearch")
            sys.exit(1)


# ============================================
# 2. VÉRIFIER LE FICHIER D'ENTRÉE
# ============================================
step("Étape 2 : Vérification du fichier source")

if not os.path.exists(INPUT_FILE):
    fail(f"Fichier introuvable : {INPUT_FILE}")
    print("  Lancez d'abord les scripts de pipeline (dans l'ordre) :")
    print("    python airflow/dags/03_nettoyage.py")
    print("    python airflow/dags/04_feature_engineering.py")
    print("    python airflow/dags/05_validation.py")
    sys.exit(1)

# Charger les documents (compat NDJSON + JSON array)
try:
    docs = load_json_documents(INPUT_FILE)
except Exception as e:
    fail(f"Format JSON invalide dans {INPUT_FILE}")
    print(f"  Erreur : {e}")
    sys.exit(1)

taille_mb = os.path.getsize(INPUT_FILE) / (1024 * 1024)
ok(f"Fichier chargé : {len(docs):,} documents ({taille_mb:.1f} MB)")

# Ajouter les données raw du scraper (optionnel) pour avoir le +103 dans Grafana.
scraper_docs, scraper_raw_path = load_latest_scraper_documents(RAW_DIR)
if scraper_docs:
    docs.extend(scraper_docs)
    info(
        f"Complément scraper ajouté : {len(scraper_docs):,} documents "
        f"(source: {scraper_raw_path})"
    )
else:
    warn("Aucun fichier raw/anime_*.json trouvé, indexation sans complément scraper.")

# Aperçu du premier document
if docs:
    first = docs[0]
    info(f"Premier document : {list(first.keys())[:8]}...")
    name_key = "name" if "name" in first else list(first.keys())[0]
    info(f"Exemple : {first.get(name_key, '?')} (score: {first.get('score', '?')})")


# ============================================
# 3. CRÉER / RECRÉER L'INDEX
# ============================================
step("Étape 3 : Création de l'index")

# Index incrémental : ne pas supprimer l'index s'il existe
# - Si l'index existe : on le réutilise (bulk avec même _id => upsert/maj)
# - Si l'index n'existe pas : on le crée avec le mapping
if es.indices.exists(index=INDEX_NAME):
    warn(f"L'index '{INDEX_NAME}' existe déjà — mise à jour incrémentale (sans suppression).")
else:
    print(f"  Création de l'index '{INDEX_NAME}' avec mapping...")
    try:
        es.indices.create(index=INDEX_NAME, body=ANIME_MAPPING)
        ok(f"Index '{INDEX_NAME}' créé avec succès")

        # Afficher le mapping (utile au démarrage)
        mapping_fields = list(ANIME_MAPPING["mappings"]["properties"].keys())
        info(f"Mapping : {len(mapping_fields)} champs définis")
        print(f"  Types principaux :")
        type_counts = {}
        for field, config in ANIME_MAPPING["mappings"]["properties"].items():
            t = config.get("type", "?")
            type_counts[t] = type_counts.get(t, 0) + 1
        for t, count in sorted(type_counts.items(), key=lambda x: -x[1]):
            print(f"    • {t:12s} : {count} champs")
    except Exception as e:
        fail(f"Erreur lors de la création de l'index : {e}")
        sys.exit(1)


# ============================================
# 4. INDEXATION BULK
# ============================================
step(f"Étape 4 : Indexation bulk ({len(docs):,} documents)")

print(f"  Taille des chunks : {BULK_CHUNK_SIZE} documents")
print(f"  Début de l'indexation...\n")

# Préparer les actions bulk
def generate_actions(documents):
    for doc in documents:
        # Déterminer l'ID du document
        doc_id = doc.get("mal_id", doc.get("anime_id", None))

        # Nettoyer les valeurs NaN/None pour ES
        clean_doc = {}
        for key, value in doc.items():
            if value is None:
                continue
            if isinstance(value, float) and (value != value):  # NaN check
                continue
            clean_doc[key] = value

        # Derive year/decade timeline fields when they are absent.
        # This enables decade-based trend charts in Grafana.
        if "decade" not in clean_doc or "decade_start" not in clean_doc:
            year = None
            premiered_val = str(clean_doc.get("premiered", "") or "")
            aired_val = str(clean_doc.get("aired", "") or "")
            year_match = re.search(r"(19|20)\d{2}", premiered_val) or re.search(r"(19|20)\d{2}", aired_val)
            if year_match:
                year = int(year_match.group(0))

            if year is not None:
                decade = (year // 10) * 10
                clean_doc.setdefault("year", year)
                clean_doc.setdefault("decade", decade)
                clean_doc.setdefault("decade_start", f"{decade}-01-01T00:00:00Z")

        # Grafana panels are configured with @timestamp as time field.
        # Ensure all indexed documents have a valid timestamp.
        if "@timestamp" not in clean_doc:
            clean_doc["@timestamp"] = datetime.now(timezone.utc).isoformat()

        action = {
            "_index": INDEX_NAME,
            "_source": clean_doc,
        }
        if doc_id is not None:
            action["_id"] = str(doc_id)

        yield action

# Indexation avec suivi de progression
start_time = time.time()
success_count = 0
error_count = 0
errors = []

try:
    for ok_flag, result in helpers.streaming_bulk(
        es,
        generate_actions(docs),
        chunk_size=BULK_CHUNK_SIZE,
        raise_on_error=False,
        raise_on_exception=False,
    ):
        if ok_flag:
            success_count += 1
        else:
            error_count += 1
            if len(errors) < 5:  # Garder les 5 premières erreurs
                errors.append(result)

        # Progression
        total = success_count + error_count
        if total % 2000 == 0 or total == len(docs):
            elapsed = time.time() - start_time
            rate = total / elapsed if elapsed > 0 else 0
            pct = total / len(docs) * 100
            bar = "█" * int(pct / 2.5) + "░" * (40 - int(pct / 2.5))
            print(f"\r  [{bar}] {pct:5.1f}% — {total:,}/{len(docs):,} — {rate:.0f} docs/s", end="", flush=True)

except Exception as e:
    fail(f"Erreur pendant l'indexation : {e}")

print()  # Nouvelle ligne après la barre de progression

elapsed = time.time() - start_time
ok(f"Indexation terminée en {elapsed:.1f} secondes")
ok(f"  Succès : {success_count:,}")
if error_count > 0:
    warn(f"  Erreurs : {error_count:,}")
    for err in errors[:3]:
        print(f"    → {err}")

# Rafraîchir l'index pour que les documents soient disponibles immédiatement
es.indices.refresh(index=INDEX_NAME)


# ============================================
# 5. VÉRIFICATION
# ============================================
step("Étape 5 : Vérification de l'indexation")

# Comptage
index_count = es.count(index=INDEX_NAME)["count"]
ok(f"Documents dans l'index : {index_count:,}")

if index_count != success_count:
    warn(f"Attention : {success_count:,} indexés mais {index_count:,} dans l'index")

# Taille de l'index
stats = es.indices.stats(index=INDEX_NAME)
size_bytes = stats["indices"][INDEX_NAME]["total"]["store"]["size_in_bytes"]
size_mb = size_bytes / (1024 * 1024)
info(f"Taille de l'index : {size_mb:.1f} MB")

# Test de recherche
print(f"\n  Tests de recherche rapides :")
print(f"  {'─'*50}")

# Test 1 : match all
result = es.search(index=INDEX_NAME, body={"query": {"match_all": {}}, "size": 1})
total_hits = result["hits"]["total"]["value"]
ok(f"match_all : {total_hits:,} résultats")

# Test 2 : recherche par nom
test_queries = [
    ("Naruto", {"query": {"match": {"name": "naruto"}}}),
    ("Score > 9", {"query": {"range": {"score": {"gte": 9}}}}),
    ("Genre: Action", {"query": {"term": {"main_genre": "Action"}}}),
    ("Studio: Bones", {"query": {"match": {"main_studio": "Bones"}}}),
]

for name, query in test_queries:
    try:
        result = es.search(index=INDEX_NAME, body={**query, "size": 3})
        hits = result["hits"]["total"]["value"]
        top_name = result["hits"]["hits"][0]["_source"].get("name", "?") if result["hits"]["hits"] else "aucun"
        ok(f"{name:20s} → {hits:,} résultats (top: {top_name})")
    except Exception as e:
        warn(f"{name:20s} → Erreur : {e}")

# Test 3 : agrégation top genres
print(f"\n  Agrégation : Top 5 genres")
print(f"  {'─'*50}")
agg_result = es.search(index=INDEX_NAME, body={
    "size": 0,
    "aggs": {
        "top_genres": {
            "terms": {"field": "main_genre", "size": 5}
        }
    }
})
for bucket in agg_result["aggregations"]["top_genres"]["buckets"]:
    genre = bucket["key"]
    count = bucket["doc_count"]
    bar = "█" * int(count / 500)
    print(f"    {genre:20s} : {count:>5,} {bar}")

# Test 4 : agrégation score moyen par type
print(f"\n  Agrégation : Score moyen par type")
print(f"  {'─'*50}")
agg_result = es.search(index=INDEX_NAME, body={
    "size": 0,
    "aggs": {
        "by_type": {
            "terms": {"field": "type", "size": 10},
            "aggs": {
                "avg_score": {"avg": {"field": "score"}}
            }
        }
    }
})
for bucket in agg_result["aggregations"]["by_type"]["buckets"]:
    anime_type = bucket["key"]
    avg = bucket["avg_score"]["value"]
    count = bucket["doc_count"]
    if avg:
        print(f"    {anime_type:12s} : score moyen {avg:.2f} ({count:,} animes)")


# ============================================
# 6. RÉSUMÉ FINAL
# ============================================
titre("INDEXATION TERMINÉE")

print(f"""
{C.BOLD}{C.CYAN}  Index           : {INDEX_NAME}
  Documents       : {index_count:,}
  Taille          : {size_mb:.1f} MB
  Temps           : {elapsed:.1f}s
  Débit           : {index_count/elapsed:.0f} docs/s{C.END}

{C.BOLD}  Accès :{C.END}
  {C.B}📊 Grafana        → http://localhost:3000  (admin / anidata){C.END}
  {C.B}🔍 Elasticsearch  → http://localhost:9200/anime/_search{C.END}

{C.BOLD}  Requêtes utiles :{C.END}
  {C.CYAN}curl http://localhost:9200/anime/_count{C.END}
  {C.CYAN}curl "http://localhost:9200/anime/_search?q=name:naruto&pretty"{C.END}
  {C.CYAN}curl "http://localhost:9200/anime/_search?q=main_genre:Action&size=5&pretty"{C.END}

{C.BOLD}{C.G}✅ Les données sont prêtes dans Elasticsearch !
   Ouvrez Grafana pour créer vos dashboards.{C.END}
""")