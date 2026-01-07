# dags/import_events_from_google_excels_social_enricher.py

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ✅ On importe ton pipeline existant
# Assure-toi que social_enricher.py est importable depuis Airflow
from social_enricher import run_pipeline  # type: ignore


PARIS_TZ = ZoneInfo("Europe/Paris")
DB_CONN_ID = "my_postgres"

DAG_DOC = r"""
# 🎬 Import Events depuis Google Excels + enrichissement TikTok/YouTube

**But**
- Lire tous les `google*.xlsx` dans les sous-dossiers d'un dossier `root`.
- Enrichir chaque ligne avec :
  - `youtube_short`
  - `tiktok_video`
- Insérer / mettre à jour `profil_event` (Event Django) en évitant les doublons.

**Déduplication DB**
- On considère qu’un event existe déjà si `titre` + `adresse` matchent.
- Si existant : on met à jour `bioEvent` / `website` / coords si manquantes.

**Remarques**
- Les URLs YouTube/TikTok sont stockées dans `bioEvent` (append).
- Pour TikTok, si tu utilises un `tt_state.json`, assure-toi qu’il est présent sur le worker Airflow.
"""


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
POSTAL_RE = re.compile(r"\b(\d{5})\b")

def safe_str(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()

def extract_postal_code(address: str) -> str | None:
    if not address:
        return None
    m = POSTAL_RE.search(address)
    return m.group(1) if m else None

def guess_city(address: str) -> str | None:
    # Heuristique rapide : si ça contient "paris"
    if not address:
        return None
    a = address.lower()
    if "paris" in a:
        return "Paris"
    return None

def build_bio_event(base_bio: str, youtube: str | None, tiktok: str | None) -> str:
    base_bio = safe_str(base_bio)
    lines = [base_bio] if base_bio else []

    # append social links (sans doublonner)
    if youtube:
        yt_line = f"YouTube Shorts: {youtube}"
        if yt_line not in base_bio:
            lines.append(yt_line)
    if tiktok:
        tt_line = f"TikTok: {tiktok}"
        if tt_line not in base_bio:
            lines.append(tt_line)

    return "\n".join([l for l in lines if l]).strip()


# -----------------------------------------------------------------------------
# Task 1: Enrich + write CSV
# -----------------------------------------------------------------------------
def enrich_and_export_csv(**context):
    """
    Lance ton pipeline (playwright) puis exporte un CSV sur disque.
    """
    params = context["params"]

    root_folder = params["root_folder"]
    name_col = params["name_col"]
    address_col = params["address_col"]
    headless = params.get("headless", False)
    tt_state_path = params.get("tt_state_path", "tt_state.json")
    do_geocode = params.get("do_geocode", False)
    geocode_cache = params.get("geocode_cache", "geocode_cache.csv")

    output_dir = params.get("output_dir", "/opt/airflow/data")
    os.makedirs(output_dir, exist_ok=True)
    out_csv = os.path.join(output_dir, "events_enriched.csv")

    logging.info("🚀 [enrich] Starting pipeline...")
    df = asyncio.run(
        run_pipeline(
            root_folder=root_folder,
            name_col=name_col,
            address_col=address_col,
            do_geocode=do_geocode,
            geocode_cache=geocode_cache,
            tt_state_path=tt_state_path,
            headless=headless,
        )
    )

    # Normalisation minimale des colonnes attendues
    for col in ["youtube_short", "tiktok_video", "category", "source_folder", "source_file"]:
        if col not in df.columns:
            df[col] = None

    df.to_csv(out_csv, index=False)
    logging.info(f"✅ [enrich] Exported: {out_csv} | rows={len(df)}")

    context["ti"].xcom_push(key="enriched_csv_path", value=out_csv)


# -----------------------------------------------------------------------------
# Task 2: Insert/Update profil_event
# -----------------------------------------------------------------------------
def upsert_events_from_csv(**context):
    """
    Upsert simple dans profil_event:
      - clé logique: titre + adresse
      - insert si absent
      - update si présent (bioEvent/website/coords)
    """
    params = context["params"]
    name_col = params["name_col"]
    address_col = params["address_col"]
    website_col = params.get("website_col", "MRe4xd href")  # dans tes excels
    region_default = params.get("region_default", "Paris")
    creator_winker_id = 116
    active_default = int(params.get("active_default", 0))
    max_participants = int(params.get("max_participants", 99999999))
    access_comment = bool(params.get("access_comment", True))
    validated_from_web = False

    ti = context["ti"]
    csv_path = ti.xcom_pull(task_ids="enrich_and_export_csv", key="enriched_csv_path")
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"Enriched CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    logging.info(f"📥 [db] Loading CSV rows={len(df)} from {csv_path}")

    conn = BaseHook.get_connection(DB_CONN_ID)
    connection = psycopg2.connect(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password,
        dbname=conn.schema,
    )
    cursor = connection.cursor()

    inserted = 0
    updated = 0
    skipped = 0

    try:
        for _, row in df.iterrows():
            titre = safe_str(row.get(name_col))
            adresse = safe_str(row.get(address_col))
            if not titre or not adresse:
                skipped += 1
                continue

            region = safe_str(row.get("category")) or region_default
            city = guess_city(adresse) or "Paris"
            code_postal = extract_postal_code(adresse)

            website = safe_str(row.get(website_col)) or None
            youtube = safe_str(row.get("youtube_short")) or None
            tiktok = safe_str(row.get("tiktok_video")) or None

            lat = row.get("latitude", None)
            lon = row.get("longitude", None)

            # Bio: on met au minimum les liens sociaux
            bio_event = build_bio_event("", youtube=youtube, tiktok=tiktok)

            # 1) Check existence (titre+adresse)
            cursor.execute(
                """
                SELECT id, "bioEvent", website, lat, lon
                FROM profil_event
                WHERE titre = %s AND adresse = %s
                LIMIT 1
                """,
                (titre, adresse),
            )
            existing = cursor.fetchone()

            if existing:
                event_id, old_bio, old_website, old_lat, old_lon = existing

                new_bio = build_bio_event(old_bio or "", youtube=youtube, tiktok=tiktok)

                # On met à jour seulement ce qui apporte de la valeur
                final_website = old_website or website
                final_lat = old_lat if old_lat is not None else lat
                final_lon = old_lon if old_lon is not None else lon

                cursor.execute(
                    """
                    UPDATE profil_event
                    SET
                        "bioEvent" = %s,
                        website = %s,
                        lat = %s,
                        lon = %s
                    WHERE id = %s
                    """,
                    (new_bio, final_website, final_lat, final_lon, event_id),
                )
                updated += 1
                continue

            # 2) Insert new event (reprend les colonnes vues dans tes DAGs)
            cursor.execute(
                """
                INSERT INTO profil_event (
                    titre, adresse, region, city, "codePostal",
                    "bioEvent", website, "creatorWinker_id",
                    active, lat, lon, "maxNumberParticipant", "accessComment", validated_from_web
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    titre,
                    adresse,
                    region,
                    city,
                    code_postal,
                    bio_event,
                    website,
                    creator_winker_id,
                    active_default,
                    lat,
                    lon,
                    max_participants,
                    access_comment,
                    validated_from_web,
                ),
            )
            _new_id = cursor.fetchone()[0]
            inserted += 1

        connection.commit()
        logging.info(f"✅ [db] Inserted={inserted} Updated={updated} Skipped={skipped}")

    except Exception as e:
        connection.rollback()
        logging.error(f"❌ [db] Failure: {e}")
        raise
    finally:
        cursor.close()
        connection.close()


# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="import_events_from_google_excels_social_enricher",
    start_date=days_ago(1),
    schedule_interval="0 3 * * *",  # tous les jours à 03:00
    catchup=False,
    default_args=default_args,
    tags=["event", "import", "tiktok", "youtube", "playwright"],
) as dag:
    dag.doc_md = DAG_DOC
    dag.timezone = PARIS_TZ

    t1 = PythonOperator(
        task_id="enrich_and_export_csv",
        python_callable=enrich_and_export_csv,
        provide_context=True,
        params={
            # 🔧 À ADAPTER
            "root_folder": "/opt/airflow/data/maps",   # dossier contenant les sous-dossiers avec google*.xlsx
            "name_col": "OSrXXb",
            "address_col": "rllt__details 3",
            "website_col": "MRe4xd href",

            # Playwright
            "headless": False,                         # pour debug, passe à True si ok
            "tt_state_path": "/opt/airflow/data/tt_state.json",

            # Geocode optionnel (si geopy + cache)
            "do_geocode": False,
            "geocode_cache": "/opt/airflow/data/geocode_cache.csv",

            # Output
            "output_dir": "/opt/airflow/data",
        },
    )

    t2 = PythonOperator(
        task_id="upsert_events_from_csv",
        python_callable=upsert_events_from_csv,
        provide_context=True,
        params={
            "name_col": "OSrXXb",
            "address_col": "rllt__details 3",
            "website_col": "MRe4xd href",

            # Defaults DB
            "region_default": "Paris",
            "creator_winker_id": 116,
            "active_default": 0,
            "max_participants": 99999999,
            "access_comment": True,
        },
    )

    t1 >> t2
