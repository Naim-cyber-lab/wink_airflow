# dags/add_new_events_from_google_maps.py

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
from psycopg2 import sql
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from social_enricher import run_pipeline  # type: ignore


PARIS_TZ = ZoneInfo("Europe/Paris")
DB_CONN_ID = "my_postgres"

DAG_DOC = r"""
# 🎬 Import Events depuis Google Excels + enrichissement TikTok/YouTube

- Charge tous les `google*.xlsx` dans les sous-dossiers de `root_folder`
- Enrichit avec:
  - youtube_query, youtube_short
  - tiktok_query, tiktok_video
  - latitude/longitude (via geocode)
- Upsert dans `profil_event`

## Debug rapide
- `debug_limit_rows: 5` => limite le dataframe à 5 lignes AVANT Playwright + geocode.
- Pour revenir au normal: mets `debug_limit_rows: None`.
"""


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
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
    if not address:
        return None
    a = address.lower()
    if "paris" in a:
        return "Paris"
    return None


def _get_table_columns(cursor, table: str) -> set[str]:
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table,),
    )
    return {r[0] for r in cursor.fetchall()}


def _build_bio(
    old_bio: str,
    *,
    category: str | None,
    price: str | None,
    website: str | None,
    youtube_query: str | None,
    youtube_short: str | None,
    tiktok_query: str | None,
    tiktok_video: str | None,
) -> str:
    """
    On met TOUT ce qui est enrichissement dans bioEvent,
    même si on a aussi des colonnes dédiées.
    """
    base = safe_str(old_bio)
    lines = [base] if base else []

    def add_line(label: str, value: str | None):
        v = safe_str(value)
        if not v:
            return
        line = f"{label}: {v}"
        if line not in base:
            lines.append(line)

    add_line("Category", category)
    add_line("Price", price)
    add_line("Website", website)
    add_line("YouTube query", youtube_query)
    add_line("YouTube Shorts", youtube_short)
    add_line("TikTok query", tiktok_query)
    add_line("TikTok", tiktok_video)

    return "\n".join([l for l in lines if l]).strip()


def _exec_insert(cursor, table: str, data: dict[str, object]):
    cols = [sql.Identifier(c) for c in data.keys()]
    placeholders = [sql.Placeholder() for _ in data.keys()]
    query = sql.SQL("INSERT INTO {t} ({cols}) VALUES ({vals}) RETURNING id").format(
        t=sql.Identifier(table),
        cols=sql.SQL(", ").join(cols),
        vals=sql.SQL(", ").join(placeholders),
    )
    cursor.execute(query, list(data.values()))
    return cursor.fetchone()[0]


def _exec_update(cursor, table: str, event_id: int, data: dict[str, object]):
    """
    UPDATE table SET col=%s,... WHERE id=%s
    """
    assignments = [
        sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
        for k in data.keys()
    ]
    query = sql.SQL("UPDATE {t} SET {assignments} WHERE id = {id_col}").format(
        t=sql.Identifier(table),
        assignments=sql.SQL(", ").join(assignments),
        id_col=sql.SQL("id = %s"),
    )
    values = list(data.values()) + [event_id]
    cursor.execute(query, values)


# ---------------------------------------------------------------------
# Task 1: enrich + export csv
# ---------------------------------------------------------------------
def enrich_and_export_csv(**context):
    params = context["params"]

    root_folder = params["root_folder"]
    name_col = params["name_col"]
    address_col = params["address_col"]

    # Playwright
    headless = bool(params.get("headless", True))
    tt_state_path = params.get("tt_state_path")

    # Geocode
    do_geocode = bool(params.get("do_geocode", True))
    geocode_cache = params.get("geocode_cache", "/opt/airflow/data/geocode_cache.csv")

    # 🧪 Debug
    debug_limit_rows = params.get("debug_limit_rows", 5)  # mets None pour full

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
            debug_limit_rows=debug_limit_rows,
        )
    )

    # colonnes minimales attendues
    for col in [
        "category",
        "source_folder",
        "source_file",
        "latitude",
        "longitude",
        "youtube_query",
        "youtube_short",
        "tiktok_query",
        "tiktok_video",
    ]:
        if col not in df.columns:
            df[col] = None

    df.to_csv(out_csv, index=False)
    logging.info("✅ [enrich] Exported: %s | rows=%s", out_csv, len(df))

    context["ti"].xcom_push(key="enriched_csv_path", value=out_csv)


# ---------------------------------------------------------------------
# Task 2: upsert
# ---------------------------------------------------------------------
def upsert_events_from_csv(**context):
    params = context["params"]

    name_col = params["name_col"]
    address_col = params["address_col"]
    website_col = params.get("website_col", "MRe4xd href")

    # ✅ IMPORTANT: region = category (dossier) par défaut
    region_default = params.get("region_default", "Paris")
    use_category_as_region = bool(params.get("use_category_as_region", True))

    creator_winker_id = int(params.get("creator_winker_id", 116))
    active_default = int(params.get("active_default", 0))
    max_participants = int(params.get("max_participants", 99999999))
    access_comment = bool(params.get("access_comment", True))
    validated_from_web = bool(params.get("validated_from_web", False))

    ti = context["ti"]
    csv_path = ti.xcom_pull(task_ids="enrich_and_export_csv", key="enriched_csv_path")
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"Enriched CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    logging.info("📥 [db] Loading CSV rows=%s from %s", len(df), csv_path)

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
        cols = _get_table_columns(cursor, "profil_event")
        logging.info("🧱 [db] profil_event columns detected: %s cols", len(cols))

        for _, row in df.iterrows():
            titre = safe_str(row.get(name_col))
            adresse = safe_str(row.get(address_col))
            if not titre or not adresse:
                skipped += 1
                continue

            category = safe_str(row.get("category")) or None

            # ✅ region depuis category (bar paris / escape game paris / etc)
            region = (category if (use_category_as_region and category) else safe_str(region_default)) or "Paris"

            city = guess_city(adresse) or "Paris"
            code_postal = extract_postal_code(adresse)

            website = safe_str(row.get(website_col)) or None
            price = safe_str(row.get("price")) or None

            youtube_query = safe_str(row.get("youtube_query")) or None
            youtube_short = safe_str(row.get("youtube_short")) or None
            tiktok_query = safe_str(row.get("tiktok_query")) or None
            tiktok_video = safe_str(row.get("tiktok_video")) or None

            lat = row.get("latitude", None)
            lon = row.get("longitude", None)

            # 1) existing?
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

                new_bio = _build_bio(
                    old_bio or "",
                    category=category,
                    price=price,
                    website=website or old_website,
                    youtube_query=youtube_query,
                    youtube_short=youtube_short,
                    tiktok_query=tiktok_query,
                    tiktok_video=tiktok_video,
                )

                update_map: dict[str, object] = {}

                if "bioEvent" in cols:
                    update_map["bioEvent"] = new_bio

                # website: fill if missing
                if "website" in cols and (old_website is None and website is not None):
                    update_map["website"] = website

                # lat/lon: fill if missing
                if "lat" in cols and old_lat is None and lat is not None:
                    update_map["lat"] = float(lat)
                if "lon" in cols and old_lon is None and lon is not None:
                    update_map["lon"] = float(lon)

                # region/city/codePostal
                if "region" in cols:
                    update_map["region"] = region
                if "city" in cols:
                    update_map["city"] = city
                if "codePostal" in cols and code_postal:
                    update_map["codePostal"] = code_postal

                # enrich columns if exist
                if "youtube_query" in cols and youtube_query:
                    update_map["youtube_query"] = youtube_query
                if "youtube_short" in cols and youtube_short:
                    update_map["youtube_short"] = youtube_short
                if "tiktok_query" in cols and tiktok_query:
                    update_map["tiktok_query"] = tiktok_query
                if "tiktok_video" in cols and tiktok_video:
                    update_map["tiktok_video"] = tiktok_video
                if "price" in cols and price:
                    update_map["price"] = price
                if "category" in cols and category:
                    update_map["category"] = category

                if update_map:
                    _exec_update(cursor, "profil_event", int(event_id), update_map)

                updated += 1
                continue

            # 2) insert new
            bio_event = _build_bio(
                "",
                category=category,
                price=price,
                website=website,
                youtube_query=youtube_query,
                youtube_short=youtube_short,
                tiktok_query=tiktok_query,
                tiktok_video=tiktok_video,
            )

            insert_map: dict[str, object] = {}

            # core
            if "titre" in cols:
                insert_map["titre"] = titre
            if "titre_fr" in cols:
                insert_map["titre_fr"] = titre
            if "adresse" in cols:
                insert_map["adresse"] = adresse

            if "region" in cols:
                insert_map["region"] = region
            if "city" in cols:
                insert_map["city"] = city
            if "codePostal" in cols:
                insert_map["codePostal"] = code_postal

            if "bioEvent" in cols:
                insert_map["bioEvent"] = bio_event
            if "website" in cols:
                insert_map["website"] = website

            # required/not null
            if "nbStories" in cols:
                insert_map["nbStories"] = 0

            # creator / flags
            if "creatorWinker_id" in cols:
                insert_map["creatorWinker_id"] = creator_winker_id
            if "active" in cols:
                insert_map["active"] = active_default
            if "maxNumberParticipant" in cols:
                insert_map["maxNumberParticipant"] = max_participants
            if "accessComment" in cols:
                insert_map["accessComment"] = access_comment
            if "validated_from_web" in cols:
                insert_map["validated_from_web"] = validated_from_web

            # lat/lon
            if "lat" in cols and lat is not None:
                insert_map["lat"] = float(lat)
            if "lon" in cols and lon is not None:
                insert_map["lon"] = float(lon)

            # enrich columns if exist
            if "youtube_query" in cols:
                insert_map["youtube_query"] = youtube_query
            if "youtube_short" in cols:
                insert_map["youtube_short"] = youtube_short
            if "tiktok_query" in cols:
                insert_map["tiktok_query"] = tiktok_query
            if "tiktok_video" in cols:
                insert_map["tiktok_video"] = tiktok_video
            if "price" in cols:
                insert_map["price"] = price
            if "category" in cols:
                insert_map["category"] = category

            if not insert_map:
                raise RuntimeError("Aucune colonne détectée pour insert dans profil_event (schéma inattendu).")

            _exec_insert(cursor, "profil_event", insert_map)
            inserted += 1

        connection.commit()
        logging.info("✅ [db] Inserted=%s Updated=%s Skipped=%s", inserted, updated, skipped)

    except Exception as e:
        connection.rollback()
        logging.error("❌ [db] Failure: %s", e)
        raise
    finally:
        cursor.close()
        connection.close()


# ---------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="import_events_from_google_excels_social_enricher",
    start_date=days_ago(1),
    schedule_interval="0 3 * * *",
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
            "root_folder": "/opt/airflow/data",
            "name_col": "OSrXXb",
            "address_col": "rllt__details 3",
            "website_col": "MRe4xd href",

            # Playwright
            "headless": True,
            "tt_state_path": "/opt/airflow/data/tt_state.json",

            # ✅ Geocode ON pour corriger lat/lon
            "do_geocode": True,
            "geocode_cache": "/opt/airflow/data/geocode_cache.csv",

            # 🧪 DEBUG: mets None pour revenir au normal
            "debug_limit_rows": 5,

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

            # ✅ region = category par défaut
            "use_category_as_region": True,
            "region_default": "Paris",

            "creator_winker_id": 116,
            "active_default": 0,
            "max_participants": 99999999,
            "access_comment": True,
            "validated_from_web": False,
        },
    )

    t1 >> t2
