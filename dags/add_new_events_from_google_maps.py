# dags/import_events_from_google_excels_social_enricher.py

from __future__ import annotations

import asyncio
import inspect
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

import social_enricher  # type: ignore


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

**Mode debug**
- Param `debug_limit_rows`: si défini (ex: 5), on limite le DF à N lignes **avant** YouTube/TikTok.
  -> Pour revenir au normal, mets `debug_limit_rows: None` (ou commente la ligne).
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
    if not address:
        return None
    a = address.lower()
    if "paris" in a:
        return "Paris"
    return None


def build_bio_event(base_bio: str, youtube: str | None, tiktok: str | None) -> str:
    base_bio = safe_str(base_bio)
    lines = [base_bio] if base_bio else []

    if youtube:
        yt_line = f"YouTube Shorts: {youtube}"
        if yt_line not in base_bio:
            lines.append(yt_line)
    if tiktok:
        tt_line = f"TikTok: {tiktok}"
        if tt_line not in base_bio:
            lines.append(tt_line)

    return "\n".join([l for l in lines if l]).strip()


def _call_with_supported_kwargs(func, **kwargs):
    """
    Appelle une fonction en filtrant les kwargs qui ne sont pas dans la signature.
    Ça rend le DAG plus robuste si social_enricher évolue.
    """
    sig = inspect.signature(func)
    accepted = {k: v for k, v in kwargs.items() if k in sig.parameters and v is not None}
    return func(**accepted)


# -----------------------------------------------------------------------------
# Task 1: Enrich + write CSV (avec limite AVANT Playwright)
# -----------------------------------------------------------------------------
async def _run_pipeline_limited(
    root_folder: str,
    name_col: str,
    address_col: str,
    do_geocode: bool,
    geocode_cache: str | None,
    tt_state_path: str | None,
    headless: bool,
    debug_limit_rows: int | None,
    skip_tiktok_if_no_state: bool,
):
    """
    Pipeline reconstruit à partir des fonctions de social_enricher, pour pouvoir faire:
    load -> dedup -> head(N) -> enrich youtube/tiktok
    """

    # 1) Load excels
    load_all = getattr(social_enricher, "load_all_google_excels", None)
    if load_all is None:
        raise ImportError("social_enricher.load_all_google_excels introuvable")

    df = _call_with_supported_kwargs(load_all, root_folder=root_folder)

    # 2) Debug: limiter AVANT enrichment social
    if debug_limit_rows is not None:
        df = df.head(int(debug_limit_rows)).copy()
        logging.warning(f"🧪 [debug] Limitation DataFrame à {len(df)} lignes (debug_limit_rows={debug_limit_rows})")

    # 3) YouTube
    add_yt = getattr(social_enricher, "add_youtube_shorts_to_df", None)
    if add_yt is None:
        logging.warning("⚠️ add_youtube_shorts_to_df introuvable, skip YouTube")
    else:
        df = await _call_with_supported_kwargs(
            add_yt,
            df=df,
            name_col=name_col,
            address_col=address_col,
            headless=headless,
        )

    # 4) TikTok
    add_tt = getattr(social_enricher, "add_tiktok_videos_to_df", None)
    if add_tt is None:
        logging.warning("⚠️ add_tiktok_videos_to_df introuvable, skip TikTok")
        return df

    # Si le fichier n'existe pas, ça plante (tu l'as vu). En debug on peut skip.
    if tt_state_path and not os.path.exists(tt_state_path):
        msg = f"⚠️ tt_state.json introuvable: {tt_state_path}"
        if skip_tiktok_if_no_state:
            logging.warning(msg + " -> skip TikTok (skip_tiktok_if_no_state=True)")
            return df
        logging.warning(msg + " -> on tente TikTok sans storage state (tt_state_path=None)")
        tt_state_path = None

    df = await _call_with_supported_kwargs(
        add_tt,
        df=df,
        name_col=name_col,
        address_col=address_col,
        tt_state_path=tt_state_path,
        headless=headless,
    )

    return df


def enrich_and_export_csv(**context):
    """
    Lance le pipeline (playwright) puis exporte un CSV sur disque.
    """
    params = context["params"]

    root_folder = params["root_folder"]
    name_col = params["name_col"]
    address_col = params["address_col"]

    headless = bool(params.get("headless", True))
    tt_state_path = params.get("tt_state_path")
    do_geocode = bool(params.get("do_geocode", False))
    geocode_cache = params.get("geocode_cache")

    # 🧪 DEBUG: limite AVANT YouTube/TikTok
    debug_limit_rows = params.get("debug_limit_rows", 5)  # <-- mets None pour revenir au normal
    # debug_limit_rows = None  # <-- (alternative) décommente ça pour revenir au normal

    # si tt_state.json absent: on peut skip TikTok en debug
    skip_tiktok_if_no_state = bool(params.get("skip_tiktok_if_no_state", True))

    output_dir = params.get("output_dir", "/opt/airflow/data")
    os.makedirs(output_dir, exist_ok=True)
    out_csv = os.path.join(output_dir, "events_enriched.csv")

    logging.info("🚀 [enrich] Starting pipeline...")

    df = asyncio.run(
        _run_pipeline_limited(
            root_folder=root_folder,
            name_col=name_col,
            address_col=address_col,
            do_geocode=do_geocode,
            geocode_cache=geocode_cache,
            tt_state_path=tt_state_path,
            headless=headless,
            debug_limit_rows=debug_limit_rows,
            skip_tiktok_if_no_state=skip_tiktok_if_no_state,
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
    website_col = params.get("website_col", "MRe4xd href")
    region_default = params.get("region_default", "Paris")
    creator_winker_id = int(params.get("creator_winker_id", 116))
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

            bio_event = build_bio_event("", youtube=youtube, tiktok=tiktok)

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
            cursor.fetchone()
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

            # 🧪 DEBUG (facile à enlever)
            "debug_limit_rows": 5,           # <-- mets None ou commente cette ligne pour full dataframe
            "skip_tiktok_if_no_state": True, # <-- évite le crash si tt_state.json absent

            # Geocode optionnel
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

            "region_default": "Paris",
            "creator_winker_id": 116,
            "active_default": 0,
            "max_participants": 99999999,
            "access_comment": True,
        },
    )

    t1 >> t2
