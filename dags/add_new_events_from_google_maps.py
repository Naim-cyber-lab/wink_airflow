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

from social_enricher import run_pipeline

PARIS_TZ = ZoneInfo("Europe/Paris")
DB_CONN_ID = "my_postgres"

DAG_DOC = r"""
# 🎬 Import Events depuis Google Excels + enrichissement TikTok/YouTube/Instagram

- Charge tous les `google*.xlsx` dans les sous-dossiers de `root_folder`
- Enrichit avec:
  - youtube_query, youtube_short
  - tiktok_query, tiktok_video
  - instagram_query, instagram_reel
  - latitude/longitude (via geocode)
- Upsert dans `profil_event`

## Debug
- `debug_limit_rows: 5` => limite le dataframe à 5 lignes AVANT Playwright + geocode
"""


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def get_conn():
    conn = BaseHook.get_connection(DB_CONN_ID)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )


def _slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^a-z0-9\-]", "", s)
    return s[:120] if s else ""


def _safe_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _normalize_region_from_address(address: str) -> str | None:
    a = (address or "").lower()
    if "lille" in a:
        return "Lille"
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


def _upsert_events(df: pd.DataFrame, table: str = "profil_event"):
    """
    Upsert on (title, address) if those columns exist; fallback to (title) otherwise.
    This script tries to be resilient to schema changes by only inserting columns
    that exist in the DB table.
    """
    if df.empty:
        logging.warning("⚠️ [db] DataFrame empty => nothing to upsert.")
        return

    conn = get_conn()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            table_cols = _get_table_columns(cur, table)

            # keep only columns existing in DB
            keep_cols = [c for c in df.columns if c in table_cols]
            if not keep_cols:
                raise RuntimeError(f"No matching columns between DF and DB table {table}")

            df2 = df[keep_cols].copy()

            # Determine conflict keys
            conflict_cols = []
            if "title" in table_cols:
                conflict_cols.append("title")
            if "address" in table_cols and "address" in df2.columns:
                conflict_cols.append("address")

            if not conflict_cols:
                raise RuntimeError("Cannot determine upsert conflict keys (need at least 'title').")

            # Build INSERT ... ON CONFLICT
            cols_sql = sql.SQL(", ").join(map(sql.Identifier, keep_cols))
            placeholders = sql.SQL(", ").join(sql.Placeholder() * len(keep_cols))

            update_cols = [c for c in keep_cols if c not in conflict_cols]
            update_sql = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                for c in update_cols
            )

            query = sql.SQL(
                """
                INSERT INTO {table} ({cols})
                VALUES ({vals})
                ON CONFLICT ({conflict})
                DO UPDATE SET {updates}
                """
            ).format(
                table=sql.Identifier(table),
                cols=cols_sql,
                vals=placeholders,
                conflict=sql.SQL(", ").join(map(sql.Identifier, conflict_cols)),
                updates=update_sql if update_cols else sql.SQL("title = EXCLUDED.title"),
            )

            rows = df2.to_dict(orient="records")
            logging.info("💾 [db] Upserting %s rows into %s", len(rows), table)

            for r in rows:
                values = [r.get(c) for c in keep_cols]
                cur.execute(query, values)

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


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

    # Social flags (from Trigger DAG params)
    do_youtube = bool(params.get("do_youtube", True))
    do_tiktok = bool(params.get("do_tiktok", True))
    do_instagram = bool(params.get("do_instagram", False))

    # geocode
    do_geocode = bool(params.get("do_geocode", True))
    geocode_cache = params.get("geocode_cache", "/opt/airflow/data/geocode_cache.csv")

    # TikTok state
    tt_state_path = params.get("tt_state_path", "/opt/airflow/data/tt_state.json")

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
            do_youtube=do_youtube,
            do_tiktok=do_tiktok,
            do_instagram=do_instagram,
        )
    )

    # colonnes minimales attendues
    for col in [
        "category",
        "title",
        "address",
        "region",
        "latitude",
        "longitude",
        "youtube_query",
        "youtube_short",
        "tiktok_query",
        "tiktok_video",
        "instagram_query",
        "instagram_reel",
    ]:
        if col not in df.columns:
            df[col] = None

    # region: fallback
    use_category_as_region = bool(params.get("use_category_as_region", True))
    region_default = params.get("region_default", "Paris")

    if "region" in df.columns:
        for i, row in df.iterrows():
            if row.get("region"):
                continue

            addr = str(row.get("address", "") or "")
            reg = _normalize_region_from_address(addr)

            if reg:
                df.at[i, "region"] = reg
            elif use_category_as_region and row.get("category"):
                df.at[i, "region"] = row.get("category")
            else:
                df.at[i, "region"] = region_default

    # champs event (defaults)
    creator_winker_id = int(params.get("creator_winker_id", 116))
    active_default = int(params.get("active_default", 0))
    max_participants = int(params.get("max_participants", 99999999))
    access_comment = _safe_bool(params.get("access_comment", True))
    validated_from_web = _safe_bool(params.get("validated_from_web", False))

    # mapping vers DB si les colonnes existent
    if "creator_winker_id" not in df.columns:
        df["creator_winker_id"] = creator_winker_id
    if "active" not in df.columns:
        df["active"] = active_default
    if "maxParticipants" not in df.columns:
        df["maxParticipants"] = max_participants
    if "accessComment" not in df.columns:
        df["accessComment"] = access_comment
    if "validatedFromWeb" not in df.columns:
        df["validatedFromWeb"] = validated_from_web

    # title/address normalization
    if "title" not in df.columns:
        df["title"] = df[name_col].astype(str).fillna("")
    if "address" not in df.columns:
        df["address"] = df[address_col].astype(str).fillna("")

    # slug (if supported by db)
    if "slug" not in df.columns:
        df["slug"] = df["title"].apply(_slugify)

    # Put social links into bioEvent if exists
    if "bioEvent" not in df.columns:
        df["bioEvent"] = ""

    for i, row in df.iterrows():
        parts = []
        if row.get("youtube_short"):
            parts.append(f"YouTube: {row.get('youtube_short')}")
        if row.get("tiktok_video"):
            parts.append(f"TikTok: {row.get('tiktok_video')}")
        if row.get("instagram_reel"):
            parts.append(f"Instagram: {row.get('instagram_reel')}")
        if parts:
            cur = str(row.get("bioEvent") or "").strip()
            if cur:
                cur = cur + "\n" + "\n".join(parts)
            else:
                cur = "\n".join(parts)
            df.at[i, "bioEvent"] = cur

    df.to_csv(out_csv, index=False)
    logging.info("✅ [enrich] Saved enriched CSV to %s", out_csv)

    # push XCom
    context["ti"].xcom_push(key="out_csv", value=out_csv)


# ---------------------------------------------------------------------
# Task 2: load into DB
# ---------------------------------------------------------------------
def load_csv_to_db(**context):
    params = context["params"]
    table = params.get("table", "profil_event")

    out_csv = context["ti"].xcom_pull(key="out_csv", task_ids="enrich_and_export_csv")
    if not out_csv or not os.path.exists(out_csv):
        raise FileNotFoundError(f"CSV not found: {out_csv}")

    df = pd.read_csv(out_csv)
    logging.info("📥 [db] Loaded CSV: %s rows", len(df))
    _upsert_events(df, table=table)
    logging.info("✅ [db] Upsert done.")


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
    tags=["event", "import", "tiktok", "youtube", "instagram", "playwright"],
) as dag:
    dag.doc_md = DAG_DOC
    dag.timezone = PARIS_TZ

    t1 = PythonOperator(
        task_id="enrich_and_export_csv",
        python_callable=enrich_and_export_csv,
        provide_context=True,
        params={
            # Root
            "root_folder": "/opt/airflow/data/google_excels",
            "output_dir": "/opt/airflow/data",

            # Columns
            "name_col": "name",
            "address_col": "address",

            # Playwright
            "headless": True,
            "tt_state_path": "/opt/airflow/data/tt_state.json",

            # Social platforms (modifiable at Trigger DAG)
            # - do_youtube  : enrich YouTube Shorts
            # - do_tiktok   : enrich TikTok video
            # - do_instagram: enrich Instagram Reel (best effort, no login)
            "do_youtube": True,
            "do_tiktok": True,
            "do_instagram": False,

            # Geocode
            "do_geocode": True,
            "geocode_cache": "/opt/airflow/data/geocode_cache.csv",

            # Debug
            "debug_limit_rows": 5,

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

    t2 = PythonOperator(
        task_id="load_csv_to_db",
        python_callable=load_csv_to_db,
        provide_context=True,
        params={
            "table": "profil_event",
        },
    )

    t1 >> t2
