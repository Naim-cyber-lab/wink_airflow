from __future__ import annotations

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
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


PARIS_TZ = ZoneInfo("Europe/Paris")
DB_CONN_ID = "my_postgres"

CONFIDENCE_OK = {"high", "medium", "low"}


# ── Helpers (même style que ton DAG existant) ─────────────────────────────────

def safe_str(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def safe_float(x) -> float | None:
    s = safe_str(x)
    if not s or s.lower() in ("nan", "none", "null", ""):
        return None
    s = s.replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def safe_bool(x) -> bool:
    return safe_str(x).lower() in ("true", "1", "yes")


def _short(s: str | None, n: int = 80) -> str:
    s = safe_str(s)
    if not s:
        return ""
    s = " ".join(s.split())
    return s if len(s) <= n else s[: n - 1] + "…"


def _log_row(action: str, *, event_id: int, titre: str, details: str = "") -> None:
    base = f"[db] {action} id={event_id} | titre='{_short(titre, 60)}'"
    if details:
        base += f" | {details}"
    logging.info(base)


def _exec_update(cursor, table: str, event_id: int, data: dict[str, object]):
    assignments = [
        sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder())
        for k in data.keys()
    ]
    query = sql.SQL("UPDATE {t} SET {assignments} WHERE id = %s").format(
        t=sql.Identifier(table),
        assignments=sql.SQL(", ").join(assignments),
    )
    cursor.execute(query, list(data.values()) + [event_id])


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


# ── Task 1 : charger et valider le CSV ───────────────────────────────────────

def load_and_validate(**context):
    params   = context["params"]
    csv_path = params["csv_path"]

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV introuvable : {csv_path}")

    df = pd.read_csv(csv_path, dtype=str)
    logging.info("📥 [load] CSV chargé : %s lignes depuis %s", len(df), csv_path)
    logging.info("📥 [load] Colonnes : %s", list(df.columns))

    if "id" not in df.columns:
        raise ValueError("Colonne 'id' manquante dans le CSV")
    if "confidence" not in df.columns:
        raise ValueError("Colonne 'confidence' manquante — utilise le CSV produit par le notebook")

    # Stats par confidence
    logging.info("📊 [load] Distribution confidence :\n%s", df["confidence"].value_counts().to_string())

    # Filtrer les lignes exploitables
    df_valid = df[df["confidence"].isin(CONFIDENCE_OK)].copy()
    df_skip  = df[~df["confidence"].isin(CONFIDENCE_OK)]
    logging.info("✅ [load] %s lignes à updater | %s ignorées (not_found/error/NaN)", len(df_valid), len(df_skip))

    # Aperçu des prix trouvés
    logging.info(
        "🧪 [load] prix head :\n%s",
        df_valid[["id", "titre", "priceEvent", "prixInitial", "confidence"]].head(10).to_string(index=False),
    )

    context["ti"].xcom_push(key="rows_json", value=df_valid.to_json(orient="records"))
    context["ti"].xcom_push(key="total", value=len(df_valid))


# ── Task 2 : update DB (uniquement colonnes prix) ─────────────────────────────

def update_price_columns(**context):
    rows_json = context["ti"].xcom_pull(key="rows_json")
    rows = pd.read_json(rows_json, orient="records").to_dict(orient="records")
    logging.info("🚀 [db] Update de %s events...", len(rows))

    conn_meta = BaseHook.get_connection(DB_CONN_ID)
    connection = psycopg2.connect(
        host=conn_meta.host,
        port=conn_meta.port,
        user=conn_meta.login,
        password=conn_meta.password,
        dbname=conn_meta.schema,
    )
    cursor = connection.cursor()

    updated = skipped = noops = errors = 0

    try:
        db_cols = _get_table_columns(cursor, "profil_event")
        logging.info("🧱 [db] profil_event : %s colonnes détectées", len(db_cols))
        logging.info(
            "🧪 [db] colonnes prix présentes : priceEvent=%s prixInitial=%s prixReduction=%s containReduction=%s price=%s price_summary=%s",
            "priceEvent"       in db_cols,
            "prixInitial"      in db_cols,
            "prixReduction"    in db_cols,
            "containReduction" in db_cols,
            "price"            in db_cols,
            "price_summary"    in db_cols,
        )

        for row in rows:
            event_id = safe_str(row.get("id"))
            titre    = safe_str(row.get("titre"))

            if not event_id or event_id.lower() in ("nan", "none", "null", ""):
                skipped += 1
                logging.warning("[db] SKIP — id manquant | titre=%r", _short(titre))
                continue

            try:
                event_id_int = int(float(event_id))
            except Exception:
                skipped += 1
                logging.warning("[db] SKIP — id invalide=%r | titre=%r", event_id, _short(titre))
                continue

            # ── Valeurs à écrire ─────────────────────────────────────────
            price_event    = safe_str(row.get("priceEvent"))   or None
            prix_initial   = safe_float(row.get("prixInitial"))
            prix_reduction = safe_float(row.get("prixReduction"))
            contain_reduc  = safe_bool(row.get("containReduction"))
            price_summary  = safe_str(row.get("price_summary")) or None

            # ── Construire l'update_map (seulement colonnes qui existent en DB) ──
            # On ne touche RIEN d'autre que ces 6 colonnes
            update_map: dict[str, object] = {}

            if "priceEvent"       in db_cols and price_event    is not None:
                update_map["priceEvent"]       = price_event
            if "prixInitial"      in db_cols and prix_initial   is not None:
                update_map["prixInitial"]      = prix_initial
            if "prixReduction"    in db_cols and prix_reduction is not None:
                update_map["prixReduction"]    = prix_reduction
            if "containReduction" in db_cols:
                update_map["containReduction"] = contain_reduc
            if "price"            in db_cols and price_summary  is not None:
                update_map["price"]            = price_summary
            if "price_summary"    in db_cols and price_summary  is not None:
                update_map["price_summary"]    = price_summary

            if not update_map:
                noops += 1
                _log_row("NOOP", event_id=event_id_int, titre=titre, details="aucune valeur à écrire")
                continue

            _exec_update(cursor, "profil_event", event_id_int, update_map)

            if cursor.rowcount == 0:
                skipped += 1
                _log_row("SKIP", event_id=event_id_int, titre=titre, details="id non trouvé en DB")
            else:
                updated += 1
                _log_row(
                    "UPDATE",
                    event_id=event_id_int,
                    titre=titre,
                    details=f"cols={sorted(update_map.keys())} | priceEvent={price_event} prixInitial={prix_initial}",
                )

        connection.commit()
        logging.info(
            "✅ [db] Updated=%s | Skipped=%s | Noop=%s | Errors=%s",
            updated, skipped, noops, errors,
        )

    except Exception as e:
        connection.rollback()
        logging.error("❌ [db] Failure: %s", e)
        raise
    finally:
        cursor.close()
        connection.close()

    context["ti"].xcom_push(key="updated", value=updated)
    context["ti"].xcom_push(key="errors",  value=errors)

    if errors > len(rows) * 0.1:
        raise RuntimeError(f"Trop d'erreurs ({errors}/{len(rows)})")


# ── Task 3 : résumé ──────────────────────────────────────────────────────────

def log_summary(**context):
    total   = context["ti"].xcom_pull(key="total")
    updated = context["ti"].xcom_pull(key="updated")
    errors  = context["ti"].xcom_pull(key="errors")
    logging.info("🏁 [summary] DAG terminé — updated=%s / total=%s | errors=%s", updated, total, errors)


# ── DAG ──────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="update_event_prices",
    start_date=days_ago(1),
    schedule_interval=None,   # déclenché manuellement
    catchup=False,
    default_args=default_args,
    tags=["event", "prices", "profil_event"],
    params={
        "csv_path": Param(
            "/opt/airflow/data/profil_event_prices.csv",
            type="string",
            description="Chemin vers le CSV produit par le notebook laptop",
        ),
    },
) as dag:
    dag.timezone = PARIS_TZ

    t1 = PythonOperator(
        task_id="load_and_validate_csv",
        python_callable=load_and_validate,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="update_price_columns",
        python_callable=update_price_columns,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
        provide_context=True,
    )

    t1 >> t2 >> t3
