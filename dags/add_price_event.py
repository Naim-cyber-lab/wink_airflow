import logging
import re
from datetime import timedelta
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import psycopg2
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# ================== CONFIG ==================
PARIS_TZ = ZoneInfo("Europe/Paris")
DB_CONN_ID = "my_postgres"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ================== DB HELPERS ==================
def _pg_connect():
    """Connexion PostgreSQL via la Connection Airflow `my_postgres`."""
    conn = BaseHook.get_connection(DB_CONN_ID)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port or 5432,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )


# ================== PRICE PARSER ==================
def extract_price_info(text: str) -> Dict[str, Any]:
    """
    Extrait une info prix depuis un texte FR.
    Retour:
      {
        "mentions": [...],
        "best": { ... } | None
      }
    best:
      - type=single: {"type":"single","value":12.0,"currency":"EUR","raw":"12€"}
      - type=range: {"type":"range","min":10.0,"max":20.0|None,"currency":"EUR","raw":"entre 10 et 20€","operator":"between|range|max|min"}
      - type=free:  {"type":"free","value":0.0,"currency":"EUR","raw":"gratuit"}
      - type=scale: {"type":"scale","scale":"€€","raw":"€€"}
    """
    if not text:
        return {"mentions": [], "best": None}

    t = text.replace("\u202f", " ").replace("\xa0", " ")
    for d in ["–", "—", "−", "‐", "‒"]:
        t = t.replace(d, "-")

    free_re = re.compile(r"\b(gratuit(?:e)?|free|entrée\s+libre|entree\s+libre)\b", re.IGNORECASE)

    def to_float(s: str) -> float:
        return float(s.replace(",", "."))

    less_re = re.compile(
        r"""\b(?:moins\s+de|à\s+moins\s+de|<)\s*(?P<val>\d{1,4}(?:[.,]\d{1,2})?)\s*(?P<cur>€|eur|euros?)(?!\w)""",
        re.IGNORECASE | re.VERBOSE,
    )
    upto_re = re.compile(
        r"""\b(?:jusqu['’]?\s*à|jusqua|maximum|max|<=)\s*(?P<val>\d{1,4}(?:[.,]\d{1,2})?)\s*(?P<cur>€|eur|euros?)(?!\w)""",
        re.IGNORECASE | re.VERBOSE,
    )
    from_re = re.compile(
        r"""\b(?:à\s+partir\s+de|a\s+partir\s+de|dès|des|min(?:imum)?|>=)\s*(?P<val>\d{1,4}(?:[.,]\d{1,2})?)\s*(?P<cur>€|eur|euros?)(?!\w)""",
        re.IGNORECASE | re.VERBOSE,
    )
    between_re = re.compile(
        r"""\b(?:entre)\s*(?P<min>\d{1,4}(?:[.,]\d{1,2})?)\s*(?:et)\s*(?P<max>\d{1,4}(?:[.,]\d{1,2})?)\s*(?P<cur>€|eur|euros?)(?!\w)""",
        re.IGNORECASE | re.VERBOSE,
    )
    range_re = re.compile(
        r"""(?P<min>\d{1,4}(?:[.,]\d{1,2})?)\s*(?:-|à)\s*(?P<max>\d{1,4}(?:[.,]\d{1,2})?)\s*(?P<cur>€|eur|euros?)(?!\w)""",
        re.IGNORECASE | re.VERBOSE,
    )
    single_re = re.compile(
        r"""(?P<val>\d{1,4}(?:[.,]\d{1,2})?)\s*(?P<cur>€|eur|euros?)(?!\w)""",
        re.IGNORECASE | re.VERBOSE,
    )
    euro_scale_re = re.compile(r"(?<!\w)(€{1,4})(?!\w)")

    mentions = []

    for rx in [between_re, range_re, less_re, upto_re, from_re, single_re, euro_scale_re]:
        for m in rx.finditer(t):
            mentions.append(m.group(0).strip())

    if free_re.search(t):
        best = {"type": "free", "value": 0.0, "currency": "EUR", "raw": free_re.search(t).group(0).strip()}
    else:
        best = None

        m = between_re.search(t)
        if m:
            best = {
                "type": "range",
                "min": to_float(m.group("min")),
                "max": to_float(m.group("max")),
                "currency": "EUR",
                "raw": m.group(0).strip(),
                "operator": "between",
            }
        else:
            m = range_re.search(t)
            if m:
                best = {
                    "type": "range",
                    "min": to_float(m.group("min")),
                    "max": to_float(m.group("max")),
                    "currency": "EUR",
                    "raw": m.group(0).strip(),
                    "operator": "range",
                }
            else:
                m = less_re.search(t) or upto_re.search(t)
                if m:
                    best = {
                        "type": "range",
                        "min": 0.0,
                        "max": to_float(m.group("val")),
                        "currency": "EUR",
                        "raw": m.group(0).strip(),
                        "operator": "max",
                    }
                else:
                    m = from_re.search(t)
                    if m:
                        best = {
                            "type": "range",
                            "min": to_float(m.group("val")),
                            "max": None,
                            "currency": "EUR",
                            "raw": m.group(0).strip(),
                            "operator": "min",
                        }
                    else:
                        ms = euro_scale_re.search(t)
                        if ms:
                            best = {"type": "scale", "scale": ms.group(1), "raw": ms.group(1)}
                        else:
                            m2 = single_re.search(t)
                            if m2:
                                best = {
                                    "type": "single",
                                    "value": to_float(m2.group("val")),
                                    "currency": "EUR",
                                    "raw": m2.group(0).strip(),
                                }

    uniq, seen = [], set()
    for x in mentions:
        if x and x not in seen:
            uniq.append(x)
            seen.add(x)

    return {"mentions": uniq, "best": best}


def normalize_price(best: Optional[Dict[str, Any]]) -> Optional[str]:
    """Retourne une string propre à mettre dans profil_event.price (char)."""
    if not best:
        return None

    t = best.get("type")
    if t == "free":
        return "0€ (gratuit)"

    if t == "single":
        v = best.get("value")
        if v is None:
            return None
        vv = int(v) if float(v).is_integer() else v
        return f"{vv}€"

    if t == "range":
        mn, mx = best.get("min"), best.get("max")
        op = best.get("operator")

        if op == "min" and mn is not None:
            vv = int(mn) if float(mn).is_integer() else mn
            return f"à partir de {vv}€"

        if op == "max" and mx is not None:
            vv = int(mx) if float(mx).is_integer() else mx
            return f"jusqu'à {vv}€"

        if mn is not None and mx is not None:
            a = int(mn) if float(mn).is_integer() else mn
            b = int(mx) if float(mx).is_integer() else mx
            return f"{a}–{b}€"

        if mn is not None and mx is None:
            a = int(mn) if float(mn).is_integer() else mn
            return f"à partir de {a}€"

        return best.get("raw")

    if t == "scale":
        return best.get("raw")

    return best.get("raw")


# ================== DB QUERIES ==================
def fetch_events_missing_price(limit: int = 500):
    """
    On vise les events sans info prix:
      - priceevent NULL/'' ET price NULL/''
    On prend les champs texte pour extraction + prixinitial pour éventuellement le remplir.
    IMPORTANT: colonnes Postgres en snake_case (bioevent_fr, bioevent, priceevent, prixinitial).
    """
    sql = """
        SELECT
            id,
            COALESCE(titre_fr, '') AS titre_fr,
            COALESCE("bioEvent_fr", '') AS bioevent_fr,
            COALESCE("bioEvent", '') AS bioevent,
            COALESCE("prixInitial", 0) AS prixinitial,
            COALESCE("priceEvent", '') AS priceevent,
            COALESCE(price, '') AS price
        FROM profil_event
        WHERE ("priceEvent" IS NULL OR "priceEvent" = '')
          AND (price IS NULL OR price = '')
          AND (
                COALESCE(titre_fr,'') <> ''
             OR COALESCE("bioEvent_fr",'') <> ''
             OR COALESCE("bioEvent",'') <> ''
          )
        ORDER BY id ASC
        LIMIT %s
    """
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            return cur.fetchall()
    finally:
        conn.close()


def update_event_price(
    event_id: int,
    price_event_raw: str,
    price_norm: str,
    prix_initial_value: Optional[float],
) -> None:
    """
    Met à jour:
      - priceevent (raw trouvé)
      - price (normalisé)
      - prixinitial uniquement si valeur fournie (et qu'on veut l'écrire)
    """
    if prix_initial_value is None:
        sql = "UPDATE profil_event SET priceevent = %s, price = %s WHERE id = %s"
        params = (price_event_raw, price_norm, event_id)
    else:
        sql = "UPDATE profil_event SET priceevent = %s, price = %s, prixinitial = %s WHERE id = %s"
        params = (price_event_raw, price_norm, float(prix_initial_value), event_id)

    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


# ================== AIRFLOW TASK ==================
def add_price_to_event(**_context):
    """
    - récupère les events sans prix
    - extrait un prix depuis titre_fr/bioevent_fr/bioevent
    - met à jour priceevent + price (+ prixinitial si pertinent)
    """
    limit = int(Variable.get("PRICE_BATCH_LIMIT", default_var="500"))
    rows = fetch_events_missing_price(limit=limit)

    if not rows:
        logging.info("Aucun événement à traiter (prix déjà présents).")
        return

    processed = 0
    skipped = 0
    failed = 0

    for r in rows:
        try:
            event_id, titre_fr, bio_fr, bio, prix_initial, _, _ = r

            text = "\n".join([titre_fr, bio_fr, bio]).strip()

            info = extract_price_info(text)
            best = info.get("best")

            if not best:
                skipped += 1
                continue

            price_norm = normalize_price(best)
            if not price_norm:
                skipped += 1
                continue

            price_event_raw = best.get("raw") or price_norm

            prix_initial_to_write = None
            try:
                current_pi = float(prix_initial or 0)
            except Exception:
                current_pi = 0.0

            if current_pi <= 0.0:
                if best.get("type") == "single":
                    prix_initial_to_write = float(best["value"])
                elif best.get("type") == "range":
                    mn = best.get("min")
                    if mn is not None:
                        prix_initial_to_write = float(mn)
                elif best.get("type") == "free":
                    prix_initial_to_write = 0.0

            update_event_price(
                event_id=int(event_id),
                price_event_raw=str(price_event_raw)[:455],  # max_length priceEvent (Django)
                price_norm=str(price_norm)[:255],            # max_length price
                prix_initial_value=prix_initial_to_write,
            )

            processed += 1
            logging.info(
                "Event %s: priceevent=%s | price=%s | prixinitial=%s",
                event_id,
                price_event_raw,
                price_norm,
                prix_initial_to_write,
            )

        except Exception as e:
            failed += 1
            logging.exception("Event %s: échec MAJ price (%s)", r[0], e)

    logging.info("Terminé. processed=%s skipped=%s failed=%s", processed, skipped, failed)


# ================== DAG ==================
with DAG(
    dag_id="profil_event_fill_prices",
    description="Complète profil_event.price/priceevent (et éventuellement prixinitial) depuis titre_fr, bioevent_fr, bioevent",
    default_args=default_args,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    schedule_interval=None,  # mets "@hourly" ou "0 3 * * *" si tu veux automatique
    tags=["event", "price", "profil_event"],
) as dag:
    dag.timezone = PARIS_TZ

    fill_prices = PythonOperator(
        task_id="fill_prices",
        python_callable=add_price_to_event,
    )

    fill_prices