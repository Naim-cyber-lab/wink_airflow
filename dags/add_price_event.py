import logging
import re
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple
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
def _clean_text(text: str) -> str:
    if not text:
        return ""
    t = text.replace("\u202f", " ").replace("\xa0", " ")
    for d in ["–", "—", "−", "‐", "‒"]:
        t = t.replace(d, "-")
    return t


def _to_float(s: str) -> float:
    return float(s.replace(",", "."))


def _pick_best_single_by_context(t: str, singles: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Heuristique:
      - Si un mot-clé type 'Prix', 'Tarif', 'L’addition', 'entrée', etc. est présent,
        on prend le premier prix après ce mot-clé (dans une fenêtre).
      - Sinon: on prend le MAX des prix (cas resto: 9€, 3€, 11€ => 11€)
    """
    if not singles:
        return None

    # Fenêtre pour chercher un prix après un mot clé
    anchors = [
        "prix", "tarif", "l’addition", "l'addition", "addition", "entrée", "entree", "démarre", "demarre"
    ]

    low = t.lower()
    for a in anchors:
        idx = low.find(a)
        if idx != -1:
            window = t[idx: idx + 180]  # petite fenêtre après le mot-clé
            # reprends les mêmes regex "single" pour la fenêtre
            single_re = re.compile(
                r"""(?P<val>\d{1,4}(?:[.,]\d{1,2})?)\s*(?P<cur>€|eur|euros?)(?!\w)""",
                re.IGNORECASE | re.VERBOSE,
            )
            m = single_re.search(window)
            if m:
                return {
                    "type": "single",
                    "value": _to_float(m.group("val")),
                    "currency": "EUR",
                    "raw": m.group(0).strip(),
                }

    # fallback: max value
    return max(singles, key=lambda x: x.get("value", 0.0))


def extract_price_info(text: str) -> Dict[str, Any]:
    """
    Extrait une info prix depuis un texte FR.
    Retour:
      {"mentions": [...], "best": {...}|None}
    """
    if not text:
        return {"mentions": [], "best": None}

    t = _clean_text(text)

    free_re = re.compile(r"\b(gratuit(?:e)?|free|entrée\s+libre|entree\s+libre)\b", re.IGNORECASE)

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

    # Collect mentions (debug/audit)
    mentions: List[str] = []
    for rx in [between_re, range_re, less_re, upto_re, from_re, single_re, euro_scale_re]:
        for m in rx.finditer(t):
            mentions.append(m.group(0).strip())

    # Unique mentions
    uniq, seen = [], set()
    for x in mentions:
        if x and x not in seen:
            uniq.append(x)
            seen.add(x)

    # 1) Gratuit
    mfree = free_re.search(t)
    if mfree:
        return {"mentions": uniq, "best": {"type": "free", "value": 0.0, "currency": "EUR", "raw": mfree.group(0).strip()}}

    # 2) Ranges en priorité
    m = between_re.search(t)
    if m:
        return {
            "mentions": uniq,
            "best": {
                "type": "range",
                "min": _to_float(m.group("min")),
                "max": _to_float(m.group("max")),
                "currency": "EUR",
                "raw": m.group(0).strip(),
                "operator": "between",
            },
        }

    m = range_re.search(t)
    if m:
        return {
            "mentions": uniq,
            "best": {
                "type": "range",
                "min": _to_float(m.group("min")),
                "max": _to_float(m.group("max")),
                "currency": "EUR",
                "raw": m.group(0).strip(),
                "operator": "range",
            },
        }

    m = less_re.search(t) or upto_re.search(t)
    if m:
        return {
            "mentions": uniq,
            "best": {
                "type": "range",
                "min": 0.0,
                "max": _to_float(m.group("val")),
                "currency": "EUR",
                "raw": m.group(0).strip(),
                "operator": "max",
            },
        }

    m = from_re.search(t)
    if m:
        return {
            "mentions": uniq,
            "best": {
                "type": "range",
                "min": _to_float(m.group("val")),
                "max": None,
                "currency": "EUR",
                "raw": m.group(0).strip(),
                "operator": "min",
            },
        }

    # 3) Singles (plusieurs possibles)
    singles: List[Dict[str, Any]] = []
    for ms in single_re.finditer(t):
        singles.append(
            {
                "type": "single",
                "value": _to_float(ms.group("val")),
                "currency": "EUR",
                "raw": ms.group(0).strip(),
            }
        )

    if singles:
        best_single = _pick_best_single_by_context(t, singles)
        return {"mentions": uniq, "best": best_single}

    # 4) scale uniquement si aucun prix numérique
    ms = euro_scale_re.search(t)
    if ms:
        return {"mentions": uniq, "best": {"type": "scale", "scale": ms.group(1), "raw": ms.group(1)}}

    return {"mentions": uniq, "best": None}


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
        # on garde, mais normalement ça arrive seulement si aucun prix numérique
        return best.get("raw")

    return best.get("raw")


# ================== DB QUERIES ==================
BAD_PRICE_VALUES = ("€", "€€", "€€€", "€€€€", "eur", "euros")


def fetch_events_missing_price(limit: int = 500):
    """
    On traite:
      - price NULL/'' OU price dans valeurs inutiles
      - OU "priceEvent" NULL/'' OU "priceEvent" dans valeurs inutiles
    IMPORTANT: schéma actuel => "bioEvent_fr", "bioEvent", "priceEvent", "prixInitial" existent (CamelCase),
               et price est une colonne snake_case.
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
        WHERE
            (
                price IS NULL OR price = '' OR price = ANY(%s)
                OR "priceEvent" IS NULL OR "priceEvent" = '' OR "priceEvent" = ANY(%s)
            )
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
            cur.execute(sql, (list(BAD_PRICE_VALUES), list(BAD_PRICE_VALUES), limit))
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
      - "priceEvent" (raw trouvé)
      - price (normalisé)
      - "prixInitial" uniquement si valeur fournie
    """
    if prix_initial_value is None:
        sql = 'UPDATE profil_event SET "priceEvent" = %s, price = %s WHERE id = %s'
        params = (price_event_raw, price_norm, event_id)
    else:
        sql = 'UPDATE profil_event SET "priceEvent" = %s, price = %s, "prixInitial" = %s WHERE id = %s'
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
    limit = int(Variable.get("PRICE_BATCH_LIMIT", default_var="500"))
    rows = fetch_events_missing_price(limit=limit)

    if not rows:
        logging.info("Aucun événement à traiter (prix déjà présents ou pas de texte).")
        return

    processed = 0
    skipped = 0
    failed = 0

    for r in rows:
        try:
            event_id, titre_fr, bio_fr, bio, prix_initial, current_priceevent, current_price = r

            text = "\n".join([titre_fr, bio_fr, bio]).strip()
            if not text:
                skipped += 1
                continue

            info = extract_price_info(text)
            best = info.get("best")

            if not best:
                skipped += 1
                continue

            price_norm = normalize_price(best)
            if not price_norm or price_norm.strip() in BAD_PRICE_VALUES:
                skipped += 1
                continue

            price_event_raw = (best.get("raw") or price_norm).strip()

            # Protection: ne jamais écrire un raw vide ou juste "€"
            if not price_event_raw or price_event_raw in BAD_PRICE_VALUES:
                # si raw est nul/€ mais normalize est OK, on force le raw à normalize
                price_event_raw = price_norm

            # Remplir prixInitial seulement si c'est vide/0
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
                price_event_raw=str(price_event_raw)[:455],  # max_length "priceEvent"
                price_norm=str(price_norm)[:255],            # max_length price
                prix_initial_value=prix_initial_to_write,
            )

            processed += 1
            logging.info(
                "Event %s updated: old(price=%r, priceEvent=%r) -> new(price=%r, priceEvent=%r, prixInitial=%r)",
                event_id,
                current_price,
                current_priceevent,
                price_norm,
                price_event_raw,
                prix_initial_to_write,
            )

        except Exception as e:
            failed += 1
            logging.exception("Event %s: échec MAJ price (%s)", r[0], e)

    logging.info("Terminé. processed=%s skipped=%s failed=%s", processed, skipped, failed)


# ================== DAG ==================
with DAG(
    dag_id="profil_event_fill_prices",
    description='Complète profil_event.price + "priceEvent" (+ "prixInitial") depuis titre_fr, "bioEvent_fr", "bioEvent"',
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