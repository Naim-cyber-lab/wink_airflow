import asyncio
import json
import logging
import re
from datetime import timedelta
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import psycopg2
from playwright.async_api import async_playwright

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# ================== CONFIG ==================
PARIS_TZ   = ZoneInfo("Europe/Paris")
DB_CONN_ID = "my_postgres"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}



# ================== URL CLEANER ==================
def _clean_url(raw: str) -> str | None:
    """
    Nettoie et valide une URL Google Reviews avant scraping.
    - Détecte les URLs concaténées (ex: "https://wwwhttps//...")
    - Extrait la première URL valide si plusieurs sont collées
    - Retourne None si l'URL est irrécupérable
    """
    if not raw or not isinstance(raw, str):
        return None

    url = raw.strip()

    # Cas : deux URLs collées — on extrait la dernière occurrence de "https://"
    # ex: "https://wwwhttps//www.google.com/..." ou "urlhttps://www.google.com/..."
    parts = re.split(r'(?<!^)https?://', url)
    if len(parts) > 1:
        # Garde la dernière partie qui ressemble à google.com
        for part in reversed(parts):
            candidate = "https://" + part if not part.startswith("http") else part
            if "google.com" in candidate:
                url = candidate
                break

    # Nettoyage basique
    url = url.replace("wwwhttps//", "www.")  # artefact courant
    url = url.strip()

    # Validation minimale
    if not url.startswith("https://") and not url.startswith("http://"):
        return None
    if "google.com" not in url:
        return None

    return url

# ================== DB HELPERS ==================
def _pg_connect():
    conn = BaseHook.get_connection(DB_CONN_ID)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port or 5432,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )


def fetch_events_to_scrape(limit: int) -> list[tuple[int, str]]:
    """
    Recupere les events ayant urlGoogleMapsAvis renseignee.
    Ordre : jamais scrappe en premier (NULLS FIRST), puis les plus anciens.
    """
    sql = """
        SELECT id, "urlGoogleMapsAvis"
        FROM profil_event
        WHERE "urlGoogleMapsAvis" IS NOT NULL
          AND "urlGoogleMapsAvis" <> ''
        ORDER BY
            google_reviews_updated_at ASC NULLS FIRST,
            id ASC
        LIMIT %s
    """
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            return [(int(r[0]), str(r[1])) for r in cur.fetchall()]
    finally:
        conn.close()


def update_event_reviews_in_db(event_id: int, reviews: list[dict]) -> None:
    """Sauvegarde les avis et horodate dans profil_event."""
    sql = """
        UPDATE profil_event
        SET google_reviews            = %s,
            google_reviews_updated_at = NOW()
        WHERE id = %s
    """
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (json.dumps(reviews, ensure_ascii=False), event_id))
        conn.commit()
    finally:
        conn.close()


# ================== PARSER ==================
def _parse_reviews_from_body(body: str) -> list[dict]:
    """Parse la reponse JSON brute de GetLocalBoqProxy / PrivateLocalSearch."""
    body_clean = re.sub(r"^\)\]\}'\n?", "", body)
    data = json.loads(body_clean)
    raw = data[1][10][2]
    reviews = []
    for r in raw:
        try:
            note       = r[1]
            date       = r[2][0].replace("\xa0", " ")
            author     = r[3][0]
            text       = r[27] if len(r) > 27 and isinstance(r[27], str) else ""
            visit_date = None
            for i in [32, 33, 34]:
                if len(r) > i and isinstance(r[i], str) and r[i].startswith("Visit"):
                    visit_date = r[i]
                    break
            reviews.append({
                "author":     author,
                "rating":     note,
                "date":       date,
                "text":       text,
                "visit_date": visit_date,
            })
        except Exception:
            pass
    return reviews


# ================== SCRAPER PLAYWRIGHT ==================
# JS helpers pour scroller dans le panneau Google Reviews
_JS_FIND_SCROLL = """
() => {
    let best = null, bestHeight = 0;
    for (const el of document.querySelectorAll('*')) {
        const ov = window.getComputedStyle(el).overflowY;
        if ((ov === 'auto' || ov === 'scroll') && el.scrollHeight > el.clientHeight + 100) {
            if (el.scrollHeight > bestHeight) { bestHeight = el.scrollHeight; best = el; }
        }
    }
    if (best) { best.setAttribute('data-scroll-target', 'true'); return true; }
    return false;
}
"""

_JS_SCROLL = """
(pos) => {
    const el = document.querySelector('[data-scroll-target]');
    if (el) { el.scrollTop = pos; return el.scrollTop; }
    window.scrollTo(0, pos); return window.scrollY;
}
"""

_JS_POS = """
() => {
    const el = document.querySelector('[data-scroll-target]');
    if (el) return { top: el.scrollTop, height: el.scrollHeight, client: el.clientHeight };
    return { top: window.scrollY, height: document.body.scrollHeight, client: window.innerHeight };
}
"""


async def _scrape_one_url(
    url: str,
    max_reviews: int,
    scroll_step: int   = 600,
    scroll_delay: float = 1.8,
    max_stale: int     = 8,
) -> list[dict]:
    """
    Ouvre l'URL dans Playwright, intercepte les reponses XHR GetLocalBoqProxy
    et scrolle jusqu'a obtenir max_reviews avis.

    C'est la seule approche fiable : l'API necessite une vraie session Google
    (cookies de session, tokens CSRF) impossibles a reproduire avec requests.
    """
    all_reviews: list[dict] = []
    seen: set[tuple] = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            locale="fr-FR",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        # Cookies de consentement pour bypasser la page consent.google.com
        await context.add_cookies([
            {"name": "CONSENT", "value": "YES+cb.20231020-07-p0.fr+FX+NW",
             "domain": ".google.com", "path": "/"},
            {"name": "SOCS",    "value": "CAESEwgDEgk0OTc5NzMwNzIaAmZyIAEaBgiAnMezBg",
             "domain": ".google.com", "path": "/"},
        ])
        page = await context.new_page()

        # Intercepte toutes les reponses XHR Google Reviews
        async def on_response(response):
            if "GetLocalBoqProxy" not in response.url and "PrivateLocalSearch" not in response.url:
                return
            try:
                body = await response.text()
                # Filtre rapide : on vérifie que c'est bien une réponse d'avis
                # (évite de parser des réponses vides ou d'autres endpoints)
                # On accepte toutes les formes de dates : "il y a", "ago",
                # "cette semaine", "hier", "janvier", dates absolues, etc.
                # Seul vrai signal fiable : présence d'un champ auteur/note
                if "GetLocalBoqProxy" not in response.url and "PrivateLocalSearch" not in response.url:
                    return
                if len(body) < 500:  # réponse trop courte = pas d'avis
                    return
                batch = _parse_reviews_from_body(body)
                for r in batch:
                    key = (r["author"], r["date"])
                    if key not in seen:
                        seen.add(key)
                        all_reviews.append(r)
            except Exception:
                pass

        page.on("response", on_response)

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            await asyncio.sleep(4)  # Attendre le chargement initial des avis

            # Retry find scroll : le panneau avis peut mettre du temps à apparaître
            found_scroll = False
            for attempt in range(3):
                found_scroll = await page.evaluate(_JS_FIND_SCROLL)
                if found_scroll:
                    break
                logging.info("Scroll container non trouvé (tentative %d/3), attente 2s...", attempt + 1)
                await asyncio.sleep(2)

            if not found_scroll:
                logging.warning("Scroll container introuvable après 3 tentatives — fallback window.scroll")

            scroll_pos  = 0
            stale_count = 0
            prev_count  = 0

            while len(all_reviews) < max_reviews:
                scroll_pos += scroll_step
                await page.evaluate(_JS_SCROLL, scroll_pos)
                await asyncio.sleep(scroll_delay)

                pos      = await page.evaluate(_JS_POS)
                at_bottom = (pos["top"] + pos["client"] >= pos["height"] - 50)

                if len(all_reviews) == prev_count:
                    stale_count += 1
                else:
                    stale_count = 0
                    prev_count  = len(all_reviews)

                if stale_count >= max_stale:
                    logging.info("Scroll stale depuis %d tours, arret.", max_stale)
                    break
                if at_bottom and stale_count >= 3:
                    logging.info("Bas de page atteint, arret.")
                    break

        finally:
            await browser.close()

    return all_reviews[:max_reviews]


def scrape_reviews(url: str, max_reviews: int) -> list[dict]:
    """Wrapper synchrone pour appeler le scraper Playwright depuis Airflow."""
    return asyncio.run(_scrape_one_url(url, max_reviews))


# ================== AIRFLOW TASK ==================
def update_google_reviews(**_context):
    """
    Pour chaque event de profil_event ayant urlGoogleMapsAvis renseignee :
      1. Ouvre l'URL avec Playwright (seule approche fiable — l'API Google
         requiert une vraie session navigateur, requests renvoie 400)
      2. Intercepte les reponses XHR GetLocalBoqProxy
      3. Scrolle pour paginer jusqu'a max_reviews avis
      4. Met a jour google_reviews + google_reviews_updated_at dans profil_event
    """
    limit       = int(Variable.get("GOOGLE_REVIEWS_BATCH_LIMIT",   default_var="200"))
    max_reviews = int(Variable.get("GOOGLE_REVIEWS_MAX_PER_EVENT", default_var="20"))

    rows = fetch_events_to_scrape(limit=limit)

    if not rows:
        logging.info("Aucun evenement a traiter.")
        return

    logging.info("%d evenement(s) a scraper (max %d avis chacun)", len(rows), max_reviews)

    processed = skipped = failed = 0

    for event_id, url in rows:
        try:
            clean = _clean_url(url)
            if not clean:
                skipped += 1
                logging.warning(
                    "Event %s : URL invalide ou irrécupérable, ignoré. URL brute: %.120s",
                    event_id, url
                )
                continue
            if clean != url:
                logging.warning(
                    "Event %s : URL nettoyée\n  brut  : %.120s\n  propre: %.120s",
                    event_id, url, clean
                )
            reviews = scrape_reviews(clean, max_reviews)

            if reviews:
                update_event_reviews_in_db(event_id, reviews)
                processed += 1
                logging.info("Event %s : %d avis sauvegardes.", event_id, len(reviews))
            else:
                # On ne touche PAS la DB si 0 avis — l'event sera retenté
                # au prochain run (google_reviews_updated_at reste NULL/ancien)
                skipped += 1
                logging.warning("Event %s : aucun avis recupere (URL peut-etre invalide ou temporairement inaccessible).", event_id)

        except Exception as exc:
            failed += 1
            logging.exception("Event %s : echec (%s)", event_id, exc)

    logging.info(
        "Termine — processed=%d  skipped=%d  failed=%d",
        processed, skipped, failed,
    )


# ================== DAG ==================
with DAG(
    dag_id="profil_event_update_google_reviews",
    description=(
        "Met a jour google_reviews dans profil_event via Playwright "
        "(interception XHR GetLocalBoqProxy). Priorite aux events jamais scrappe."
    ),
    default_args=default_args,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    schedule_interval="@daily",
    tags=["event", "google", "reviews"],
) as dag:
    dag.timezone = PARIS_TZ

    scrape_and_store_reviews = PythonOperator(
        task_id="scrape_and_store_reviews",
        python_callable=update_google_reviews,
    )

    scrape_and_store_reviews
