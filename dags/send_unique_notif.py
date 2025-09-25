# dag_expo_push_notification.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import re
import json
import logging
import requests  # ajoute 'requests' dans requirements.txt si besoin

PARIS_TZ = ZoneInfo("Europe/Paris")

EXPO_PUSH_URL = os.environ.get(
    "EXPO_PUSH_URL",
    "https://exp.host/--/api/v2/push/send"
)
# Si tu utilises un token d'accès Expo (facultatif mais recommandé côté prod)
EXPO_ACCESS_TOKEN = os.environ.get("EXPO_ACCESS_TOKEN")  # "Bearer <token>"

DEFAULT_PUSH_TOKEN = "ExponentPushToken[Xpc9_0CnSvzouox6mvLuZN]"
DEFAULT_USER_ID = 189
DEFAULT_TITLE = "Des utilisateurs veulent rejoindre votre conversation"
DEFAULT_BODY = "Vous avez 5 nouvelles demandes pour rejoindre votre conversation !"
DEFAULT_DATA = os.environ.get("DEFAULT_PUSH_DATA_JSON", '{}')  # JSON string

TOKEN_REGEX = re.compile(r"^ExponentPushToken\[[A-Za-z0-9_-]+\]$")


def _validate_inputs(push_token: str, user_id: str):
    if not push_token or not TOKEN_REGEX.match(push_token):
        raise ValueError(
            f"Token Expo invalide: '{push_token}'. "
            "Attendu un format ExponentPushToken[...]."
        )
    if not user_id:
        raise ValueError("user_id manquant ou vide.")


def send_unique_notif(**context):
    """
    Envoie une notification Expo à un utilisateur.
    Paramètres attendus dans dag_run.conf (ou valeurs par défaut via env):
      - push_token (str) : ExponentPushToken[...]
      - user_id (str|int) : identifiant interne de l'utilisateur
      - title (str) : titre de la notif (optionnel)
      - body (str)  : contenu de la notif (optionnel)
      - data (dict|str JSON) : payload data (optionnel)
      - priority (str) : 'default' | 'high' (optionnel, défaut 'default')
    """
    conf = context.get("dag_run").conf if context.get("dag_run") else {}

    push_token = conf.get("push_token", DEFAULT_PUSH_TOKEN)
    user_id = str(conf.get("user_id", DEFAULT_USER_ID))
    title = conf.get("title", DEFAULT_TITLE)
    body = conf.get("body", DEFAULT_BODY)
    priority = conf.get("priority", "default")
    data = conf.get("data", DEFAULT_DATA)

    # data peut être un dict ou une string JSON — on unifie
    if isinstance(data, str):
        try:
            data = json.loads(data) if data.strip() else {}
        except json.JSONDecodeError:
            logging.warning("data fourni n'est pas un JSON valide; utilisation d'un dict vide.")
            data = {}
    if not isinstance(data, dict):
        data = {}

    # On glisse toujours l'user_id dans le data pour traçabilité
    data.setdefault("user_id", user_id)

    _validate_inputs(push_token, user_id)

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if EXPO_ACCESS_TOKEN:
        headers["Authorization"] = f"Bearer {EXPO_ACCESS_TOKEN}"

    payload = {
        "to": push_token,
        "title": title,
        "body": body,
        # options utiles
        "sound": "default",
        "priority": priority,  # 'default' ou 'high'
        "data": data,
    }

    logging.info(f"Envoi Expo → {push_token} (user_id={user_id}) : {title} / {body}")
    resp = requests.post(EXPO_PUSH_URL, headers=headers, json=payload, timeout=15)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        logging.error(f"Expo push HTTP error: {e} / body={resp.text}")
        raise

    result = resp.json()
    logging.info(f"Réponse Expo: {json.dumps(result, ensure_ascii=False)}")

    # Vérification basique du retour Expo
    if "data" in result:
        # la v2 renvoie généralement {'data': {'status': 'ok', 'id': '...'}} ou erreur
        data_result = result["data"]
        status = data_result.get("status")
        if status != "ok":
            raise RuntimeError(f"Expo a renvoyé un statut non-ok: {result}")
    elif "errors" in result:
        raise RuntimeError(f"Expo a renvoyé des erreurs: {result['errors']}")

    return result


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="expo_push_notification",
    description="Envoi d'une notification push via Expo à un utilisateur donné",
    start_date=datetime(2024, 1, 1, tzinfo=PARIS_TZ),
    schedule_interval=None,  # lancement manuel
    catchup=False,
    default_args=default_args,
    tags=["expo", "push", "notification", "mobile"],
) as dag:

    dag.timezone = PARIS_TZ

    send_push = PythonOperator(
        task_id="send_unique_notif",
        python_callable=send_unique_notif,
        provide_context=True,
    )
