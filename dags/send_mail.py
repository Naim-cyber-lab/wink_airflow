from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
import logging
import pytz
import traceback

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ────────────────────────────────────────────────────────────────────────────────
# Envoi e-mail (Mailjet) : réutilise ta fonction, avec fallback depuis Variables
# ────────────────────────────────────────────────────────────────────────────────
def envoyer_mail(destinataire: str, sujet: str, corps: str):
    print(f"📧 Préparation de l'envoi d'un email à : {destinataire}")
    try:
        from mailjet_rest import Client
    except Exception as e:
        print("⚠️ mailjet_rest non installé dans l'environnement :", str(e))
        traceback.print_exc()
        return

    # Essaye d'abord Airflow Variables, sinon utilise les constantes fournies
    API_KEY = Variable.get("MAILJET_API_KEY", default_var="3ce0be795c018accb4374777d5f8c1b0")
    API_SECRET = Variable.get("MAILJET_API_SECRET", default_var="24498e4564332925daa27dbab6798359")

    try:
        mailjet = Client(auth=(API_KEY, API_SECRET), version='v3.1')
        data = {
            'Messages': [
                {
                    "From": {"Email": "nisuapp@gmail.com", "Name": "NisuApp"},
                    "To": [{"Email": destinataire}],
                    "Subject": sujet,
                    "TextPart": corps,
                }
            ]
        }
        print("🚀 Envoi du mail via Mailjet...")
        result = mailjet.send.create(data=data)
        print(f"📬 Status code: {result.status_code}")
        if result.status_code == 200:
            print("✅ Mail envoyé avec succès")
        else:
            print(f"❌ Échec de l'envoi : {result.status_code} - {result.json()}")
    except Exception as e:
        print("⚠️ Exception lors de l'envoi du mail :", str(e))
        traceback.print_exc()


PARIS = ZoneInfo("Europe/Paris")

default_args = {
    "start_date": datetime(2023, 1, 1, tzinfo=PARIS),
    "retries": 0,
}

DAG_ID = "reminders_conversations_J3_J2_J1"

# SQL : récupère toutes les conversations qui commencent dans N jours (1,2,3)
# et les e-mails des participants.
SQL_CONV_PARTICIPANTS_IN_D_DAYS = """
WITH target AS (
  SELECT
    c.id AS conversation_id,
    c."date" AS start_at,
    COALESCE(c.title, 'Votre rencontre') AS title
  FROM profil_conversationactivity c
  WHERE DATE((c."date" AT TIME ZONE 'Europe/Paris'))
        = DATE((NOW() AT TIME ZONE 'Europe/Paris') + (%(days)s || ' days')::interval)
)
SELECT
  t.conversation_id,
  t.start_at,
  t.title,
  w.email                        AS email,
  COALESCE(w.first_name, '')     AS first_name,
  COALESCE(w.last_name,  '')     AS last_name
FROM target t
JOIN profil_participantconversationactivity p
  ON p."conversationActivity_id" = t.conversation_id
JOIN profil_winker w
  ON w.id = p."winker_id"
WHERE w.email IS NOT NULL AND w.email <> '';
"""

def _format_date_fr(dt: datetime) -> str:
    # Affiche la date/heure en fuseau Paris
    dtp = dt.astimezone(PARIS)
    # Exemple : "mardi 5 novembre 2025 à 19:30"
    jours = ["lundi","mardi","mercredi","jeudi","vendredi","samedi","dimanche"]
    mois  = ["janvier","février","mars","avril","mai","juin","juillet","août","septembre","octobre","novembre","décembre"]
    j = jours[dtp.weekday()]
    m = mois[dtp.month - 1]
    return f"{j} {dtp.day} {m} {dtp.year} à {dtp:%H:%M}"

def build_subject(days: int, title: str, start_at: datetime) -> str:
    if days == 1:
        prefix = "Rappel J-1"
    elif days == 2:
        prefix = "Rappel J-2"
    else:
        prefix = "Rappel J-3"
    return f"[{prefix}] {title} — {start_at.astimezone(PARIS):%d/%m/%Y %H:%M}"

def build_body(days: int, title: str, start_at: datetime) -> str:
    when_txt = _format_date_fr(start_at)
    if days == 1:
        horizon = "demain"
    else:
        horizon = f"dans {days} jours"
    return (
        f"Bonjour,\n\n"
        f"Ceci est un rappel : la rencontre « {title} » commence {horizon}, le {when_txt}.\n\n"
        f"Si vous ne pouvez finalement pas venir, merci de prévenir tout le monde avant le début de la rencontre "
        f"afin d'ajuster l'organisation.\n\n"
        f"À très vite,\nL'équipe NisuApp"
    )

def send_reminders_for_offset(days: int, **context):
    """
    Envoie les rappels pour toutes les conversations qui commencent dans `days` jours (J-1, J-2, J-3).
    """
    hook = PostgresHook(postgres_conn_id="my_postgres")
    rows = hook.get_records(SQL_CONV_PARTICIPANTS_IN_D_DAYS, parameters={"days": days})

    if not rows:
        logging.info(f"Aucune conversation à rappeler pour J-{days}.")
        return

    # rows: conversation_id, start_at, title, email, first_name, last_name
    count = 0
    for conv_id, start_at, title, email, first_name, last_name in rows:
        subject = build_subject(days, title, start_at)
        body = build_body(days, title, start_at)
        logging.info(f"Envoi J-{days} -> conv {conv_id} -> {email}")
        try:
            envoyer_mail(email, subject, body)
            count += 1
        except Exception as e:
            logging.exception(f"Echec envoi à {email} (conv {conv_id}) : {e}")

    logging.info(f"J-{days} : {count} e-mails envoyés.")

with DAG(
    dag_id=DAG_ID,
    schedule="0 10 * * *",
    default_args=default_args,
    catchup=False,
    tags=["reminder", "conversations", "email"],
) as dag:

    j3 = PythonOperator(
        task_id="send_reminders_J_minus_3",
        python_callable=send_reminders_for_offset,
        op_kwargs={"days": 3},
    )

    j2 = PythonOperator(
        task_id="send_reminders_J_minus_2",
        python_callable=send_reminders_for_offset,
        op_kwargs={"days": 2},
    )

    j1 = PythonOperator(
        task_id="send_reminders_J_minus_1",
        python_callable=send_reminders_for_offset,
        op_kwargs={"days": 1},
    )

    # ordre non strict nécessaire, mais on peut chaîner pour la lisibilité
    j3 >> j2 >> j1
