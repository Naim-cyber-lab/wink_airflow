#!/bin/bash

# Installer les dépendances Python
pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Initialiser DB et créer un utilisateur
airflow db migrate
airflow users create \
  --username "$_AIRFLOW_WWW_USER_USERNAME" \
  --password "$_AIRFLOW_WWW_USER_PASSWORD" \
  --firstname "$_AIRFLOW_WWW_USER_FIRSTNAME" \
  --lastname "$_AIRFLOW_WWW_USER_LASTNAME" \
  --role Admin \
  --email "$_AIRFLOW_WWW_USER_EMAIL"

# Lancer scheduler + webserver
airflow scheduler &
exec airflow webserver
