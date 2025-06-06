cd airflow-minimal

docker run -d --rm --name airflow -p 8080:8080 -v /c/Users/naims/Documents/nisu/airflow/dags:/opt/airflow/dags apache/airflow:2.9.1 standalone



docker exec -it airflow airflow users create  --username admin --password admin --firstname Admin --lastname Admin  --role Admin  --email naim.soun1789i@outlook.fr
