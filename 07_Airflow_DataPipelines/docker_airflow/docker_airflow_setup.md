# Steps to setup Airflow on Docker

## Get docker-compose file
Download via most recent link here: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
`curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.0/docker-compose.yaml'`

---

## Initialize Airflow database
Creates admin user for Airflow database - apparently based on info in docker-compose file
`sudo docker compose up airflow-init`

---

## Start Airflow services
This will make Airflow available on localhost:/8080 (default login is 'airflow' and pw is 'airflow')
`sudo docker compose up`