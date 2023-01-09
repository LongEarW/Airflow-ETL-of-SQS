# ETL off a SQS Queue
The ETL off a SQS Queue implements the whole ETL process through Airflow. And it will lauch a new run every 5 minutes.
The ETL consists 3 steps: 
  - **Data Extraction**: use BashOperator to extract data through awscli-local from SQS
  - **Data Cleaning**: flattern nested Json data, convert to Pandas dataframe; mark PII and convert app version to integer; save the transformed data into csv file.
  - **Data Storage**: insert the csv file into Postgres.


## Table of contents
- Requirements
- Installation
- Maintenance
- Answer to Questions
## Requirements
This ETL requires the following modules:
- [Docker]
- [Docker Compose]

## Installation
1. Clone the repo, enter the directory in terminal and build the services
```
docker compose up -d
```
2. Before move to next step, make sure every service is healthy by checking:
```
docker compose ps
```
3. Move the `sqs_dag.py` in `/dags`;
4. In the Airflow dashboard (http://localhost:8080/, username: airflow, password: airflow), create the PostgreSQL connection at `Admin>Connections>ADD+`: 
  - Connection Id: postgres_sqs
  - Connection Type: Postgres
  - Host: postgres_sqs
  - Login: postgres
  - Password: postgres
  - Port: 5432  
  <img src="./CreateConnection.png" width="200">
5. Turn on the toggle of DAG: `sqs_msg_etl`. Refresh and a DAG runs will start and complete soon. (The sqs_msg_etl DAG instance takes up to 5 minutes to show in dashboard)
   <img src="./TurnOnToggle.png" width="200">
6. You can check the PostgresSQL if the data inserted:

  - enter the container and the postgres CLI
    ```
    docker exec -it sqs_airflow_etl-postgres_sqs-1 /bin/bash
    psql -Upostgres
    ```
  - In postgres CLI, connect to database `postgres` and query on the table
    ```
    \c postgres
    SELECT * FROM user_logins LIMIT 10;
    ```
  ![DatabaseResult](./DatabaseResult.png)


## Maintenance
In the Airflow dashboard (http://localhost:8080/), check `Grid` and `Calendar` for overview of runs' status, and check log for detialed process.

<img src="./Grid.png" width="200">
