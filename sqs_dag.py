""
This file creates the DAG for the ETL workflow, and will be parsed by Airflow
Put this file under directory /dags

Supports basic configuration:
    FETCH_TOKEN  - encyption token for PII
    MAX_VERSION_BIT  - maxium sub version length
    START_DATE - for workflow schedule inital date
    DAG_SCHEDULE  - for workflow frequency
    
@author Wenqing Wei
"""

from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator

import pandas as pd
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from datetime import datetime
import json
import base64

FETCH_TOKEN = "FETCH_TOKEN_1234"  # encyption token
MAX_VERSION_BIT = 8   # max sub version length, according to sematic versioning: x.y.z, x,y,z <= MAX_VERSION_BIT
START_DATE = datetime(2023, 1, 1)  # date the workflow start
DAG_SCHEDULE = '*/5 * * * *'  # runs frequency, defualt is every 5 minutes

"""
The function accecpts a string and make encyption with ECB. 
Return an encypted and decoded string
@param text (string)
@return encypted text
"""
def encypt_text(text):
    aes = AES.new(FETCH_TOKEN.encode("utf8"), AES.MODE_ECB)
    encypt_text = aes.encrypt(pad(text.encode("utf8"), 16))
    encypt_text = str(base64.encodebytes(encypt_text), encoding='utf-8').replace('\n', '')
    return encypt_text

"""
The fucntion converts accept a version string and convert into version number with bit mask.
The sub version should be within the range of MAX_VERSION_BIT to ensure comparable and recover of the converted version number.
@param version (string)
@return version number (integer)
"""
def version_to_integer(version):
    version_bits = version.split('.')
    if len(version_bits) == 3:
        version_number_x = int(version_bits[0]) << (MAX_VERSION_BIT * 2) 
        version_number_y = int(version_bits[1]) << MAX_VERSION_BIT
        version_number_z = int(version_bits[2])
    elif len(version_bits) == 2:
        version_number_x = int(version_bits[0]) << (MAX_VERSION_BIT * 2) 
        version_number_y = int(version_bits[1]) << MAX_VERSION_BIT
        version_number_z = 0
    elif len(version_bits) == 1:
        version_number_x = int(version_bits[0]) << (MAX_VERSION_BIT * 2) 
        version_number_y = 0
        version_number_z = 0
    return version_number_x | version_number_y | version_number_z
    
"""
The DAG decorator for the ETL workflow
@param start_date (datetime)
@param START_DATE (datetime)
@param catchup (boolean): if catch up history runs since start_date
@param schedule: CRON expression schedule for the workflow
"""
@dag(
    dag_id='sqs_msg_etl',
    start_date=START_DATE,
    catchup=False,
    schedule=DAG_SCHEDULE
)
def tasks_flow():
    # extract data from SQS, the output is concatenated into single line for inter-task transmission
    extract_msg = BashOperator(
        task_id="extract_msg",
        bash_command="awslocal sqs receive-message  --queue-url http://localhost:4566/000000000000/login-queue --endpoint-url http://localstack:4566/  | tr '\n' ' '"
    )

    # data cleaning
    @task(task_id='process_msg')
    def process_extracted_user(msg_infos):
        # first json parse: string to dataframe with nested columns
        processed_msg = pd.json_normalize(data=json.loads(msg_infos)["Messages"])  
        # second json parse: select and flatten nested columns
        processed_msg = processed_msg['Body'].apply(lambda x: json.loads(x))
        processed_msg = pd.DataFrame(list(processed_msg))
        # fields selection and transformation
        processed_msg['masked_ip'] = processed_msg['ip'].apply(encypt_text)
        processed_msg['masked_device_id'] = processed_msg['device_id'].apply(encypt_text)
        processed_msg['app_version'] = processed_msg['app_version'].apply(version_to_integer)
        processed_msg = processed_msg[['user_id', 'device_type', 'masked_ip', 'masked_device_id', 'locale', 'app_version']]
        processed_msg['create_date'] = datetime.now()
        # store data as csv file for further operation
        processed_msg.to_csv('/tmp/processed_msg.csv', index=None, header=False)

    # create hook for postgres of SQS messages
    postgres_hook = PostgresHook(
        postgres_conn_id="postgres_sqs"
    )
    # insert data into postgres
    @task(task_id='store_msg')
    def store_msg():
        postgres_hook.copy_expert(
            sql="COPY user_logins FROM stdin WITH DELIMITER as ','",
            filename='/tmp/processed_msg.csv'
        )
    # specify task dependencies
    extract_msg >> process_extracted_user(extract_msg.output) >> store_msg()

sqs_etl_dag = tasks_flow()
