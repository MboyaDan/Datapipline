import uuid
from datetime import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define default arguments
default_args = {
    'owner': 'airscholar',
    'start_date': dt(2024, 9, 25, 11, 00)
}

# Function to get data from the API
def get_data():
    import json
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res

# Function to format the data
def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{location['street']['number']} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

# Function to stream the data into Kafka
def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:  # Stream data for 1 minute
            break
        try:
            res = get_data()
            formatted_data = format_data(res)

            producer.send('users_created', json.dumps(formatted_data).encode('utf-8'))
            producer.flush()
            logging.info(f'Sent data: {formatted_data}')
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue

# Define the DAG
with DAG(
    'User_automation',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:

    # Task to stream data
    streaming_task = PythonOperator(
        task_id='streaming_data_from_api',
        python_callable=stream_data
    )
