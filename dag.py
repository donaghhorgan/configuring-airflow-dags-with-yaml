import json
from pathlib import Path

import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

CONFIG_FILE = 'config.yml'
CONFIG_DB_KEY = 'hello_world_config'


def say_hello(name):
    print('Hello, {}'.format(name))


# Load the DAG configuration, setting a default if none is present
path = Path(__file__).with_name(CONFIG_FILE)
with path.open() as f:
    default_config = yaml.safe_load(f)
default_config = json.loads(json.dumps(default_config, default=str))  # Serialize datetimes to strings
config = Variable.setdefault(CONFIG_DB_KEY, default_config, deserialize_json=True)

# Create the DAG
with DAG(**config['dag']) as dag:
    # Start the graph with a dummy task
    last_task = DummyOperator(task_id='start')

    # Extend the graph with a task for each new name
    for name in config['say_hello']:
        task = PythonOperator(
            task_id='hello_{}'.format(name),
            python_callable=say_hello,
            op_args=(name,)
        )

        last_task >> task
        last_task = task
