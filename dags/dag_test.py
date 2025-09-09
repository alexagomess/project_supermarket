from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dag_test",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={"codigo_nf": "2003325650090001524461842994705"},
) as dag:

    def print_hello():
        print("Hello World man!")

    hello_word = PythonOperator(
        task_id="hello_task",
        python_callable=print_hello,
    )

    raw_data = BashOperator(
        task_id="raw_data_task",
        bash_command="""
        cd /opt/airflow/scripts/raw && 
        export PYTHONPATH=/opt/airflow:/opt/airflow/scripts:$PYTHONPATH && 
        echo "Iniciando script com código: {{ params.codigo_nf }}" &&
        python -u web_scraping.py {{ params.codigo_nf }}
        """,
        execution_timeout=timedelta(
            minutes=10
        ),  # Timeout maior para permitir web scraping
    )

    hello_word >> raw_data
