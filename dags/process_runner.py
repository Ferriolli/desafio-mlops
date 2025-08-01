from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG
from datetime import datetime


with DAG(
    dag_id='feature_engineering',
    start_date=datetime(2025, 7, 1),
    schedule='@daily',
    catchup=False
) as dag:

    iniciar_processamento = BashOperator(task_id='iniciar_processamento_task', bash_command="echo Iniciando pipeline...")

    processar_e_salvar_features = SparkSubmitOperator(
        task_id='processar_e_salvar_features',
        application='/opt/airflow/jobs/job.py',
        conn_id='spark_custom',
        conf={"spark.master": "spark://spark-master:7077"},
        # master='spark://spark-master:7077',
        # application_args=[],
        verbose=True,
        executor_memory='1g',
        driver_memory='1g',
        dag=dag,
    )

    end_task = DummyOperator(task_id='end_task')

    iniciar_processamento >> processar_e_salvar_features >> end_task
