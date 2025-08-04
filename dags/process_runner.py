from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.models import Param
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import redis


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


try:
    REDIS_HOST = os.getenv("REDIS_HOST")
    REDIS_PORT = int(os.getenv("REDIS_PORT"))
    REDIS_DB = int(os.getenv("REDIS_DB"))
except KeyError as ke:
    raise RuntimeError("Missing environment variable") from ke
except ValueError as ve:
    raise RuntimeError(f"Invalid env var {ve}") from ve


def check_redis_connection():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        r.ping()
        logger.info("Conexão com Redis OK")
    except redis.exceptions.ConnectionError as rec:
        logger.error("Erro ao conectar com Redis")
        raise RuntimeError("Redis inacessível") from rec


default_args = {"retries": 3, "retry_delay": timedelta(minutes=3)}

with DAG(
    dag_id="feature_engineering",
    start_date=datetime(2025, 7, 1),
    schedule="@daily",
    default_args=default_args,
    params={
        "caminho_arquivo": Param(
            default="/mlops/data/novas_empresas.csv", type="string"
        ),
        "save_to_db": Param(default=True),
    },
    catchup=False,
) as dag:

    iniciar_processamento = BashOperator(
        task_id="iniciar_processamento_task", bash_command="echo Iniciando pipeline..."
    )

    redis_health_check = PythonOperator(
        task_id="check_redis",
        python_callable=check_redis_connection,
    )

    processar_e_salvar_features = SparkSubmitOperator(
        task_id="processar_e_salvar_features",
        application="/mlops/jobs/job.py",
        conn_id="spark_custom",
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=True,
        application_args=[
            "{{ params.caminho_arquivo }}",
            "{{ params.save_to_db | lower }}",
            REDIS_HOST,
            str(REDIS_PORT),
            str(REDIS_DB),
        ],
        executor_memory="1g",
        driver_memory="1g",
        env_vars={
            "REDIS_HOST": REDIS_HOST,
            "REDIS_PORT": str(REDIS_PORT),
            "REDIS_DB": str(REDIS_DB),
        },
        dag=dag,
    )

    end_task = DummyOperator(task_id="end_task")

    (
        iniciar_processamento
        >> redis_health_check
        >> processar_e_salvar_features
        >> end_task
    )
