import logging
import sys

from pyspark.sql import SparkSession, functions
from pyspark.sql.utils import AnalysisException
import redis


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


class SparkCSVReader:
    def __init__(self):
        self._spark = SparkSession.builder.appName("CSVReader").getOrCreate()

    @staticmethod
    def make_process_partition(redis_params):
        """
        Função que recebe os parâmetros de conexão com o Redis e cria uma função que será passada executada
        para cada partição.
        :param redis_params: Dicionário com os parâmetros de conexão do Redis.
        :return: Função com os parãmetros de conexão do Redis já injetados.
        """

        def process_partition(partition):
            """
            Função chamada para cada partição processada pelo Spark.
            Cria conexão com o banco, cria um dicionário com os dados processados, e salva no banco (caso solicitado).
            :param partition: Partição com os dados (Spark)
            :return: None
            """
            r = redis.Redis(**redis_params)
            data = {}
            for row in partition:
                cidade = row["cidade"]
                data[cidade] = {
                    "capital_social_total": row["capital_social_total"],
                    "quantidade_empresas": row["quantidade_empresas"],
                    "capital_social_medio": row["capital_social_medio"],
                }
            for key, value in data.items():
                r.hset(name=key, mapping=value)

        return process_partition

    def create_dataframe_from_file(self, filename):
        """
        Função que recebe o caminho do arquivo como parâmetro e cria um DataFrame do Spark com os dados lidos.
        :param filename: Caminho do arquivo dentro do Container.
        :return: DataFrame do Spark com os dados lidos.
        """
        try:
            return self._spark.read.csv(filename, header=True, inferSchema=True)
        except AnalysisException as e:
            logger.error(f"[Erro Spark] --- Erro ao ler o arquivo - {e}")
            raise e

    def run_operations(self, dataframe, redis_params: dict, use_redis: bool = False):
        """
        Função principal, faz as agregações e chama a função que salva os dados no banco.
        :param dataframe: DataFrame do Spark com os dados lidos.
        :param redis_params: Dicionário com os parâmetros de conexão do Redis.
        :param use_redis: Booleana que indica se os dados devem ou não ser salvos no banco.
        :return:
        """
        result = dataframe.groupBy("cidade").agg(
            functions.count("cnpj").alias("quantidade_empresas"),
            functions.avg("capital_social").alias("capital_social_medio"),
            functions.sum("capital_social").alias("capital_social_total"),
        )

        if use_redis:
            logger.info(f"Salvando dados no redis.")
            process_func = self.make_process_partition(redis_params)
            result.rdd.foreachPartition(process_func)
        else:
            logger.info(f"Redis desativado, mostrando resultado na tela.")
            result.show()
        return result

    @staticmethod
    def parse_args():
        """
        Função que faz parse dos argumentos passados pelo SparkSubmitOperator do Airflow.
        :return: Todos os argumentos parseados e tratados.
        """
        _caminho_arquivo = sys.argv[1]
        _save_to_db = sys.argv[2].lower() == "true"
        redis_host = sys.argv[3].lower()
        redis_port = int(sys.argv[4])
        redis_db = int(sys.argv[5])

        _redis_info = {"host": redis_host, "port": redis_port, "db": redis_db}

        return _caminho_arquivo, _save_to_db, _redis_info


if __name__ == "__main__":
    spark_reader = SparkCSVReader()
    caminho_arquivo, save_to_db, redis_info = spark_reader.parse_args()
    df = spark_reader.create_dataframe_from_file(caminho_arquivo)
    spark_reader.run_operations(df, redis_info, save_to_db)
