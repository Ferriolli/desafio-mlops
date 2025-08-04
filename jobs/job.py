import logging
import sys

from pyspark.sql import SparkSession, functions
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
        def process_partition(partition):
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
        return self._spark.read.csv(filename, header=True, inferSchema=True)

    def run_operations(self, dataframe, redis_params: dict, use_redis: bool = False):
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
