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
    def process_partition(partition):
        data = {}

        for row in partition:
            cidade = row["cidade"]
            data[cidade] = {
                "capital_social_total": row["capital_social_total"],
                "quantidade_empresas": row["quantidade_empresas"],
                "capital_social_medio": row["capital_social_medio"],
            }
        SparkCSVReader.save_to_redis(data=data)

    @staticmethod
    def save_to_redis(data: dict):
        redis_connection = redis.Redis(host='redis', port=6379, db=0)
        for key, value in data.items():
            redis_connection.hset(name=f"cidade: {key}", mapping=value)

    def create_dataframe_from_file(self, filename):
        return self._spark.read.csv(filename, header=True, inferSchema=True)

    def run_operations(self, dataframe, use_redis):
        result = dataframe.groupBy("cidade").agg(
            functions.count("cnpj").alias("quantidade_empresas"),
            functions.avg("capital_social").alias("capital_social_medio"),
            functions.sum("capital_social").alias("capital_social_total"),
        )

        if use_redis:
            logger.info(f"Salvando informação no redis.")
            result.rdd.foreachPartition(self.process_partition)
        else:
            logger.info(f"Redis desativado, mostrando resultado na tela.")
            result.show()
        return result


if __name__ == "__main__":
    spark_reader = SparkCSVReader()
    caminho_arquivo = sys.argv[1]
    save_to_db = sys.argv[2].lower() == "true"
    df = spark_reader.create_dataframe_from_file(caminho_arquivo)
    spark_reader.run_operations(df, save_to_db)
