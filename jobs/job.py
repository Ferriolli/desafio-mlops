from pyspark.sql import SparkSession, functions
import redis


class SparkCSVReader:
    def __init__(self):
        self._spark = SparkSession.builder.appName("CSVReader").getOrCreate()

    @staticmethod
    def process_partition(partition):
        redis_connection = redis.Redis(host="redis", port=6379, db=0)
        for row in partition:
            cidade = row['cidade']
            data = {
                'capital_social_total': row['capital_social_total'],
                'quantidade_empresas': row['quantidade_empresas'],
                'capital_social_medio': row['capital_social_medio']
            }

            redis_connection.hset(f"cidade: {cidade}", mapping=data)

    def create_dataframe_fom_file(self, filename):
        return self._spark.read.csv(filename, header=True, inferSchema=True)

    def run_operations(self, dataframe):
        result = (
            dataframe.groupBy('cidade')
            .agg(
                functions.count('cnpj').alias("quantidade_empresas"),
                functions.avg("capital_social").alias("capital_social_medio"),
                functions.sum("capital_social").alias("capital_social_total")
            )
        )

        result.rdd.foreachPartition(self.process_partition)


if __name__ == '__main__':
    spark_reader = SparkCSVReader()
    df = spark_reader.create_dataframe_fom_file('/opt/airflow/jobs/novas_empresas.csv')
    spark_reader.run_operations(df)
