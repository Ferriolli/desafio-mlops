import pytest
from pyspark.sql import SparkSession
from jobs.job import SparkCSVReader


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master('local[*]').appName("TestCSVReader").getOrCreate()


@pytest.fixture
def sample_dataframe(spark):
    data = [
        {"cnpj": "01234", "data_abertura": "2025-07-28T09:00:00Z", "cidade": "São Paulo", "estado": "SP", "capital_social": 60000},
        {"cnpj": "02345", "data_abertura": "2025-07-28T09:30:00Z", "cidade": "São Paulo", "estado": "SP", "capital_social": 40000},
        {"cnpj": "03456", "data_abertura": "2025-07-28T10:15:00Z", "cidade": "Balo Horizonte", "estado": "MG", "capital_social": 75000}
    ]
    return spark.createDataFrame(data)


def test_create_dataframe_from_file(tmp_path, spark, sample_dataframe):
    csv_path = tmp_path / "test.csv"
    sample_dataframe.write.csv(str(csv_path), header=True, mode="overwrite")

    reader = SparkCSVReader()
    df = reader.create_dataframe_from_file(str(csv_path))
    assert df.count() == 3
    assert set(df.columns) == {'cnpj', 'data_abertura', 'cidade', 'estado', 'capital_social'}


def test_run_operations(sample_dataframe):
    reader = SparkCSVReader()
    reader._spark = sample_dataframe.sparkSession

    result_df = reader.run_operations(sample_dataframe, use_redis=False)

    assert set(result_df.columns) == {'cidade', 'quantidade_empresas', 'capital_social_medio', 'capital_socaial_total'}
