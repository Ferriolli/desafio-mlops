import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from jobs.job import SparkCSVReader


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[*]").appName("TestCSVReader").getOrCreate()
    )


@pytest.fixture
def sample_dataframe(spark):
    data = [
        {
            "cnpj": "01234",
            "data_abertura": "2025-07-28T09:00:00Z",
            "cidade": "São Paulo",
            "estado": "SP",
            "capital_social": 60000,
        },
        {
            "cnpj": "02345",
            "data_abertura": "2025-07-28T09:30:00Z",
            "cidade": "São Paulo",
            "estado": "SP",
            "capital_social": 40000,
        },
        {
            "cnpj": "03456",
            "data_abertura": "2025-07-28T10:15:00Z",
            "cidade": "Belo Horizonte",
            "estado": "MG",
            "capital_social": 75000,
        },
    ]
    return spark.createDataFrame(data)


def test_create_dataframe_from_file(tmp_path, spark, sample_dataframe):
    csv_path = tmp_path / "test.csv"
    sample_dataframe.write.csv(str(csv_path), header=True, mode="overwrite")

    reader = SparkCSVReader()
    df = reader.create_dataframe_from_file(str(csv_path))
    assert df.count() == 3
    assert set(df.columns) == {
        "cnpj",
        "data_abertura",
        "cidade",
        "estado",
        "capital_social",
    }


def test_run_operations(sample_dataframe):
    reader = SparkCSVReader()
    reader._spark = sample_dataframe.sparkSession

    result_df = reader.run_operations(
        sample_dataframe,
        redis_params={"host": "fake_host", "port": 1234, "db": 0},
        use_redis=False,
    )

    assert set(result_df.columns) == {
        "cidade",
        "quantidade_empresas",
        "capital_social_medio",
        "capital_social_total",
    }


def test_make_process_partition():
    fake_partition = [
        {
            "cidade": "São Paulo",
            "capital_social_total": 30000,
            "quantidade_empresas": 2,
            "capital_social_medio": 15000,
        },
        {
            "cidade": "Belo Horizonte",
            "capital_social_total": 20000,
            "quantidade_empresas": 1,
            "capital_social_medio": 20000,
        },
    ]

    redis_params = {"host": "fake_host", "port": 1234, "db": 0}

    with patch("jobs.job.redis.Redis") as mocked_redis_cls:
        mock_redis = MagicMock()
        mocked_redis_cls.return_value = mock_redis

        process_func = SparkCSVReader.make_process_partition(redis_params)
        process_func(fake_partition)

        mock_redis.hset.assert_any_call(
            name="São Paulo",
            mapping={
                "capital_social_total": 30000,
                "quantidade_empresas": 2,
                "capital_social_medio": 15000,
            },
        )

        mock_redis.hset.assert_any_call(
            name="Belo Horizonte",
            mapping={
                "capital_social_total": 20000,
                "quantidade_empresas": 1,
                "capital_social_medio": 20000,
            },
        )


def test_parse_args(monkeypatch):
    test_args = ["fake_arg", "caminho/arquivo.csv", "True", "fake_redis", "1234", "0"]

    reader = SparkCSVReader()

    monkeypatch.setattr("sys.argv", test_args)

    caminho_arquivo, save_to_db, redis_info = reader.parse_args()

    assert caminho_arquivo == "caminho/arquivo.csv"
    assert save_to_db is True
    assert redis_info["host"] == "fake_redis"
    assert redis_info["port"] == 1234
    assert redis_info["db"] == 0
