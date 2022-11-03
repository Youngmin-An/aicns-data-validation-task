"""
Function level adapters
"""
from rawDataLoader import RawDataLoader, DatePartitionedRawDataLoader
import os
import pendulum
from utils.feature_util import mongo_to_dict
from featureMetadataFetcher import FeatureMetadataFetcher, SensorMetadataFetcher
from univariate.analyzer import AnalysisReport
from univariate.ts_validator import Validator, MockValidator
from univariate.strategy.period import PeriodCalcType
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    FloatType,
    IntegerType,
    StringType,
)
import pyspark.sql.functions as F

__all__ = [
    "get_feature_metadata",
    "get_conf_from_evn",
    "parse_spark_extra_conf",
    "load_raw_data",
    "mock_validate",
    "save_validated_data_to_dwh",
]


def get_feature_metadata(app_conf):
    fetcher: FeatureMetadataFetcher = SensorMetadataFetcher()
    fetcher.get_or_create_conn(app_conf)
    sensors, positions = fetcher.fetch_metadata()
    sensor = next(
        filter(lambda sensor: sensor.ss_id == int(app_conf["FEATURE_ID"]), sensors)
    )  # todo: AICNS-33
    print("sensor: ", mongo_to_dict(sensor))
    return sensor


def get_conf_from_evn():
    """
        Get conn info from env variables
    :return:
    """
    conf = dict()
    try:
        # Feature id
        conf["FEATURE_ID"] = os.getenv("FEATURE_ID")
        # Metadata
        conf["METADATA_HOST"] = os.getenv("METADATA_HOST")
        conf["METADATA_PORT"] = os.getenv("METADATA_PORT")
        conf["METADATA_TYPE"] = os.getenv("METADATA_TYPE", default="sensor")
        conf["METADATA_BACKEND"] = os.getenv("METADATA_BACKEND", default="MongoDB")
        # Data source
        conf["SOURCE_HOST"] = os.getenv("SOURCE_HOST")
        conf["SOURCE_PORT"] = os.getenv("SOURCE_PORT")
        conf["SOURCE_DATA_PATH_PREFIX"] = os.getenv(
            "SOURCE_DATA_PATH_PREFIX", default=""
        )
        conf["SOURCE_BACKEND"] = os.getenv("SOURCE_BACKEND", default="HDFS")
        # Raw data period
        start_datetime = os.getenv("APP_TIME_START")  # yyyy-MM-dd'T'HH:mm:ss
        end_datetime = os.getenv("APP_TIME_END")  # yyyy-MM-dd'T'HH:mm:ss
        conf["APP_TIMEZONE"] = os.getenv("APP_TIMEZONE", default="UTC")

        conf["SPARK_EXTRA_CONF_PATH"] = os.getenv(
            "SPARK_EXTRA_CONF_PATH", default=""
        )  # [AICNS-61]
        conf["start"] = pendulum.parse(start_datetime).in_timezone(conf["APP_TIMEZONE"])
        conf["end"] = pendulum.parse(end_datetime).in_timezone(conf["APP_TIMEZONE"])

        # todo: temp patch for day resolution parsing, so later with [AICNS-59] resolution will be subdivided.
        conf["end"] = conf["end"].subtract(minutes=1)

    except Exception as e:
        print(e)
        raise e
    return conf


def parse_spark_extra_conf(app_conf):
    """
    Parse spark-default.xml style config file.
    It is for [AICNS-61] that is spark operator take only spark k/v confs issue.
    :param app_conf:
    :return: Dict (key: conf key, value: conf value)
    """
    with open(app_conf["SPARK_EXTRA_CONF_PATH"], "r") as cf:
        lines = cf.read().splitlines()
        config_dict = dict(
            list(
                filter(
                    lambda splited: len(splited) == 2,
                    (map(lambda line: line.split(), lines)),
                )
            )
        )
    return config_dict


def load_raw_data(app_conf, feature, time_col_name, data_col_name):
    loader: RawDataLoader = DatePartitionedRawDataLoader()
    loader.prepare_to_load(**app_conf)
    feature_raw_df = (
        loader.load_feature_data_by_object(
            start=app_conf["start"], end=app_conf["end"], feature=feature
        )
        .select(time_col_name, data_col_name)
        .sort(time_col_name)
    )
    return feature_raw_df


def mock_validate(ts: DataFrame, time_col_name: str) -> AnalysisReport:
    """

    :return:
    """
    validator: Validator = (
        MockValidator()
    )  # todo: who has responsibility about automated dependency injection
    report: AnalysisReport = validator.validate(ts=ts, time_col_name=time_col_name)
    return report


def append_partition_cols(ts: DataFrame, time_col_name: str, data_col_name):
    return (
        ts.withColumn("datetime", F.from_unixtime(F.col(time_col_name) / 1000))
        .select(
            time_col_name,
            data_col_name,
            F.year("datetime").alias("year"),
            F.month("datetime").alias("month"),
            F.dayofmonth("datetime").alias("day"),
        )
        .sort(time_col_name)
    )


def save_validated_data_to_dwh(
    ts: DataFrame, time_col_name: str, data_col_name: str, app_conf
):
    """

    :return:
    """
    # todo: transaction
    table_name = "validated"
    SparkSession.getActiveSession().sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} ({time_col_name} BIGINT, {data_col_name} DOUBLE) PARTITIONED BY (feature_id char(10),year int, month int, day int) STORED AS PARQUET"
    )
    period = pendulum.period(app_conf["start"], app_conf["end"])

    # Create partition columns(year, month, day) from timestamp
    partition_df = append_partition_cols(ts, time_col_name, data_col_name)

    for date in period.range("days"):
        # Drop Partition for immutable task
        SparkSession.getActiveSession().sql(
            f"ALTER TABLE {table_name} DROP IF EXISTS PARTITION(feature_id={app_conf['FEATURE_ID']}, year={date.year}, month={date.month}, day={date.day})"
        )
    # Save
    partition_df.select(time_col_name, data_col_name, F.lit(app_conf["FEATURE_ID"]).alias("feature_id"), "year", "month", "day").write.format("hive").mode("append").insertInto(table_name)
