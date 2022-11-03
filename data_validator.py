"""
Data validation Task
# 0. Mock validator
"""

from func import *
from pyspark.sql import SparkSession
from univariate.analyzer import AnalysisReport

if __name__ == "__main__":
    # Initialize app
    app_conf = get_conf_from_evn()

    SparkSession.builder.config(
        "spark.hadoop.hive.exec.dynamic.partition", "true"
    ).config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")

    # [AICNS-61]
    if app_conf["SPARK_EXTRA_CONF_PATH"] != "":
        config_dict = parse_spark_extra_conf(app_conf)
        for conf in config_dict.items():
            SparkSession.builder.config(conf[0], conf[1])

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    app_conf["sql_context"] = spark

    # Get feature metadata
    sensor = get_feature_metadata(app_conf)
    data_col_name = (
        "input_data"  # todo: metadata concern or strict validation column names
    )
    time_col_name = "event_time"

    # Load data  # todo: will validated data go dwh?
    ts = load_raw_data(app_conf, sensor, time_col_name, data_col_name)

    # Mock validate data
    mock_validation_report: AnalysisReport = mock_validate(
        ts=ts, time_col_name=time_col_name
    )

    validated_ts = mock_validation_report.parameters["validated_df"]
    # Save validated data and period to DWH
    save_validated_data_to_dwh(
        ts=validated_ts,
        time_col_name=time_col_name,
        data_col_name=data_col_name,
        app_conf=app_conf,
    )
    # todo: store offline report

    spark.stop()
