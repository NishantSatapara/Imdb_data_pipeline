from datetime import datetime

from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="imdb_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "hive"],
)
def imdb_etl():

    # raw_layer = SparkSubmitOperator(
    #     task_id="spark_raw_layer",
    #     application="/opt/airflow/etl_script/raw_data_load.py",
    #     deploy_mode="client",
    #     executor_memory="2G",
    #     executor_cores=2,
    #     num_executors=2,
    #     total_executor_cores=4,
    #     driver_memory="2G",
    #     verbose=True,
    # )

    # bronze_layer = SparkSubmitOperator(
    #     task_id="spark_bronze_layer",
    #     application="/opt/airflow/etl_script/bronze_data_load.py",
    #     deploy_mode="client",
    #     executor_memory="2G",
    #     executor_cores=2,
    #     num_executors=2,
    #     driver_memory="2G",
    #     total_executor_cores=4,
    #     verbose=True,
    # ) 

    # silver_layer = SparkSubmitOperator(
    #     task_id="spark_silver_layer",
    #     application="/opt/airflow/etl_script/silver_data_load.py",
    #     deploy_mode="client",
    #     executor_memory="2G",
    #     executor_cores=2,
    #     num_executors=2,
    #     driver_memory="2G",
    #     total_executor_cores=4,
    #     verbose=True,
    # )

    gold_layer = SparkSubmitOperator(
        task_id="spark_gold_layer",
        application="/opt/airflow/etl_script/gold_data_load.py",
        deploy_mode="client",
        executor_memory="2G",
        executor_cores=2,
        num_executors=2,
        driver_memory="2G",
        total_executor_cores=4,
        verbose=True,
    )

    olap_layer = SparkSubmitOperator(
        task_id="olap_load_layer",
        application="/opt/airflow/etl_script/load_to_olap.py",
        deploy_mode="client",
        executor_memory="2G",
        executor_cores=2,
        num_executors=2,
        driver_memory="2G",
        total_executor_cores=4,
        verbose=True,
    )

    #raw_layer>>bronze_layer>>silver_layer>>
    gold_layer >>olap_layer

imdb_etl()