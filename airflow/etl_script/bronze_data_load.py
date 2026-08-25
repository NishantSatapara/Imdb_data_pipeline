import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, explode

def build_spark_session():
    spark = SparkSession.builder \
    .appName("IMDb Lakehouse Pipeline Bronze Load") \
    .config("spark.sql.warehouse.dir", "s3a://imdb-bucket/hive_catalog/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "lakehouse_admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "lakehouse_password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")\
    .config("spark.sql.parquet.compression.codec", "snappy")\
    .enableHiveSupport()\
    .getOrCreate()
    return spark

def create_database(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS imdb_olap_bronze;")
    spark.sql("USE imdb_olap_bronze;")
    
def read_csv(spark, file_path, sep="\t", header=True, infer_schema=False):
    return (
        spark.read
        .option("sep", sep)
        .option("header", header)
        .option("inferSchema", infer_schema)
        .csv(file_path)
        .na.replace("\\N", None)
    )

def save_table(df,table_name,path=None,mode="overwrite",fmt="parquet",partition_cols=None):
    writer = (df.write.format(fmt).mode(mode))
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    if path:
        writer = writer.option("path", path)
    writer.saveAsTable(table_name)

def main():

    print("Bronze Pipeline Execution Start Successfully.")

    spark = build_spark_session()
    create_database(spark)
    
    INPUT_DIR = "s3a://imdb-bucket/raw"
    OUTPUT_DIR = "s3a://imdb-bucket/bronze"
  
    title_basics_df = read_csv(spark,f"{INPUT_DIR}/title.basics.tsv")
    save_table(title_basics_df,'title_basics',f"{OUTPUT_DIR}/title_basics")

    title_ratings_df = read_csv(spark,f"{INPUT_DIR}/title.ratings.tsv")
    save_table(title_ratings_df,'title_ratings',f"{OUTPUT_DIR}/title_ratings")

    name_basics_df = read_csv(spark,f"{INPUT_DIR}/name.basics.tsv")
    save_table(name_basics_df,'name_basics',f"{OUTPUT_DIR}/name_basics")

    title_principals_df = read_csv(spark,f"{INPUT_DIR}/title.principals.tsv")
    save_table(title_principals_df,'title_principals',f"{OUTPUT_DIR}/title_principals")
    
    title_akas_df = read_csv(spark,f"{INPUT_DIR}/title.akas.tsv")
    save_table(title_akas_df,'title_akas',f"{OUTPUT_DIR}/title_akas")

    print("Bronze Pipeline Execution Completed Successfully.")
    spark.stop()

if __name__ == "__main__":
    main()