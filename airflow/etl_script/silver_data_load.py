import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, explode

def build_spark_session():
    spark = SparkSession.builder \
    .appName("IMDb Lakehouse Pipeline silver Load") \
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
    
    return spark

def create_database(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS imdb_olap_silver;")
    spark.sql("USE imdb_olap_silver;")


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
    BRONZE_DB = 'imdb_olap_bronze'
    OUTPUT_DIR = "s3a://imdb-bucket/silver"

    title_basics_df = spark.read.table(f"{BRONZE_DB}.title_basics")
    title_basics_df = title_basics_df.select(
                "tconst", "titleType", "primaryTitle", "originalTitle",
                col("isAdult").cast("int"),
                col("startYear").cast("int"),
                col("endYear").cast("int"),
                col("runtimeMinutes"),
                split(col("genres"), ",").alias("genres")
            )
    save_table(title_basics_df,'title_basics',f"{OUTPUT_DIR}/title_basics")


    title_ratings_df = spark.read.table(f"{BRONZE_DB}.title_ratings")
    title_ratings_df = (
        title_ratings_df.select(
            "tconst",
            col("averageRating").cast("float"),
            col("numVotes").cast("int")
        )
    )
    save_table(title_ratings_df,'title_ratings',f"{OUTPUT_DIR}/title_ratings")


    name_basics_df = spark.read.table(f"{BRONZE_DB}.name_basics")
    name_basics_df = (
        name_basics_df.select(
            "nconst",
            "primaryName",
            col("birthYear").cast("int"),
            col("deathYear").cast("int"),
            split(col("primaryProfession"), ",").alias("primaryProfession"),
            split(col("knownForTitles"), ",").alias("knownForTitles")
        )
    )
    save_table(name_basics_df,'name_basics',f"{OUTPUT_DIR}/name_basics")


    title_principals_df = spark.read.table(f"{BRONZE_DB}.title_principals")
    title_principals_df = (
        title_principals_df.select(
            "tconst",
            col("ordering").cast("int"),
            "nconst",
            "category",
            "job",
            "characters"
        )
    )
    save_table(title_principals_df,'title_principals',f"{OUTPUT_DIR}/title_principals")
    
    title_akas_df = spark.read.table(f"{BRONZE_DB}.title_akas")
    title_akas_df = (
        title_akas_df
        .select(
            col("titleId"),
            col("ordering").cast("int"),
            col("title"),
            col("region"),
            col("language"),
            split(col("types"), ",").alias("types"),
            split(col("attributes"), ",").alias("attributes"),
            col("isOriginalTitle").cast("int")
        )
    )
    save_table(title_akas_df,'title_akas',f"{OUTPUT_DIR}/title_akas")

    print("Silver Pipeline Execution Completed Successfully.")
    spark.stop()

if __name__ == "__main__":
    main()