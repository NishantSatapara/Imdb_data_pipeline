import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, explode

def build_spark_session():
    spark = SparkSession.builder \
    .appName("IMDb Lakehouse Pipeline") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "4") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.parquet.compression.codec", "snappy")\
    .getOrCreate()
    
    return spark

def create_database(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS imdb_olap_bronze;")
    spark.sql("CREATE DATABASE IF NOT EXISTS imdb_olap_silver;")
    spark.sql("CREATE DATABASE IF NOT EXISTS imdb_olap_gold;")

    
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

    spark = build_spark_session()
    create_database(spark)
    RAW_DIR = "/opt/spark/data/raw"
    BRONZE_DIR = "/opt/spark/data/bronze"
    SILVER_DIR = "/opt/spark/data/silver"
    GOLD_DIR = "/opt/spark/data/gold"
    INPUT_DIR = RAW_DIR
    OUTPUT_DIR = BRONZE_DIR

    # 1. Load Datasets & Clean IMDb '\N' null indicators
    title_basics_df = read_csv(spark,f"{raw_dir}/title.basics.tsv")
    title_basics_df = title_basics_df.select(
            "tconst", "titleType", "primaryTitle", "originalTitle",
            col("isAdult").cast("int"),
            col("startYear").cast("int"),
            col("endYear").cast("int"),
            col("runtimeMinutes").cast("int"),
            split(col("genres"), ",").alias("genres")
        )
    write_parquet(title_basics_df,f"{output_dir}/title_basics")



    title_ratings_df = read_csv(spark,f"{raw_dir}/title.ratings.tsv")
    title_ratings_df = title_ratings_df.select(
            "tconst",
            col("averageRating").cast("float"),
            col("numVotes").cast("int")
        )
    write_parquet(title_ratings_df,f"{output_dir}/title_ratings")


    name_basics_df = read_csv(spark,f"{raw_dir}/name.basics.tsv")
    name_basics_df = name_basics_df.select(
            "nconst",
            "primaryName",
            col("birthYear").cast("int"),
            col("deathYear").cast("int"),
            split(col("primaryProfession"), ",").alias("primaryProfession"),
            split(col("knownForTitles"), ",").alias("knownForTitles")
        )
    write_parquet(name_basics_df,f"{output_dir}/name_basics")


    title_principals_df = read_csv(spark,f"{raw_dir}/title.principals.tsv")
    title_principals_df = title_principals_df.select(
            "tconst",
            col("ordering").cast("int"),
            "nconst",
            "category",
            "job",
            "characters"
        )
    write_parquet(title_principals_df,f"{output_dir}/title_principals")
    

    title_akas_df = read_csv(spark,f"{raw_dir}/title.akas.tsv")
    title_akas_df = title_akas_df.select(
            col("titleId"),
            col("ordering").cast("int"),
            col("title"),
            col("region"),
            col("language"),
            split(col("types"), ",").alias("types"),
            split(col("attributes"), ",").alias("attributes"),
            col("isOriginalTitle").cast("int")
        )
    write_parquet(title_akas_df,f"{output_dir}/title_akas")

    # 2. Join Titles and Ratings
    transformed_df = basics_df.join(ratings_df, on="tconst", how="left") \
        .join(episodes_df, col("tconst") == col("episode_tconst"), how="left") \
        .drop("episode_tconst")

    # Filter out missing years for stable partitioning
    clean_df = transformed_df.filter(col("startYear").isNotNull())

    # 3. Write Partitioned Snappy Parquet
    # Strategy: Partition by titleType and startYear to optimize time-series queries
    print("Writing Parquet Lake...")
    write_parquet(clean_df,f"{output_dir}/titles_partitioned",partition_cols=["titleType","startYear"])
    print("Pipeline Execution Completed Successfully.")
    spark.stop()

if __name__ == "__main__":
    main()