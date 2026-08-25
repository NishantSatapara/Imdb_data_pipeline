import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, explode
from pyspark.sql import Window
from pyspark.sql.functions import dense_rank,row_number,monotonically_increasing_id
from pyspark.sql.functions import broadcast

def build_spark_session():
    spark = SparkSession.builder \
    .appName("IMDb Lakehouse Pipeline gold Load") \
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
    spark.sql("CREATE DATABASE IF NOT EXISTS imdb_olap_gold;")
    spark.sql("USE imdb_olap_gold;")

def save_table(df,table_name,path=None,mode="overwrite",fmt="parquet",partition_cols=None):
    writer = (df.write.format(fmt).mode(mode))
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    if path:
        writer = writer.option("path", path)
    writer.saveAsTable(table_name)

def main():

    print("Gold Pipeline Execution Start Successfully.")
    
    spark = build_spark_session()
    SILVER_DB = 'imdb_olap_silver'
    OUTPUT_DIR = "s3a://imdb-bucket/gold"

    # reading silver table 
    title_basics_df = spark.read.table(f"{SILVER_DB}.title_basics")
    name_basics_df = spark.read.table(f"{SILVER_DB}.name_basics")
    title_ratings_df = spark.read.table(f"{SILVER_DB}.title_ratings")

    title_akas_df = spark.read.table(f"{SILVER_DB}.title_akas")
    title_principals_df = spark.read.table(f"{SILVER_DB}.title_principals")

    create_database(spark)

    # No transforamtion for gold layer for this table 
    save_table(title_akas_df,'title_akas',f"{OUTPUT_DIR}/title_akas")

    # No transforamtion for gold layer for this table 
    save_table(title_principals_df,'title_principals',f"{OUTPUT_DIR}/title_principals")

    dim_titles_df = title_basics_df.select(
            "tconst", "titleType", "primaryTitle", "originalTitle",
            col("isAdult"),
            col("startYear"),
            col("endYear"),
            col("runtimeMinutes")
        )
    save_table(dim_titles_df,'dim_titles',f"{OUTPUT_DIR}/dim_titles")

    dim_genres_df = title_basics_df \
    .select(explode("genres").alias("genre")) \
    .filter(col("genre").isNotNull() & (col("genre") != "")) \
    .distinct() \
    .withColumn("genre_id", dense_rank().over(Window.orderBy("genre")))
    save_table(dim_genres_df,'dim_genres',f"{OUTPUT_DIR}/dim_genres")

    dim_people_df = name_basics_df.select(
        "nconst",
        "primaryName",
        col("birthYear"),
        col("deathYear")
    )
    save_table(dim_people_df,'dim_peoples',f"{OUTPUT_DIR}/dim_peoples")

    dim_professions_df = name_basics_df \
    .select(explode("primaryProfession").alias("profession")) \
    .filter(col("profession").isNotNull() & (col("profession") != "")) \
    .distinct() \
    .withColumn("profession_id", dense_rank().over(Window.orderBy("profession")))
    save_table(dim_professions_df,'dim_professions',f"{OUTPUT_DIR}/dim_professions")

    # fact people related
    fact_name_title_explod_df = name_basics_df.select(
            "nconst",
            explode(col("primaryProfession")).alias("people_profession"),
            explode(col("knownForTitles")).alias("people_known_for_title")
        )
    fact_people_related_df = fact_name_title_explod_df.join(dim_professions_df.hint("broadcast"),\
                                    fact_name_title_explod_df['people_profession'] == dim_professions_df['profession'],"left")\
                                    .drop('profession','people_profession')

    save_table(fact_people_related_df,'fact_people_related',f"{OUTPUT_DIR}/fact_people_related")

   # fact title related
    fact_title_explod_df = title_basics_df.select(
            "tconst",
            explode(col("genres")).alias("genre")
        )
    fact_title_generes_df = fact_title_explod_df.join(dim_genres_df.hint("broadcast"),\
                            fact_title_explod_df['genre'] == dim_genres_df['genre'],"left")\
                            .drop('genre')
    fact_title_related_df = fact_title_generes_df.join(title_ratings_df,\
                        fact_title_generes_df['tconst'] == title_ratings_df['tconst'],'left').drop(title_ratings_df['tconst'])
    save_table(fact_title_related_df,'fact_title_related',f"{OUTPUT_DIR}/fact_title_related")

    # fina people and tile fact combine
    fact_people_title_df = fact_people_related_df.join(fact_title_related_df,\
                fact_people_related_df['people_known_for_title'] == fact_title_related_df['tconst'],'right')\
                .drop('people_known_for_title')
    fact_final_df = fact_people_title_df.join(dim_titles_df,'tconst','inner')
    save_table(fact_final_df,'fact_final',f"{OUTPUT_DIR}/fact_final",partition_cols = ['titleType','startYear'])

    #pipeline completion
    print("Gold Pipeline Execution Completed Successfully.")
    spark.stop()

if __name__ == "__main__":
    main()