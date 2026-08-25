import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, explode
from pyspark.sql import Window
from pyspark.sql.functions import dense_rank,row_number,monotonically_increasing_id
from pyspark.sql.functions import broadcast

def build_spark_session():
    spark = SparkSession.builder \
    .appName("IMDb Lakehouse Pipeline ClickHouse Load") \
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
    .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog") \
    .config("spark.sql.catalog.clickhouse.host", "clickhouse-olap") \
    .config("spark.sql.catalog.clickhouse.http_port", "8123") \
    .config("spark.sql.catalog.clickhouse.user", "clickhouse_admin") \
    .config("spark.sql.catalog.clickhouse.password", "clickhouse_password") \
    .config("spark.sql.catalog.clickhouse.database", "imdb_olap")\
    .config("spark.clickhouse.write.format", "json")\
    .getOrCreate()
    
    return spark

def main():
    spark = build_spark_session()
    spark.sql("USE imdb_olap_gold")
    title_principals = spark.sql("SELECT * FROM title_principals")
    dim_genres = spark.sql("SELECT * FROM dim_genres")
    dim_peoples = spark.sql("SELECT * FROM dim_peoples")
    dim_professions = spark.sql("SELECT * FROM dim_professions")
    dim_titles = spark.sql("SELECT * FROM dim_titles")
    fact_people_related = spark.sql("SELECT * FROM fact_people_related")
    fact_title_related = spark.sql("SELECT * FROM fact_title_related")
    fact_final= spark.sql("SELECT * FROM fact_final")

    # dime generes load
    dim_genres.printSchema()
    spark.sql(""" 
    CREATE OR REPLACE TABLE clickhouse.imdb_olap.dim_genres(
        genre String,
        genre_id INT
    )USING ClickHouse
    TBLPROPERTIES (
    engine = 'MergeTree()',
    order_by = 'genre_id',
    'settings.allow_nullable_key' = '1',
    'settings.index_granularity' = '8192'
    );
    """)
    dim_genres.writeTo("clickhouse.imdb_olap.dim_genres").append()

    # dim people load
    dim_peoples.printSchema()
    spark.sql(""" 
    CREATE OR REPLACE TABLE clickhouse.imdb_olap.dim_peoples(
        nconst String,
        primaryName String,
        birthYear INT,
        deathYear INT
    )USING ClickHouse
    TBLPROPERTIES (
    engine = 'MergeTree()',
    order_by = 'birthYear',
    'settings.allow_nullable_key' = '1',
    'settings.index_granularity' = '8192'
    );
    """)
    dim_peoples.writeTo("clickhouse.imdb_olap.dim_peoples").append()

    #dim profession load
    dim_professions.printSchema()
    spark.sql(""" 
    CREATE OR REPLACE TABLE clickhouse.imdb_olap.dim_professions(
        profession String,
        profession_id INT
    )USING ClickHouse
    TBLPROPERTIES (
    engine = 'MergeTree()',
    order_by = 'profession_id',
    'settings.allow_nullable_key' = '1',
    'settings.index_granularity' = '8192'
    );
    """)
    dim_professions.writeTo("clickhouse.imdb_olap.dim_professions").append()

    # dim titles load
    dim_titles.printSchema()
    spark.sql(""" 
    CREATE OR REPLACE TABLE clickhouse.imdb_olap.dim_titles(
        tconst String,
        titleType String,
        originalTitle String,
        isAdult INT,
        startYear INT,
        endYear INT,
        runtimeMinutes String
    )USING ClickHouse
    TBLPROPERTIES (
    engine = 'MergeTree()',
    PARTITION = 'titleType,startYear',
    order_by = 'titleType,startYear',
    'settings.allow_nullable_key' = '1',
    'settings.index_granularity' = '8192'
    );
    """)
    dim_titles.writeTo("clickhouse.imdb_olap.dim_titles").append()

    # fact final load
    fact_final.printSchema()
    spark.sql(""" 
    CREATE OR REPLACE TABLE clickhouse.imdb_olap.fact_final(
        tconst String,
        nconst String,
        profession_id Int,
        genre_id Int,
        averageRating Float,
        numVotes Int,
        primaryTitle String,
        originalTitle String,
        isAdult Int,
        endYear Int,
        runtimeMinutes Int,
        titleType String,
        startYear Int
    )USING ClickHouse
    TBLPROPERTIES (
    engine = 'MergeTree()',
    PARTITION = 'titleType,startYear',
    order_by = 'titleType,startYear',
    'settings.allow_nullable_key' = '1',
    'settings.index_granularity' = '8192'
    );
    """)
    fact_final.writeTo("clickhouse.imdb_olap.fact_final").append()

    print("ClickHouse LOAD Pipeline Execution Completed Successfully.")
    spark.stop()

if __name__ == "__main__":
    main()