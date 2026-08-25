IMDb End-to-End Data Engineering Pipeline

This project implements an end-to-end IMDb data engineering and analytics pipeline using Apache Airflow, Apache Spark, Docker, MinIO, Hive Metastore, PostgreSQL, and ClickHouse. The objective is to build a local data platform that can ingest the IMDb dataset, process and transform the raw data at scale using PySpark, store the processed datasets as Parquet in an S3-compatible data lake, and finally load business-ready datasets into ClickHouse for fast analytical queries.

Architecture Overview : 
The pipeline follows a Lakehouse + OLAP architecture. Apache Airflow is responsible for orchestrating the complete workflow, while Apache Spark performs the distributed data processing. MinIO acts as the S3-compatible object storage layer and provides the data lake where raw and transformed IMDb data is stored. Hive Metastore maintains metadata about Spark tables and their underlying storage locations, with PostgreSQL used as the metadata database. After the Spark transformations are completed, the curated Gold datasets are loaded into ClickHouse, which acts as the high-performance OLAP serving layer for analytical queries.

                    ┌────────────────────┐
                    │       Kaggle       │
                    │   IMDb Dataset     │
                    └─────────┬──────────┘
                              │
                              ▼
                    ┌────────────────────┐
                    │      Airflow       │
                    │    Orchestrator    │
                    └─────────┬──────────┘
                              │
                       spark-submit
                              │
                              ▼
                 ┌─────────────────────────┐
                 │      Spark Cluster      │
                 │                         │
                 │  Spark Master           │
                 │       │                 │
                 │   ┌───┴────┐            │
                 │   ▼        ▼            │
                 │ Worker 1  Worker 2      │
                 └───────────┬─────────────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │        MinIO         │
                  │    Data Lake / S3    │
                  │                      │
                  │ Bronze → Silver → Gold│
                  └──────────┬───────────┘
                             │
                  ┌──────────┴───────────┐
                  │                      │
                  ▼                      ▼
        ┌──────────────────┐    ┌──────────────────┐
        │  Hive Metastore  │    │    ClickHouse    │
        │                  │    │                  │
        │ Metadata Catalog │    │   OLAP Engine    │
        └────────┬─────────┘    └────────┬─────────┘
                 │                       │
                 ▼                       ▼
            PostgreSQL             Analytics Queries


Data Ingestion

The pipeline starts with the IMDb dataset obtained from Kaggle. The dataset contains multiple TSV files representing different aspects of IMDb's data model, including titles, ratings, episodes, alternate titles, principals, people, crew, genres, and related information. Airflow orchestrates the ingestion process and triggers the Spark application responsible for processing the source files.

The raw IMDb files are initially downloaded and placed into the ingestion area. Instead of treating the original TSV files as the final analytical format, the pipeline uses Spark to validate the source structure, apply an explicit schema, handle IMDb's \N representation for missing values, and prepare the data for downstream processing.

Apache Airflow – Workflow Orchestration

Apache Airflow acts as the orchestration layer of the platform. The Airflow DAG defines the order and dependency of the different pipeline stages and provides centralized scheduling, monitoring, logging, retries, and failure handling.

Airflow does not perform the heavy data transformation itself. Instead, it submits PySpark applications to the Spark cluster using spark-submit. This separation allows Airflow to focus on workflow orchestration while Spark is responsible for distributed computation.

Run Analytics
Apache Spark – Distributed Data Processing


Apache Spark is the primary processing engine used to transform the IMDb dataset. The Spark cluster consists of a Spark Master and one or more Spark Workers running as Docker containers.

The Spark Master is responsible for coordinating applications and allocating resources, while the workers execute the actual Spark tasks. PySpark is used to write the transformation logic, allowing the pipeline to process the IMDb data using Spark DataFrames and SQL.

The processing layer performs operations such as schema enforcement, data type conversion, null handling, filtering invalid records, deduplication, joins between IMDb datasets, derived column creation, aggregation, and business-rule implementation.

For example, IMDb ratings can be combined with title information to produce an analytical dataset containing attributes such as title, title type, release year, runtime, genres, average rating, and number of votes.



MinIO – Data Lake

MinIO provides the project's S3-compatible object storage layer. It acts as the local Data Lake and allows the Spark cluster to work with data using S3-style paths such as:

s3a://imdb-bucket/...
The lake is organized into logical processing layers:

lake/
├── bronze/
├── silver/
└── gold/

The Bronze layer represents the raw or minimally processed IMDb data. The Silver layer contains cleaned and standardized datasets. The Gold layer contains business-ready datasets optimized for downstream analytics and OLAP ingestion.
The processed datasets are stored in Parquet format with Snappy compression, providing columnar storage, efficient compression, predicate pushdown, and column pruning for analytical workloads.



Bronze Layer

The Bronze layer preserves the source-oriented structure of the IMDb dataset. Individual IMDb datasets are stored separately so that the original relationships between titles, ratings, episodes, people, and other entities can be maintained.

Typical Bronze datasets include:

title_basics
title_ratings
title_episodes
title_akas
title_principals
name_basics
title_crew

This layer provides a reproducible starting point for subsequent transformations and prevents downstream processing from being tightly coupled to the original Kaggle files.



Silver Layer

The Silver layer contains cleaned and standardized IMDb datasets produced by PySpark. At this stage, raw string values are converted into appropriate data types, invalid records are removed or handled, missing values are standardized, and duplicate records are addressed.
Relationships between datasets are also established where required. For example, title metadata can be joined with ratings, episode information, principals, and people information to create a consistent analytical foundation.
The Silver layer is designed to contain reusable datasets rather than highly application-specific business logic.




Gold Layer

The Gold layer contains the final business-oriented datasets used for analytics. Instead of exposing the raw IMDb structure directly to analytical consumers, the pipeline creates dimensions and fact-style datasets.
The project includes datasets such as:

dim_genres
dim_titles
dim_peoples
dim_professions
fact_title_related
fact_people_related
fact_final

The Gold transformation combines the cleaned IMDb datasets and applies the final business logic required for analytical use cases.
This makes the resulting data easier for analysts to query than the original IMDb normalized source files.





Hive Metastore – Metadata Management

Hive Metastore is used as the metadata catalog for the Spark tables. It does not store the actual Parquet data. Instead, it maintains information about tables, columns, data types, partitions, table properties, and physical storage locations.
PostgreSQL is used as the persistent backend database for the Hive Metastore.

The separation is therefore:

                    Hive Metastore
                          │
                          │ metadata
                          ▼
                    PostgreSQL

This allows Spark to discover and manage tables while the actual datasets remain in the object-storage layer.



PostgreSQL
PostgreSQL is used for metadata persistence within the platform. The project uses PostgreSQL for the supporting databases required by Airflow and Hive Metastore.
It is important to distinguish PostgreSQL's role from ClickHouse's role. PostgreSQL is primarily used as a metadata and application database in this architecture, while ClickHouse is used as the analytical OLAP engine.


ClickHouse – OLAP Serving Layer

ClickHouse is the final analytical serving layer of the pipeline. Once Spark has completed the transformations and produced the Gold datasets, the business-ready data is loaded into ClickHouse.
The ClickHouse database contains analytical tables such as:

imdb_olap
├── dim_genres
├── dim_titles
├── dim_peoples
├── dim_professions
├── fact_title_related
├── fact_people_related
└── fact_final

ClickHouse is used because the final workload is primarily analytical, involving filtering, aggregation, grouping, ranking, and large-scale scans.
