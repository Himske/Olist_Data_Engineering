Perfect. Let's use **Docker Compose for the whole local environment** so the project is reproducible and easy to demonstrate in your portfolio.

I'd use this architecture:

**Airflow**

Scheduler · Webserver · DAGs

**Spark**

PySpark ingestion jobs

**Azurite**

Azure Blob Storage emulator · Bronze container

**dbt**

Silver and Gold transformations

I'd keep Airflow and Spark in separate containers initially. That makes the responsibilities clear and avoids installing a full Spark environment inside the Airflow image.

### **Proposed project structure**

```
olist-data-platform/
├── dags/
│   └── olist_ingestion_dag.py
├── spark/
│   └── olist_ingest.py
├── dbt/
│   └── olist_warehouse/
├── data/
│   └── olist/
├── config/
│   └── .env
├── docker/
│   ├── airflow/
│   │   └── Dockerfile
│   └── spark/
│       └── Dockerfile
├── docker-compose.yml
└── README.md
```

### **Docker Compose services**

I'd start with these five services:

| Service             | Responsibility                |
| ------------------- | ----------------------------- |
| `airflow-webserver` | Airflow UI                    |
| `airflow-scheduler` | Schedules and executes DAGs   |
| `spark-master`      | Accepts Spark job submissions |
| `spark-worker`      | Executes Spark jobs           |
| `azurite`           | Emulates Azure Blob Storage   |

We'll configure Airflow to submit jobs to Spark using `spark-submit`, and Spark will write Parquet files to Azurite using the Hadoop Azure connector.

One design decision: **keep dbt as a separate step initially**. First get the Bronze ingestion pipeline working, then add dbt and a suitable analytical warehouse in the next phase.

### **Suggested implementation order**

1. Create the Dockerfiles and `docker-compose.yml`.
2. Start Airflow, Spark and Azurite.
3. Create the `bronze` Blob container.
4. Configure Spark's Azure Storage connection.
5. Adapt `olist_ingest.py` from MinIO/S3A to Azure Blob.
6. Adapt the Airflow DAG and test one dataset (`orders`).
7. Run all nine ingestion tasks.

**Next step:** I can build the complete Docker Compose setup and updated project files, replacing the MinIO configuration in your existing starter project. I'll include the environment file, Dockerfiles, Spark configuration and Airflow DAG.