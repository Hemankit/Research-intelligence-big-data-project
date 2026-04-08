# Research-intelligence-big-data-project
This project is designed to extract insights from research sources at scale and perform analytics to be displayed in an interactive dashboard

# Initialize Docker + HDFS environment for data warehouse infrastructure building (run this command in GitBash in the project folder)
chmod +x setup.sh

# Run setup.sh to activae HDFS data warehouse whilst Docker Desktop Environment is running (only run once)
./setup.sh

# Start the stack (after the first setup)
docker compose up -d

# Stop the stack when done for the day (only add -v if you want to remove all data from server)
docker compose down
docker compose down -v

# View localhost ports using this command
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

# ArXiv commands for small incremental ingetions
python -m ingestion.arxiv
python -m ingestion.arxiv --lookback 7

# ArXiv commands for bulk ingestions (may result in 429s from server side)
python -m ingestion.arxiv --bulk --max 1000 --batch-size 200

# S2orc Bulk Ingestion (without full text)
python -m ingestion.S2orc --ingest --query "2023-01-01:2024-12-31" --category s2orc_bulk --batch-size 1000 --max 10000

# s2orc full text (be careful with how many shards you are using)
python -m ingestion.s2orc_bulk_download --shards 1 --local-dir ./s2orc_shards --ingest --max-per-shard 100

# OpenAlex Testing (use arXiv ID's already in the HDFS database since OpenAlex is a bibliographic database)
python -m ingestion.Openalex --ids 2603.24594 2603.24587 2603.24580 2603.24567 2603.24562 --category cs.LG

# Spark Consolidator
MSYS_NO_PATHCONV=1 docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/pipelines/spark_consolidate.py

# Spark PageRank
MSYS_NO_PATHCONV=1 docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/pipelines/spark_pagerank.py

# Spark Trend
MSYS_NO_PATHCONV=1 docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/pipelines/spark_trends.py