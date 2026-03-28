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

# OpenAlex Testing (use arXiv ID's already in the HDFS database since OpenAlex is a bibliographic database)
python -m ingestion.Openalex --ids 2603.24594 2603.24587 2603.24580 2603.24567 2603.24562 --category cs.LG