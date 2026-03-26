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