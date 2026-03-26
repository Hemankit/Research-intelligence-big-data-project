#!/usr/bin/env bash
# setup.sh
# ──────────────────────────────────────────────────────────────────────────────
# One-command setup for the Research Intelligence Pipeline stack.
# Run this once after cloning the repo.
#
# Usage:
#   chmod +x setup.sh
#   ./setup.sh
# ──────────────────────────────────────────────────────────────────────────────

set -e  # exit immediately on any error

echo ""
echo "══════════════════════════════════════════════════════"
echo "  Research Intelligence Pipeline — Stack Setup"
echo "══════════════════════════════════════════════════════"
echo ""

#1. Check prerequisites
echo "▶ Checking prerequisites..."

if ! command -v docker &> /dev/null; then
  echo "  ✗ Docker not found. Install from https://docs.docker.com/get-docker/"
  exit 1
fi
echo "  ✓ Docker: $(docker --version)"

if ! command -v docker compose &> /dev/null; then
  echo "  ✗ Docker Compose not found. Make sure Docker Desktop is up to date."
  exit 1
fi
echo "  ✓ Docker Compose: $(docker compose version)"

if ! command -v python3 &> /dev/null; then
  echo "  ✗ Python 3 not found."
  exit 1
fi
echo "  ✓ Python: $(python3 --version)"

#2. Create .env if it doesn't exist
echo ""
echo "▶ Checking .env..."
if [ ! -f .env ]; then
  echo "  .env not found — creating from template..."
  cp .env.example .env 2>/dev/null || echo "  (no .env.example found, using existing .env)"
else
  echo "  ✓ .env already exists"
fi

#3. Create dags directory (Airflow needs it at startup)
echo ""
echo "▶ Creating local directories..."
mkdir -p dags
echo "  ✓ dags/"

#4. Install Python dependencies
echo ""
echo "▶ Installing Python dependencies..."
pip install -r requirements.txt --quiet
echo "  ✓ requirements.txt installed"

#5. Pull Docker images
echo ""
echo "▶ Pulling Docker images (this may take a few minutes on first run)..."
docker compose pull

#6. Start the stack
echo ""
echo "▶ Starting the full stack..."
docker compose up -d

#7. Wait for HDFS to be ready
echo ""
echo "▶ Waiting for HDFS NameNode to be ready..."
RETRIES=20
until curl -sf http://localhost:9870/ > /dev/null 2>&1 || [ $RETRIES -eq 0 ]; do
  echo "  ...waiting ($RETRIES retries left)"
  sleep 5
  RETRIES=$((RETRIES - 1))
done

if [ $RETRIES -eq 0 ]; then
  echo "  ✗ HDFS did not become ready in time. Check logs: docker compose logs namenode"
  exit 1
fi
echo "  ✓ HDFS NameNode is up"

#8. Wait for Elasticsearch
echo ""
echo "▶ Waiting for Elasticsearch to be ready..."
RETRIES=20
until curl -sf http://localhost:9200/_cluster/health > /dev/null 2>&1 || [ $RETRIES -eq 0 ]; do
  echo "  ...waiting ($RETRIES retries left)"
  sleep 5
  RETRIES=$((RETRIES - 1))
done

if [ $RETRIES -eq 0 ]; then
  echo "  ✗ Elasticsearch did not become ready in time. Check logs: docker compose logs elasticsearch"
else
  echo "  ✓ Elasticsearch is up"
fi

#9. Done
echo ""
echo "══════════════════════════════════════════════════════"
echo "  Stack is running! Access points:"
echo ""
echo "  HDFS NameNode Web UI  →  http://localhost:9870"
echo "  Spark Master Web UI   →  http://localhost:8080"
echo "  Airflow Web UI        →  http://localhost:8085"
echo "                            user: admin / pass: admin"
echo "  Elasticsearch         →  http://localhost:9200"
echo "  HiveServer2 (JDBC)    →  localhost:10000"
echo ""
echo "  To stop the stack:    docker compose down"
echo "  To wipe all data:     docker compose down -v"
echo "  To view logs:         docker compose logs -f <service>"
echo "══════════════════════════════════════════════════════"
echo ""
