#!/bin/bash
set -e

echo "Starting Kin Orchestration Environment..."
mkdir -p logs

echo "1. Starting infrastructure (Postgres, Redis, Temporal)..."
cd infra/docker && docker compose up -d && cd ../..

echo "2. Waiting for Temporal to be ready on port 7233..."
while ! nc -z localhost 7233; do   
  sleep 1
done

echo "3. Starting backend services..."
uv run python -u -m kin.orchestrator.executor.worker > logs/worker.log 2>&1 &
echo $! > logs/worker.pid

uv run python -u -m kin.agents.researcher.main > logs/researcher.log 2>&1 &
echo $! > logs/researcher.pid

uv run python -u -m kin.agents.writer.main > logs/writer.log 2>&1 &
echo $! > logs/writer.pid

uv run uvicorn kin.gateway.main:app --host 0.0.0.0 --port 8000 > logs/gateway.log 2>&1 &
echo $! > logs/gateway.pid

echo "4. Waiting for Gateway API to be healthy..."
while ! curl -s http://localhost:8000/docs > /dev/null; do
  sleep 1
done

echo "========================================="
echo "✓ All Kin services started successfully!"
echo "API Gateway running at: http://localhost:8000"
echo "Temporal UI running at: http://localhost:8233"
echo "Logs are available in the ./logs/ directory."
echo "To stop everything, run: ./stop.sh"
echo "========================================="
