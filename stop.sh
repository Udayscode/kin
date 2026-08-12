#!/bin/bash

echo "Stopping Kin Orchestration Environment..."

if [ -d "logs" ]; then
    echo "1. Stopping python services..."
    for pid_file in logs/*.pid; do
        if [ -f "$pid_file" ]; then
            PID=$(cat "$pid_file")
            echo "Killing process $PID ($pid_file)"
            kill $PID 2>/dev/null || true
            rm "$pid_file"
        fi
    done
fi

# Fallback kill via pkill just in case
pkill -f "kin.orchestrator.executor.worker" || true
pkill -f "kin.agents.researcher.main" || true
pkill -f "kin.agents.writer.main" || true
pkill -f "kin.gateway.main" || true

echo "2. Stopping infrastructure..."
cd infra/docker && docker compose down && cd ../..

echo "✓ Kin stopped gracefully."
