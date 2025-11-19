#!/bin/bash
set -e

if [ -f /tmp/airflow_init_done ]; then
    echo "Initialization already completed, starting services directly..."
    exec airflow scheduler
fi

echo "Initializing Airflow..."

echo "Waiting for PostgreSQL..."
max_attempts=15
attempt=0
until PGPASSWORD=$POSTGRES_PASSWORD psql -h postgres -U "$POSTGRES_USER" -p 5432 -d postgres -c '\q' >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ $attempt -ge $max_attempts ]; then
        echo "PostgreSQL not available"
        exit 1
    fi
    sleep 1
done
echo "PostgreSQL ready"

echo "Checking database..."
DB_EXISTS=$(PGPASSWORD=$POSTGRES_PASSWORD psql -h postgres -U "$POSTGRES_USER" -p 5432 -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='airflow_metadata'" 2>/dev/null)

if [ -z "$DB_EXISTS" ]; then
    echo "Creating database..."
    PGPASSWORD=$POSTGRES_PASSWORD psql -h postgres -U "$POSTGRES_USER" -p 5432 -d postgres -c "CREATE DATABASE airflow_metadata;" >/dev/null 2>&1
    echo "Database created"
else
    echo "Database exists"
fi

echo "Enabling PostGIS..."
PGPASSWORD=$POSTGRES_PASSWORD psql -h postgres -U "$POSTGRES_USER" -p 5432 -d seismo_sphere \
    -c "CREATE EXTENSION IF NOT EXISTS postgis; CREATE EXTENSION IF NOT EXISTS postgis_topology;" >/dev/null 2>&1 || true

echo "Running migration..."
airflow db migrate 2>&1 | grep -E "(Upgrade|upgrade|ERROR|error|complete)" | head -20 || true

echo "Creating admin user..."
airflow users create \
    --username ${AIRFLOW_USERNAME:-admin} \
    --password ${AIRFLOW_PASSWORD:-admin} \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@seismosphere.com 2>&1 | grep -E "User|ERROR" || echo "User ready"

touch /tmp/airflow_init_done

echo ""
echo "Initialization complete!"
echo "http://localhost:8080"
echo "${AIRFLOW_USERNAME:-admin} / ${AIRFLOW_PASSWORD:-admin}"
echo ""

echo "Starting webserver in background..."
nohup airflow webserver >/dev/null 2>&1 &

echo "Starting scheduler..."
exec airflow scheduler
