# 🚀 SeismoSphere API - ML Backend Logic

SeismoSphere API is the primary backend service that handles the entire process of earthquake data capture, preprocessing (ETL), spatial analysis, and providing machine learning modeling results for the SeismicSphere application. This backend is designed as the system's foundation for managing and executing earthquake analytics pipelines in an automated, structured, and integrated manner with other components.

## 📋 **Tech Stack**

- **Orchestration**: Apache Airflow 2.9.3 (LocalExecutor)
- **Database**: PostgreSQL 17 + PostGIS 3.5 (Spatial data support)
- **Data Processing**: Polars LTS CPU 1.33+ (High-performance DataFrames)
- **Storage**: Parquet (Snappy compression)
- **Container**: Docker + Docker Compose 3.9

## 📂 **Project Structure**

```
seismosphere-api/
├── airflow/
│   ├── dags/
│   │   └── master_dag.py              # Main pipeline DAG
│   ├── operators/
│   │   ├── polars_ingestion_operator.py    # Raw data ingestion
│   │   └── polars_preprocessing_operator.py # Data cleaning & loading
│   ├── Dockerfile                     # Airflow custom image
│   ├── init-airflow.sh               # Initialization script
│   └── requirements.txt              # Python dependencies
├── data/bigdata/                     # Data storage
│   ├── raw_earthquakes.parquet      # Raw ingested data
│   └── processed_earthquakes.parquet # Cleaned data
├── docker-compose.yml                # Container orchestration
├── .env                             # Environment variables
└── README.md                        # This file
```

## 🚀 **Quick Start**

1. **Clone Repository**

   ```bash
   git clone https://github.com/SeismoSphere/seismosphere-api.git
   cd seismosphere-api
   ```

2. **Copy Environment File**

   ```bash
   cp .env.example .env
   ```

3. **Start Docker Containers**

   ```bash
   docker-compose up --build -d
   ```

   **📊 Database akan otomatis dibuat:**

   - `airflow_metadata` - Airflow internal database
   - `seismo_sphere` - Application database with PostGIS

4. **Tunggu ~60 detik untuk initialization**

   Proses yang berjalan:

   - PostgreSQL initialization
   - PostGIS extensions installation
   - Airflow database migration
   - Admin user creation
   - Scheduler & Webserver startup

5. **Verifikasi services running:**

   ```bash
   docker-compose ps
   ```

   Expected:

   - ✅ `seismo_postgres` - Up (healthy)
   - ✅ `seismo_airflow` - Up (port 8080)

6. **Access services:**

   - Airflow UI: http://localhost:8080
   - Default credentials: `admin` / `admin`

## 📊 **Data Pipeline**

### Pipeline Workflow

```
USGS API → Raw Ingestion (2 years) → Preprocessing → PostgreSQL + Parquet
```

### DAG: `master_earthquake_pipeline`

1. **Task 1: `ingest_raw_data`**

   - Fetches 2 years of earthquake data from USGS
   - Region: Asia (Lat: -10 to 55, Lon: 60 to 150)
   - Batch size: 30 days per request
   - Output: `data/bigdata/raw_earthquakes.parquet`

2. **Task 2: `preprocess_and_load`**
   - Cleaning: Removes records with null/None place
   - Reduction: Deduplicates by ID, sorts by datetime
   - Output: `data/bigdata/processed_earthquakes.parquet`
   - PostgreSQL: Inserts to `earthquakes` table with PostGIS geometry

### Schedule

- **Cron**: `0 5 1 1 *` (Yearly on Jan 1 at 5 AM UTC)
- **Manual**: Trigger via Airflow UI

## 🔧 **Development Commands**

### Docker Management

```bash
# Start services
docker-compose up --build -d

# View logs
docker logs seismo_airflow --tail 50
docker logs seismo_postgres --tail 50

# Stop services
docker-compose down

# Clean restart (removes volumes)
docker-compose down -v
docker-compose up --build -d
```
