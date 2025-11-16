# 🚀 SeismoSphere - ML Backend API

SeismoSphere is the primary backend service that handles the entire process of earthquake data capture, preprocessing (ETL), spatial analysis, and providing machine learning modeling results for the SeismicSphere application. This backend is designed as the system's foundation for managing and executing earthquake analytics pipelines in an automated, structured, and integrated manner with other components.

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
   docker-compose up -d --build
   ```

   **📊 Database akan otomatis dibuat:**

   - `airflow_metadata` - Airflow internal database (WAJIB!)
   - `seismo_sphere` - ML results database

4. **Tunggu ~30 detik untuk initialization**

5. **Verifikasi services running:**

   ```bash
   docker-compose ps
   ```

   Expected:

   - ✅ `seismo_postgres` - Up (healthy)
   - ✅ `seismo_airflow` - Up (port 8080)

6. **Access services:**
   - Airflow UI: http://localhost:8080
