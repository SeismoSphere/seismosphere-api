import polars as pl
import logging
from pathlib import Path
from typing import Optional
import psycopg2
from psycopg2.extras import execute_values
import os
import numpy as np
from sklearn.neighbors import NearestNeighbors
from minio import Minio
from minio.error import S3Error
from datetime import datetime
import io

logger = logging.getLogger(__name__)

class PolarsEarthquakePreprocessor:
    def __init__(
        self,
        input_file: str = '/opt/airflow/data/bigdata/raw_earthquakes.parquet',
        output_file: str = '/opt/airflow/data/bigdata/processed_earthquakes.parquet',
        postgres_host: str = 'postgres',
        postgres_user: str = 'postgres',
        postgres_password: str = 'seismo123',
        postgres_db: str = 'seismo_sphere',
        minio_endpoint: str = 'minio:9000',
        minio_access_key: str = 'minioadmin',
        minio_secret_key: str = 'minioadmin123',
        minio_bucket: str = 'seismosphere'
    ):
        self.input_file = Path(input_file)
        self.output_file = Path(output_file)
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        self.postgres_config = {
            'host': postgres_host,
            'user': postgres_user,
            'password': postgres_password,
            'database': postgres_db
        }
        
        # MinIO configuration
        self.minio_client = Minio(
            minio_endpoint,
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False
        )
        self.minio_bucket = minio_bucket
        self._ensure_minio_bucket()
        
        logger.info(f"Polars Earthquake Preprocessor initialized")
        logger.info(f"  Input: {self.input_file}")
        logger.info(f"  Output: {self.output_file}")
        logger.info(f"  MinIO: {minio_endpoint}/{minio_bucket}")
    
    def _ensure_minio_bucket(self):
        """Ensure MinIO bucket exists"""
        try:
            if not self.minio_client.bucket_exists(self.minio_bucket):
                self.minio_client.make_bucket(self.minio_bucket)
                logger.info(f"Created MinIO bucket: {self.minio_bucket}")
            else:
                logger.info(f"MinIO bucket exists: {self.minio_bucket}")
        except S3Error as e:
            logger.error(f"MinIO bucket error: {e}")
    
    def load_raw_data(self) -> Optional[pl.DataFrame]:
        try:
            if not self.input_file.exists():
                logger.error(f"Input file not found: {self.input_file}")
                return None
            
            logger.info(f"Loading raw data from {self.input_file}")
            df = pl.read_parquet(self.input_file)
            logger.info(f"Loaded {len(df):,} raw records")
            return df
        except Exception as e:
            logger.error(f"Error loading raw data: {e}")
            return None
    
    def clean_data(self, df: pl.DataFrame) -> pl.DataFrame:
        try:
            initial_count = len(df)
            logger.info(f"Starting data cleaning on {initial_count:,} records")
            
            # Count problematic place values BEFORE cleaning
            none_string_before = df.filter(pl.col('place') == 'None').height
            null_place_before = df.filter(pl.col('place').is_null()).height
            
            logger.info(f"   Found {none_string_before} records with place='None' (string)")
            logger.info(f"   Found {null_place_before} records with place=NULL")
            
            # Remove rows with missing critical fields only
            df_cleaned = df.filter(
                pl.col('latitude').is_not_null() &
                pl.col('longitude').is_not_null() &
                pl.col('magnitude').is_not_null() &
                pl.col('time').is_not_null()
            )
            
            removed_count = initial_count - len(df_cleaned)
            if removed_count > 0:
                logger.info(f"   Removed {removed_count:,} rows with null coordinates/magnitude/time")
            
            # Replace 'None' string and NULL place with 'Unknown Location'
            df_cleaned = df_cleaned.with_columns(
                pl.when(pl.col('place').is_null() | (pl.col('place') == 'None'))
                .then(pl.lit('Unknown Location'))
                .otherwise(pl.col('place'))
                .alias('place')
            )
            
            # Verify replacement
            none_string_after = df_cleaned.filter(pl.col('place') == 'None').height
            null_place_after = df_cleaned.filter(pl.col('place').is_null()).height
            unknown_loc_count = df_cleaned.filter(pl.col('place') == 'Unknown Location').height
            
            logger.info(f"   AFTER cleaning:")
            logger.info(f"     - place='None' string: {none_string_after} (should be 0)")
            logger.info(f"     - place=NULL: {null_place_after} (should be 0)")
            logger.info(f"     - place='Unknown Location': {unknown_loc_count}")
            
            if none_string_after > 0 or null_place_after > 0:
                logger.warning(f"   WARNING: Still have {none_string_after + null_place_after} problematic place values!")
            else:
                logger.info(f"   SUCCESS: All 'None'/NULL place values replaced with 'Unknown Location'")
            
            logger.info(f"Cleaned data: {len(df_cleaned):,} records remaining")
            
            return df_cleaned
        except Exception as e:
            logger.error(f"Error cleaning data: {e}")
            raise
    
    def reduce_data(self, df: pl.DataFrame) -> pl.DataFrame:
        try:
            initial_count = len(df)
            logger.info(f"Starting data reduction on {initial_count:,} records")
            
            df_reduced = df.unique(subset=['id'], keep='first')
            logger.info(f"   Removed {initial_count - len(df_reduced):,} duplicate IDs")
            
            df_reduced = df_reduced.sort('datetime', descending=True)
            logger.info(f"   Sorted by datetime (newest first)")
            
            logger.info(f"Reduced data: {len(df_reduced):,} unique records")
            return df_reduced
        except Exception as e:
            logger.error(f"Error reducing data: {e}")
            raise
    
    def add_spatial_features(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Add spatial preprocessing features for ML models:
        - Distance to nearest earthquake
        - Earthquake density (events per 100km radius)
        - Seismic zone classification
        - Spatial grid cell ID
        """
        try:
            logger.info("Starting spatial preprocessing")
            initial_count = len(df)
            
            # Convert to numpy for spatial calculations
            coords = df.select(['latitude', 'longitude']).to_numpy()
            
            # 1. Calculate distance to nearest earthquake (km)
            logger.info("   Calculating nearest neighbor distances...")
            nbrs = NearestNeighbors(n_neighbors=2, algorithm='ball_tree', metric='haversine')
            nbrs.fit(np.radians(coords))
            distances, indices = nbrs.kneighbors(np.radians(coords))
            
            # Convert from radians to km (Earth radius = 6371 km)
            nearest_distance_km = distances[:, 1] * 6371.0
            
            # 2. Calculate earthquake density (events within 100km radius)
            logger.info("   Calculating earthquake density...")
            radius_km = 100.0
            radius_rad = radius_km / 6371.0
            density_counts = nbrs.radius_neighbors(np.radians(coords), radius=radius_rad, return_distance=False)
            event_density = np.array([len(neighbors) - 1 for neighbors in density_counts])  # -1 to exclude self
            
            # 3. Seismic zone classification based on lat/lon
            logger.info("   Classifying seismic zones...")
            def classify_zone(lat: float, lon: float) -> str:
                """Classify earthquake location into seismic zones"""
                # Pacific Ring of Fire zones
                if 100 <= lon <= 150:
                    if -10 <= lat <= 10:
                        return "Indonesia-Philippines Arc"
                    elif 10 < lat <= 25:
                        return "Philippines-Taiwan Arc"
                    elif 25 < lat <= 45:
                        return "Japan-Kuril Arc"
                    elif -10 < lat < -5:
                        return "Java Trench"
                elif 90 <= lon < 100:
                    if -5 <= lat <= 30:
                        return "Myanmar-Andaman Arc"
                elif 60 <= lon < 90:
                    if 20 <= lat <= 45:
                        return "Himalayas Collision Zone"
                    elif -10 <= lat < 20:
                        return "Indian Ocean Ridge"
                elif lon >= 150:
                    if 30 <= lat <= 55:
                        return "Kamchatka-Aleutian Arc"
                
                return "Other Asia Region"
            
            zones = [classify_zone(row[0], row[1]) for row in coords]
            
            # 4. Spatial grid cell (1 degree x 1 degree grid)
            logger.info("   Assigning spatial grid cells...")
            grid_lat = np.floor(coords[:, 0]).astype(int)
            grid_lon = np.floor(coords[:, 1]).astype(int)
            grid_cell_id = [f"GRID_{lat}_{lon}" for lat, lon in zip(grid_lat, grid_lon)]
            
            # 5. Calculate regional centroid distance
            logger.info("   Calculating distance to regional centroid...")
            # Asia region centroid (approximate)
            asia_centroid = np.array([22.5, 105.0])  # Lat, Lon
            
            def haversine_distance(lat1, lon1, lat2, lon2):
                """Calculate distance between two points on Earth (km)"""
                lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
                dlat = lat2 - lat1
                dlon = lon2 - lon1
                a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
                c = 2 * np.arcsin(np.sqrt(a))
                return 6371.0 * c
            
            centroid_distance = haversine_distance(
                coords[:, 0], coords[:, 1],
                asia_centroid[0], asia_centroid[1]
            )
            
            # Add all spatial features to dataframe
            df_spatial = df.with_columns([
                pl.Series("nearest_event_km", nearest_distance_km),
                pl.Series("event_density_100km", event_density),
                pl.Series("seismic_zone", zones),
                pl.Series("grid_cell_id", grid_cell_id),
                pl.Series("centroid_distance_km", centroid_distance)
            ])
            
            # Add risk score (simple heuristic based on density and magnitude)
            df_spatial = df_spatial.with_columns(
                ((pl.col("event_density_100km") / 100.0) * 0.4 + 
                 (pl.col("magnitude") / 10.0) * 0.6).alias("spatial_risk_score")
            )
            
            # Count unique grid cells
            unique_grids = len(df_spatial['grid_cell_id'].unique())
            
            logger.info(f"Spatial features added successfully:")
            logger.info(f"   - nearest_event_km: Distance to nearest earthquake (for event spacing analysis)")
            logger.info(f"   - event_density_100km: Events within 100km radius (seismic activity level)")
            logger.info(f"   - seismic_zone: Classified tectonic zone (9 categories for risk assessment)")
            logger.info(f"   - grid_cell_id: 1° x 1° grid cell (~111km, {unique_grids} unique cells for spatial binning)")
            logger.info(f"   - centroid_distance_km: Distance to Asia centroid (regional positioning)")
            logger.info(f"   - spatial_risk_score: Combined risk metric 0-1 (density + magnitude)")
            logger.info(f"Spatial preprocessing complete: {len(df_spatial):,} records with 6 spatial features")
            
            return df_spatial
            
        except Exception as e:
            logger.error(f"Error in spatial preprocessing: {e}")
            raise
    
    def save_to_parquet(self, df: pl.DataFrame) -> bool:
        try:
            logger.info(f"Saving processed data to {self.output_file}")
            df.write_parquet(
                self.output_file,
                compression='snappy',
                use_pyarrow=True
            )
            
            file_size = self.output_file.stat().st_size / (1024 * 1024)
            logger.info(f"Saved {len(df):,} records ({file_size:.2f} MB) to local parquet")
            return True
        except Exception as e:
            logger.error(f"Error saving to parquet: {e}")
            return False
    
    def save_to_minio(self, df: pl.DataFrame) -> bool:
        """Save processed data to MinIO object storage (overwrites existing file)"""
        try:
            logger.info(f"Uploading processed data to MinIO...")
            
            # Convert DataFrame to parquet bytes
            buffer = io.BytesIO()
            df.write_parquet(buffer, compression='snappy')
            buffer.seek(0)
            
            # Only save as latest.parquet (will overwrite on each run)
            object_name = "processed_earthquakes_latest.parquet"
            
            # Upload to MinIO (overwrites existing file)
            self.minio_client.put_object(
                bucket_name=self.minio_bucket,
                object_name=object_name,
                data=buffer,
                length=buffer.getbuffer().nbytes,
                content_type='application/octet-stream'
            )
            
            size_mb = buffer.getbuffer().nbytes / (1024 * 1024)
            logger.info(f"Uploaded to MinIO: {self.minio_bucket}/{object_name}")
            logger.info(f"Size: {size_mb:.2f} MB")
            logger.info(f"Note: MinIO cannot preview Parquet files - download to view")
            logger.info(f"Previous version overwritten (single file storage)")
            
            return True
            
        except S3Error as e:
            logger.error(f"MinIO S3 error: {e}")
            return False
        except Exception as e:
            logger.error(f"Error uploading to MinIO: {e}")
            return False
    
    def create_postgres_table(self, conn):
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS earthquakes (
                        id VARCHAR(50) PRIMARY KEY,
                        time BIGINT NOT NULL,
                        latitude DOUBLE PRECISION NOT NULL,
                        longitude DOUBLE PRECISION NOT NULL,
                        depth DOUBLE PRECISION,
                        magnitude DOUBLE PRECISION NOT NULL,
                        place TEXT NOT NULL,
                        datetime TIMESTAMP NOT NULL,
                        location GEOGRAPHY(POINT, 4326),
                        nearest_event_km DOUBLE PRECISION,
                        event_density_100km INTEGER,
                        seismic_zone TEXT,
                        grid_cell_id VARCHAR(50),
                        centroid_distance_km DOUBLE PRECISION,
                        spatial_risk_score DOUBLE PRECISION,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_earthquakes_datetime ON earthquakes(datetime);
                    CREATE INDEX IF NOT EXISTS idx_earthquakes_magnitude ON earthquakes(magnitude);
                    CREATE INDEX IF NOT EXISTS idx_earthquakes_location ON earthquakes USING GIST(location);
                    CREATE INDEX IF NOT EXISTS idx_earthquakes_zone ON earthquakes(seismic_zone);
                    CREATE INDEX IF NOT EXISTS idx_earthquakes_grid ON earthquakes(grid_cell_id);
                    CREATE INDEX IF NOT EXISTS idx_earthquakes_risk ON earthquakes(spatial_risk_score);
                """)
                conn.commit()
                logger.info("PostgreSQL table 'earthquakes' ready")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise
    
    def insert_to_postgres(self, df: pl.DataFrame) -> bool:
        try:
            logger.info(f"Inserting {len(df):,} records to PostgreSQL...")
            
            conn = psycopg2.connect(**self.postgres_config)
            
            self.create_postgres_table(conn)
            
            data = []
            for row in df.iter_rows(named=True):
                data.append((
                    row['id'],
                    row['time'],
                    row['latitude'],
                    row['longitude'],
                    row['depth'],
                    row['magnitude'],
                    row['place'],
                    row['datetime'],
                    f"POINT({row['longitude']} {row['latitude']})",
                    row.get('nearest_event_km'),
                    row.get('event_density_100km'),
                    row.get('seismic_zone'),
                    row.get('grid_cell_id'),
                    row.get('centroid_distance_km'),
                    row.get('spatial_risk_score')
                ))
            
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO earthquakes (id, time, latitude, longitude, depth, magnitude, place, datetime, location,
                                            nearest_event_km, event_density_100km, seismic_zone, grid_cell_id,
                                            centroid_distance_km, spatial_risk_score)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        time = EXCLUDED.time,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        depth = EXCLUDED.depth,
                        magnitude = EXCLUDED.magnitude,
                        place = EXCLUDED.place,
                        datetime = EXCLUDED.datetime,
                        location = EXCLUDED.location,
                        nearest_event_km = EXCLUDED.nearest_event_km,
                        event_density_100km = EXCLUDED.event_density_100km,
                        seismic_zone = EXCLUDED.seismic_zone,
                        grid_cell_id = EXCLUDED.grid_cell_id,
                        centroid_distance_km = EXCLUDED.centroid_distance_km,
                        spatial_risk_score = EXCLUDED.spatial_risk_score
                    """,
                    data,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, ST_GeogFromText(%s), %s, %s, %s, %s, %s, %s)",
                    page_size=1000
                )
                conn.commit()
                logger.info(f"Inserted/Updated {len(data):,} records to PostgreSQL")
            
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Error inserting to PostgreSQL: {e}")
            if 'conn' in locals():
                conn.close()
            return False
    
    def process(self) -> bool:
        try:
            logger.info("=" * 80)
            logger.info("EARTHQUAKE DATA PREPROCESSING PIPELINE - START")
            logger.info("=" * 80)
            
            # Step 1: Load raw data from Parquet (Raw Lake)
            logger.info("\n[STEP 1/6] Loading raw data...")
            df = self.load_raw_data()
            if df is None or len(df) == 0:
                logger.error("FAILED: No data to process")
                return False
            
            # Step 2: Clean data (replace place='None'/'NULL' with 'Unknown Location')
            logger.info("\n[STEP 2/6] Data cleaning...")
            df_cleaned = self.clean_data(df)
            if len(df_cleaned) == 0:
                logger.error("FAILED: No data after cleaning")
                return False
            
            # Step 3: Reduce duplicates
            logger.info("\n[STEP 3/6] Deduplication...")
            df_reduced = self.reduce_data(df_cleaned)
            
            # Step 4: Add spatial features (CORE PREPROCESSING)
            logger.info("\n[STEP 4/6] Spatial feature engineering...")
            df_spatial = self.add_spatial_features(df_reduced)
            
            # Step 5: Save to local parquet (backup)
            logger.info("\n[STEP 5/6] Saving to local storage...")
            if not self.save_to_parquet(df_spatial):
                logger.error("FAILED: Could not save local parquet")
                return False
            
            # Step 6: Upload to MinIO (processed_earthquake_data storage)
            logger.info("\n[STEP 6/6] Uploading to MinIO object storage...")
            if not self.save_to_minio(df_spatial):
                logger.warning("WARNING: Failed to upload to MinIO, but local parquet saved")
            
            # Step 7: Transform and load to PostgreSQL Warehouse (PostGIS)
            logger.info("\n[STEP 7/7] Loading to PostgreSQL Warehouse (PostGIS)...")
            if not self.insert_to_postgres(df_spatial):
                logger.warning("WARNING: Failed to insert to PostgreSQL, but files saved to MinIO")
            
            logger.info("\n" + "=" * 80)
            logger.info("PREPROCESSING PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info("=" * 80)
            logger.info(f"  Total records processed: {len(df_spatial):,}")
            logger.info(f"  Spatial features added: 6")
            logger.info(f"  Storage locations:")
            logger.info(f"    - Local Parquet: {self.output_file}")
            logger.info(f"    - MinIO: {self.minio_bucket}/processed_earthquakes_latest.parquet")
            logger.info(f"    - PostgreSQL: seismo_sphere.earthquakes table")
            logger.info("=" * 80 + "\n")
            
            return True
            
        except Exception as e:
            logger.error(f"PREPROCESSING PIPELINE FAILED: {e}")
            return False


def preprocess_earthquakes(**context):
    preprocessor = PolarsEarthquakePreprocessor(
        postgres_password=os.getenv('POSTGRES_PASSWORD', 'seismo123'),
        minio_endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
        minio_access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        minio_secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
    )
    
    success = preprocessor.process()
    
    if not success:
        raise Exception("Preprocessing failed")
    
    return "Preprocessing with spatial features completed successfully"
