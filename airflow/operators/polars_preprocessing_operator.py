import polars as pl
import logging
from pathlib import Path
from typing import Optional
import psycopg2
from psycopg2.extras import execute_values
import os

logger = logging.getLogger(__name__)

class PolarsEarthquakePreprocessor:
    def __init__(
        self,
        input_file: str = '/opt/airflow/data/bigdata/raw_earthquakes.parquet',
        output_file: str = '/opt/airflow/data/bigdata/processed_earthquakes.parquet',
        postgres_host: str = 'postgres',
        postgres_user: str = 'postgres',
        postgres_password: str = 'seismo123',
        postgres_db: str = 'seismo_sphere'
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
        
        logger.info(f"Polars Earthquake Preprocessor initialized")
        logger.info(f"  Input: {self.input_file}")
        logger.info(f"  Output: {self.output_file}")
    
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
            
            df_cleaned = df.filter(
                pl.col('place').is_not_null() & 
                (pl.col('place') != 'None')
            )
            removed_place = initial_count - len(df_cleaned)
            logger.info(f"   Removed {removed_place:,} rows with null/None 'place'")
            
            df_cleaned = df_cleaned.filter(
                pl.col('latitude').is_not_null() &
                pl.col('longitude').is_not_null() &
                pl.col('magnitude').is_not_null() &
                pl.col('time').is_not_null()
            )
            
            removed_count = initial_count - len(df_cleaned)
            logger.info(f"   Total removed: {removed_count:,} rows ({removed_count/initial_count*100:.2f}%)")
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
    
    def save_to_parquet(self, df: pl.DataFrame) -> bool:
        try:
            logger.info(f"Saving processed data to {self.output_file}")
            df.write_parquet(
                self.output_file,
                compression='snappy',
                use_pyarrow=True
            )
            
            file_size = self.output_file.stat().st_size / (1024 * 1024)
            logger.info(f"Saved {len(df):,} records ({file_size:.2f} MB)")
            return True
        except Exception as e:
            logger.error(f"Error saving to parquet: {e}")
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
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_earthquakes_datetime ON earthquakes(datetime);
                    CREATE INDEX IF NOT EXISTS idx_earthquakes_magnitude ON earthquakes(magnitude);
                    CREATE INDEX IF NOT EXISTS idx_earthquakes_location ON earthquakes USING GIST(location);
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
                    f"POINT({row['longitude']} {row['latitude']})"
                ))
            
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO earthquakes (id, time, latitude, longitude, depth, magnitude, place, datetime, location)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET
                        time = EXCLUDED.time,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        depth = EXCLUDED.depth,
                        magnitude = EXCLUDED.magnitude,
                        place = EXCLUDED.place,
                        datetime = EXCLUDED.datetime,
                        location = EXCLUDED.location
                    """,
                    data,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, ST_GeogFromText(%s))",
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
            logger.info("=" * 60)
            logger.info("Starting Earthquake Data Preprocessing")
            logger.info("=" * 60)
            
            df = self.load_raw_data()
            if df is None or len(df) == 0:
                logger.error("No data to process")
                return False
            
            df_cleaned = self.clean_data(df)
            if len(df_cleaned) == 0:
                logger.error("No data after cleaning")
                return False
            
            df_reduced = self.reduce_data(df_cleaned)
            
            if not self.save_to_parquet(df_reduced):
                return False
            
            if not self.insert_to_postgres(df_reduced):
                logger.warning("Failed to insert to PostgreSQL, but parquet saved")
            
            logger.info("=" * 60)
            logger.info("Preprocessing completed successfully!")
            logger.info(f"Final record count: {len(df_reduced):,}")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            logger.error(f"Preprocessing failed: {e}")
            return False


def preprocess_earthquakes(**context):
    preprocessor = PolarsEarthquakePreprocessor(
        postgres_password=os.getenv('POSTGRES_PASSWORD', 'seismo123')
    )
    
    success = preprocessor.process()
    
    if not success:
        raise Exception("Preprocessing failed")
    
    return "Preprocessing completed successfully"
