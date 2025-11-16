import polars as pl
import requests
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import time

logger = logging.getLogger(__name__)

class PolarsEarthquakeIngestor:
    ASIA_BOUNDS = {
        'minlatitude': -10,
        'maxlatitude': 55,
        'minlongitude': 60,
        'maxlongitude': 150
    }
    
    def __init__(
        self,
        output_dir: str = '/opt/airflow/data/bigdata',
        min_magnitude: float = 0.0,
        region: str = 'asia'
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.min_magnitude = min_magnitude
        self.region = region
        self.base_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
        
        logger.info(f"🚀 Polars Earthquake Ingestor initialized")
        logger.info(f"📁 Output directory: {self.output_dir}")
        logger.info(f"🌏 Region: {region.upper()}")
        logger.info(f"📊 Min magnitude: {min_magnitude}")
    
    def fetch_data_batch(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: int = 20000
    ) -> Optional[Dict]:
        params = {
            'format': 'geojson',
            'starttime': start_date.strftime('%Y-%m-%d'),
            'endtime': end_date.strftime('%Y-%m-%d'),
            'minmagnitude': self.min_magnitude,
            'limit': limit,
            'orderby': 'time-asc'
        }
        
        if self.region == 'asia':
            params.update(self.ASIA_BOUNDS)
        
        try:
            logger.info(f"📥 Fetching: {start_date.date()} to {end_date.date()}")
            response = requests.get(self.base_url, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            count = len(data.get('features', []))
            logger.info(f"✅ Received: {count} earthquakes")
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error fetching data: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
            return None
    
    def geojson_to_polars(self, geojson_data: Dict) -> pl.DataFrame:
        """Convert GeoJSON data ke Polars DataFrame"""
        features = geojson_data.get('features', [])
        
        if not features:
            return pl.DataFrame()
        
        records = []
        
        for feature in features:
            try:
                props = feature.get('properties', {})
                coords = feature.get('geometry', {}).get('coordinates', [None, None, None])
                
                eq_id = str(feature.get('id', ''))
                eq_time = props.get('time')
                eq_lat = coords[1] if len(coords) > 1 else None
                eq_lon = coords[0] if len(coords) > 0 else None
                eq_depth = coords[2] if len(coords) > 2 else None
                eq_mag = props.get('mag')
                eq_place = str(props.get('place', ''))
                
                if eq_time is not None:
                    eq_time = float(eq_time)
                if eq_lat is not None:
                    eq_lat = float(eq_lat)
                if eq_lon is not None:
                    eq_lon = float(eq_lon)
                if eq_depth is not None:
                    eq_depth = float(eq_depth)
                if eq_mag is not None:
                    eq_mag = float(eq_mag)
                
                records.append({
                    'id': eq_id,
                    'time': eq_time,
                    'latitude': eq_lat,
                    'longitude': eq_lon,
                    'depth': eq_depth,
                    'magnitude': eq_mag,
                    'place': eq_place
                })
                
            except (ValueError, TypeError, AttributeError) as e:
                logger.warning(f"Skipping invalid record: {e}")
                continue
        
        if not records:
            return pl.DataFrame()
        
        df = pl.DataFrame(records)
        
        df = (df
            .with_columns([
                pl.when(pl.col('time').is_not_null())
                  .then(pl.col('time').cast(pl.Int64).cast(pl.Datetime('ms')))
                  .otherwise(None)
                  .alias('datetime'),
            ])
            .filter(
                pl.col('magnitude').is_not_null() &
                pl.col('latitude').is_not_null() &
                pl.col('longitude').is_not_null() &
                pl.col('datetime').is_not_null()
            )
            .unique(subset=['id'], keep='first')
            .sort('datetime')
        )
        
        return df
    
    def ingest_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        batch_days: int = 30
    ) -> pl.DataFrame:
        logger.info("="*70)
        logger.info("🌍 BATCH DATA INGESTION - POLARS")
        logger.info("="*70)
        logger.info(f"📅 Date Range: {start_date.date()} to {end_date.date()}")
        logger.info(f"📊 Batch Size: {batch_days} days")
        logger.info(f"🌏 Region: {self.region.upper()}")
        logger.info("="*70)
        
        all_dataframes = []
        current_date = start_date
        batch_count = 0
        total_records = 0
        
        while current_date < end_date:
            batch_count += 1
            batch_end = min(current_date + timedelta(days=batch_days), end_date)
            
            logger.info(f"\n📦 Batch #{batch_count}")
            logger.info(f"   Period: {current_date.date()} to {batch_end.date()}")
            
            geojson_data = self.fetch_data_batch(current_date, batch_end)
            
            if geojson_data:
                df = self.geojson_to_polars(geojson_data)
                
                if len(df) > 0:
                    all_dataframes.append(df)
                    total_records += len(df)
                    logger.info(f"   ✅ Batch records: {len(df)}")
                    logger.info(f"   📈 Total so far: {total_records}")
                else:
                    logger.warning(f"   ⚠️  No valid records in this batch")
            else:
                logger.warning(f"   ⚠️  Failed to fetch batch")
            
            current_date = batch_end
            time.sleep(1)
        
        logger.info(f"\n🔗 Combining {len(all_dataframes)} batches...")
        
        if not all_dataframes:
            logger.error("❌ No data collected!")
            return pl.DataFrame()
        
        combined_df = pl.concat(all_dataframes)
        combined_df = combined_df.unique(subset=['id'], keep='first')
        
        logger.info(f"✅ Total unique records: {len(combined_df)}")
        
        return combined_df
    
    def ingest_last_year(self) -> pl.DataFrame:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        logger.info("📅 Ingesting LAST 1 YEAR of earthquake data")
        logger.info(f"   From: {start_date.date()}")
        logger.info(f"   To: {end_date.date()}")
        
        return self.ingest_date_range(start_date, end_date, batch_days=30)
    
    def ingest_last_30_days(self) -> pl.DataFrame:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        logger.info("📅 Ingesting LAST 30 DAYS of earthquake data")
        logger.info(f"   From: {start_date.date()}")
        logger.info(f"   To: {end_date.date()}")
        
        return self.ingest_date_range(start_date, end_date, batch_days=30)
    
    def save_to_parquet(
        self,
        df: pl.DataFrame,
        filename: str = 'earthquakes_data.parquet',
        compression: str = 'snappy'
    ) -> Path:
        output_path = self.output_dir / filename
        
        logger.info(f"\n💾 Saving to Parquet...")
        logger.info(f"   File: {output_path}")
        logger.info(f"   Compression: {compression}")
        
        df.write_parquet(
            output_path,
            compression=compression,
            statistics=True,
            use_pyarrow=True
        )
        
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        
        logger.info(f"   ✅ Saved: {file_size_mb:.2f} MB")
        logger.info(f"   📊 Records: {len(df):,}")
        
        return output_path
    
    def save_to_csv(
        self,
        df: pl.DataFrame,
        filename: str = 'earthquakes_data.csv'
    ) -> Path:
        output_path = self.output_dir / filename
        
        logger.info(f"\n💾 Saving to CSV...")
        logger.info(f"   File: {output_path}")
        
        df_export = df.with_columns([
            pl.col('datetime').dt.strftime('%Y-%m-%dT%H:%M:%S').alias('time')
        ]).select([
            'id', 'time', 'latitude', 'longitude', 'depth', 'magnitude', 'place'
        ])
        
        df_export.write_csv(output_path)
        
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        logger.info(f"   ✅ Saved: {file_size_mb:.2f} MB")
        
        return output_path
    
    def get_data_summary(self, df: pl.DataFrame) -> Dict:
        if len(df) == 0:
            return {'error': 'No data available'}
        
        summary = {
            'total_records': len(df),
            'unique_earthquakes': df['id'].n_unique(),
            'date_range': {
                'start': str(df['datetime'].min()),
                'end': str(df['datetime'].max())
            },
            'magnitude': {
                'min': float(df['magnitude'].min()),
                'max': float(df['magnitude'].max()),
                'mean': float(df['magnitude'].mean()),
                'median': float(df['magnitude'].median())
            },
            'depth': {
                'min': float(df['depth'].min()),
                'max': float(df['depth'].max()),
                'mean': float(df['depth'].mean())
            },
            'geographic': {
                'latitude_range': [float(df['latitude'].min()), float(df['latitude'].max())],
                'longitude_range': [float(df['longitude'].min()), float(df['longitude'].max())]
            }
        }
        
        return summary
