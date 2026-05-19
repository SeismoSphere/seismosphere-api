import polars as pl
import pandas as pd
import logging
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN
import hdbscan
from datetime import datetime
from typing import Dict, Optional, List, Tuple

logger = logging.getLogger(__name__)


class HDBSCANEarthquakeClusterer:
    """
    Earthquake clustering operator using Polars, DBSCAN, and HDBSCAN.
    Reads data from PostgreSQL, performs clustering, and saves results back to PostgreSQL.
    """
    
    # Risk level thresholds for hazard zone classification (updated with weighted scoring)
    RISK_THRESHOLDS = {
        'LOW': {
            'description': 'Low seismic activity',
            'score_max': 1.8
        },
        'MEDIUM': {
            'description': 'Moderate seismic activity',
            'score_min': 1.8,
            'score_max': 4.0
        },
        'HIGH': {
            'description': 'High seismic activity',
            'score_min': 4.0,
            'score_max': 6.0
        },
        'VERY_HIGH': {
            'description': 'Very high seismic activity',
            'score_min': 6.0
        }
    }
    
    def __init__(
        self,
        postgres_host: str = 'postgres',
        postgres_user: str = 'postgres',
        postgres_password: str = 'seismo123',
        postgres_db: str = 'seismo_sphere',
        dbscan_eps: float = 0.5,
        dbscan_min_samples: int = 5,
        hdbscan_min_cluster_size: int = 10,
        srid: int = 4326,
        batch_size: int = 1000
    ):
        """
        Initialize the Earthquake Clusterer.
        
        Args:
            postgres_host: PostgreSQL host
            postgres_user: PostgreSQL user
            postgres_password: PostgreSQL password
            postgres_db: PostgreSQL database
            dbscan_eps: DBSCAN epsilon parameter
            dbscan_min_samples: DBSCAN min_samples parameter
            hdbscan_min_cluster_size: HDBSCAN min_cluster_size parameter
            srid: Spatial Reference ID for PostGIS (default: 4326 - WGS84)
            batch_size: Batch size for database inserts
        """
        self.postgres_config = {
            'host': postgres_host,
            'user': postgres_user,
            'password': postgres_password,
            'database': postgres_db,
            'port': 5432
        }
        
        self.dbscan_eps = dbscan_eps
        self.dbscan_min_samples = dbscan_min_samples
        self.hdbscan_min_cluster_size = hdbscan_min_cluster_size
        self.srid = srid
        self.batch_size = batch_size
        
        # Feature columns for clustering
        self.feature_columns = ['latitude', 'longitude', 'magnitude', 'depth']
        
        # Scaling factor for geographic coordinates (degrees to approximate km)
        # 1 degree ≈ 111 km
        self.feature_scaling = {
            'latitude': 111.0,
            'longitude': 111.0,
            'magnitude': 1.0,
            'depth': 1.0  # Already in km
        }
        
        self.raw_data = None
        self.features_df = None
        self.features_scaled = None
        self.dbscan_labels = None
        self.hdbscan_labels = None
        self.scaler = None
        self.conn = None  # Database connection for risk labeling
        
        logger.info(f"Earthquake Clusterer initialized")
        logger.info(f"  PostgreSQL: {postgres_host}/{postgres_db}")
        logger.info(f"  DBSCAN: eps={dbscan_eps}, min_samples={dbscan_min_samples}")
        logger.info(f"  HDBSCAN: min_cluster_size={hdbscan_min_cluster_size}")
        logger.info(f"  Batch size: {batch_size}")
    
    def connect_postgres(self) -> Optional[psycopg2.extensions.connection]:
        """
        Establish PostgreSQL connection.
        
        Returns:
            PostgreSQL connection or None if failed
        """
        try:
            conn = psycopg2.connect(**self.postgres_config)
            logger.info(f"Connected to PostgreSQL: {self.postgres_config['host']}/{self.postgres_config['database']}")
            return conn
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return None
    
    def read_earthquake_data(self) -> Optional[pl.DataFrame]:
        """
        Read earthquake data from PostgreSQL earthquakes table.
        
        Returns:
            Polars DataFrame with earthquake data or None if failed
        """
        conn = None
        try:
            conn = self.connect_postgres()
            if not conn:
                return None
            
            cur = conn.cursor()
            
            # Query to read earthquake data
            query = """
                SELECT 
                    id,
                    datetime,
                    latitude,
                    longitude,
                    magnitude,
                    depth
                FROM earthquakes
                WHERE latitude IS NOT NULL
                  AND longitude IS NOT NULL
                  AND magnitude IS NOT NULL
                  AND depth IS NOT NULL
                ORDER BY datetime DESC
            """
            
            logger.info(f"Reading earthquake data from PostgreSQL...")
            cur.execute(query)
            
            # Fetch data and convert to Polars DataFrame
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            
            if not rows:
                logger.warning(f"No data found in earthquakes table")
                return None
            
            # Create Polars DataFrame
            data_dict = {col: [] for col in columns}
            for row in rows:
                for col, val in zip(columns, row):
                    data_dict[col].append(val)
            
            df = pl.DataFrame(data_dict)
            logger.info(f"Loaded {len(df):,} earthquake records from PostgreSQL")
            
            cur.close()
            
            return df
        
        except Exception as e:
            logger.error(f"Error reading earthquake data: {e}")
            return None
        
        finally:
            if conn:
                conn.close()
    
    def prepare_features(self) -> bool:
        """
        Prepare and scale features for clustering.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Preparing features for clustering...")
            
            if self.raw_data is None or len(self.raw_data) == 0:
                logger.error(f"No data available for feature preparation")
                return False
            
            # Extract feature columns
            self.features_df = self.raw_data.select(self.feature_columns)
            
            # Convert to numpy array
            features_array = self.features_df.to_numpy()
            
            # Handle NaN values
            features_array = np.nan_to_num(features_array, nan=0.0, posinf=0.0, neginf=0.0)
            
            logger.info(f"Features shape: {features_array.shape}")
            logger.info(f"  - Latitude range: [{features_array[:, 0].min():.2f}, {features_array[:, 0].max():.2f}]")
            logger.info(f"  - Longitude range: [{features_array[:, 1].min():.2f}, {features_array[:, 1].max():.2f}]")
            logger.info(f"  - Magnitude range: [{features_array[:, 2].min():.2f}, {features_array[:, 2].max():.2f}]")
            logger.info(f"  - Depth range: [{features_array[:, 3].min():.2f}, {features_array[:, 3].max():.2f}]")
            
            # Standardize features
            self.scaler = StandardScaler()
            self.features_scaled = self.scaler.fit_transform(features_array)
            
            logger.info(f"Features scaled using StandardScaler")
            logger.info(f"  - Mean: [{self.features_scaled.mean(axis=0)}]")
            logger.info(f"  - Std: [{self.features_scaled.std(axis=0)}]")
            
            return True
        
        except Exception as e:
            logger.error(f"Error preparing features: {e}")
            return False
    
    def run_dbscan(self) -> bool:
        """
        Run DBSCAN clustering algorithm.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Running DBSCAN clustering...")
            logger.info(f"  Parameters: eps={self.dbscan_eps}, min_samples={self.dbscan_min_samples}")
            
            if self.features_scaled is None:
                logger.error(f"Features not prepared yet")
                return False
            
            # Run DBSCAN
            dbscan = DBSCAN(eps=self.dbscan_eps, min_samples=self.dbscan_min_samples)
            self.dbscan_labels = dbscan.fit_predict(self.features_scaled)
            
            # Analyze results
            n_clusters = len(set(self.dbscan_labels)) - (1 if -1 in self.dbscan_labels else 0)
            n_noise = list(self.dbscan_labels).count(-1)
            
            logger.info(f"DBSCAN Results:")
            logger.info(f"  - Number of clusters: {n_clusters}")
            logger.info(f"  - Number of noise points: {n_noise}")
            logger.info(f"  - Cluster distribution:")
            
            for cluster_id in sorted(set(self.dbscan_labels)):
                count = list(self.dbscan_labels).count(cluster_id)
                if cluster_id == -1:
                    logger.info(f"    - Noise: {count} points")
                else:
                    logger.info(f"    - Cluster {cluster_id}: {count} points")
            
            return True
        
        except Exception as e:
            logger.error(f"Error running DBSCAN: {e}")
            return False
    
    def run_hdbscan(self) -> bool:
        """
        Run HDBSCAN clustering algorithm.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Running HDBSCAN clustering...")
            logger.info(f"  Parameters: min_cluster_size={self.hdbscan_min_cluster_size}")
            
            if self.features_scaled is None:
                logger.error(f"Features not prepared yet")
                return False
            
            # Run HDBSCAN
            clusterer = hdbscan.HDBSCAN(min_cluster_size=self.hdbscan_min_cluster_size)
            self.hdbscan_labels = clusterer.fit_predict(self.features_scaled)
            self.hdbscan_probs = clusterer.probabilities_
            
            # Analyze results
            n_clusters = len(set(self.hdbscan_labels)) - (1 if -1 in self.hdbscan_labels else 0)
            n_noise = list(self.hdbscan_labels).count(-1)
            
            logger.info(f"HDBSCAN Results:")
            logger.info(f"  - Number of clusters: {n_clusters}")
            logger.info(f"  - Number of noise points: {n_noise}")
            logger.info(f"  - Cluster distribution:")
            
            for cluster_id in sorted(set(self.hdbscan_labels)):
                count = list(self.hdbscan_labels).count(cluster_id)
                if cluster_id == -1:
                    logger.info(f"    - Noise: {count} points")
                else:
                    logger.info(f"    - Cluster {cluster_id}: {count} points")
            
            return True
        
        except Exception as e:
            logger.error(f"Error running HDBSCAN: {e}")
            return False
    
    def create_cluster_tables(self) -> bool:
        """
        Create tables in PostgreSQL for storing cluster results.
        
        Returns:
            True if successful, False otherwise
        """
        conn = None
        try:
            conn = self.connect_postgres()
            if not conn:
                return False
            
            cur = conn.cursor()
            
            # Create DBSCAN table
            create_dbscan_table = f"""
                DROP TABLE IF EXISTS earthquakes_dbscan_clusters CASCADE;
                
                CREATE TABLE earthquakes_dbscan_clusters (
                    id TEXT PRIMARY KEY,
                    datetime TIMESTAMP,
                    latitude FLOAT8,
                    longitude FLOAT8,
                    magnitude FLOAT8,
                    depth FLOAT8,
                    cluster_id INT,
                    geometry geometry(POINT, {self.srid}),
                    clustering_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_dbscan_cluster_id ON earthquakes_dbscan_clusters(cluster_id);
                CREATE INDEX idx_dbscan_geometry ON earthquakes_dbscan_clusters USING GIST(geometry);
            """
            
            # Create HDBSCAN table
            create_hdbscan_table = f"""
                DROP TABLE IF EXISTS earthquakes_hdbscan_clusters CASCADE;
                
                CREATE TABLE earthquakes_hdbscan_clusters (
                    id TEXT PRIMARY KEY,
                    datetime TIMESTAMP,
                    latitude FLOAT8,
                    longitude FLOAT8,
                    magnitude FLOAT8,
                    depth FLOAT8,
                    cluster_id INT,
                    hdbscan_probability FLOAT8,
                    geometry geometry(POINT, {self.srid}),
                    clustering_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX idx_hdbscan_cluster_id ON earthquakes_hdbscan_clusters(cluster_id);
                CREATE INDEX idx_hdbscan_geometry ON earthquakes_hdbscan_clusters USING GIST(geometry);
            """
            
            logger.info(f"Creating DBSCAN clustering table...")
            cur.execute(create_dbscan_table)
            logger.info(f"Created earthquakes_dbscan_clusters table")
            
            logger.info(f"Creating HDBSCAN clustering table...")
            cur.execute(create_hdbscan_table)
            logger.info(f"Created earthquakes_hdbscan_clusters table")
            
            conn.commit()
            cur.close()
            
            return True
        
        except Exception as e:
            logger.error(f"Error creating cluster tables: {e}")
            if conn:
                conn.rollback()
            return False
        
        finally:
            if conn:
                conn.close()
    
    def save_dbscan_clusters(self) -> bool:
        """
        Save DBSCAN clustering results to PostgreSQL.
        
        Returns:
            True if successful, False otherwise
        """
        conn = None
        try:
            if self.raw_data is None or self.dbscan_labels is None:
                logger.error(f"Data or DBSCAN labels not available")
                return False
            
            conn = self.connect_postgres()
            if not conn:
                return False
            
            cur = conn.cursor()
            
            logger.info(f"Saving DBSCAN cluster results to PostgreSQL...")
            
            # Convert Polars DataFrame to list of dicts
            raw_data_dicts = self.raw_data.to_dicts()
            
            # Prepare data for insertion
            data_to_insert = []
            for idx, row_dict in enumerate(raw_data_dicts):
                cluster_id = int(self.dbscan_labels[idx])
                
                # Extract values
                earthquake_id = str(row_dict['id'])
                datetime_val = row_dict['datetime']
                latitude = float(row_dict['latitude'])
                longitude = float(row_dict['longitude'])
                magnitude = float(row_dict['magnitude'])
                depth = float(row_dict['depth'])
                
                # Handle NaN values
                latitude = 0.0 if np.isnan(latitude) else latitude
                longitude = 0.0 if np.isnan(longitude) else longitude
                magnitude = 0.0 if np.isnan(magnitude) else magnitude
                depth = 0.0 if np.isnan(depth) else depth
                
                # Create PostGIS geometry point
                geometry_wkt = f"SRID={self.srid};POINT({longitude} {latitude})"
                
                data_to_insert.append((
                    earthquake_id,
                    datetime_val,
                    latitude,
                    longitude,
                    magnitude,
                    depth,
                    cluster_id,
                    geometry_wkt
                ))
            
            # Batch insert with page_size
            insert_query = """
                INSERT INTO earthquakes_dbscan_clusters 
                (id, datetime, latitude, longitude, magnitude, depth, cluster_id, geometry)
                VALUES %s
                ON CONFLICT (id) DO NOTHING
            """
            
            # Insert in batches
            for i in range(0, len(data_to_insert), self.batch_size):
                batch = data_to_insert[i:i + self.batch_size]
                
                try:
                    execute_values(cur, insert_query, batch, page_size=self.batch_size)
                    conn.commit()
                    logger.info(f"  - Inserted batch {i//self.batch_size + 1} ({len(batch)} records)")
                
                except Exception as e:
                    logger.error(f"  - Error inserting batch: {e}")
                    conn.rollback()
                    raise
            
            logger.info(f"Successfully saved {len(data_to_insert):,} DBSCAN cluster results")
            cur.close()
            
            return True
        
        except Exception as e:
            logger.error(f"Error saving DBSCAN clusters: {e}")
            if conn:
                conn.rollback()
            return False
        
        finally:
            if conn:
                conn.close()
    
    def save_hdbscan_clusters(self) -> bool:
        """
        Save HDBSCAN clustering results to PostgreSQL.
        
        Returns:
            True if successful, False otherwise
        """
        conn = None
        try:
            if self.raw_data is None or self.hdbscan_labels is None:
                logger.error(f"Data or HDBSCAN labels not available")
                return False
            
            conn = self.connect_postgres()
            if not conn:
                return False
            
            cur = conn.cursor()
            
            logger.info(f"Saving HDBSCAN cluster results to PostgreSQL...")
            
            # Convert Polars DataFrame to list of dicts
            raw_data_dicts = self.raw_data.to_dicts()
            
            # Prepare data for insertion
            data_to_insert = []
            for idx, row_dict in enumerate(raw_data_dicts):
                cluster_id = int(self.hdbscan_labels[idx])
                probability = float(self.hdbscan_probs[idx])
                
                # Extract values
                earthquake_id = str(row_dict['id'])
                datetime_val = row_dict['datetime']
                latitude = float(row_dict['latitude'])
                longitude = float(row_dict['longitude'])
                magnitude = float(row_dict['magnitude'])
                depth = float(row_dict['depth'])
                
                # Handle NaN values
                latitude = 0.0 if np.isnan(latitude) else latitude
                longitude = 0.0 if np.isnan(longitude) else longitude
                magnitude = 0.0 if np.isnan(magnitude) else magnitude
                depth = 0.0 if np.isnan(depth) else depth
                
                # Create PostGIS geometry point
                geometry_wkt = f"SRID={self.srid};POINT({longitude} {latitude})"
                
                data_to_insert.append((
                    earthquake_id,
                    datetime_val,
                    latitude,
                    longitude,
                    magnitude,
                    depth,
                    cluster_id,
                    probability,
                    geometry_wkt
                ))
            
            # Batch insert with page_size
            insert_query = """
                INSERT INTO earthquakes_hdbscan_clusters 
                (id, datetime, latitude, longitude, magnitude, depth, cluster_id, hdbscan_probability, geometry)
                VALUES %s
                ON CONFLICT (id) DO NOTHING
            """
            
            # Insert in batches
            for i in range(0, len(data_to_insert), self.batch_size):
                batch = data_to_insert[i:i + self.batch_size]
                
                try:
                    execute_values(cur, insert_query, batch, page_size=self.batch_size)
                    conn.commit()
                    logger.info(f"  - Inserted batch {i//self.batch_size + 1} ({len(batch)} records)")
                
                except Exception as e:
                    logger.error(f"  - Error inserting batch: {e}")
                    conn.rollback()
                    raise
            
            logger.info(f"Successfully saved {len(data_to_insert):,} HDBSCAN cluster results")
            cur.close()
            
            return True
        
        except Exception as e:
            logger.error(f"Error saving HDBSCAN clusters: {e}")
            if conn:
                conn.rollback()
            return False
        
        finally:
            if conn:
                conn.close()
    
    def get_summary_statistics(self) -> Dict:
        """
        Generate summary statistics for clustering results.
        
        Returns:
            Dictionary with summary statistics
        """
        try:
            summary = {
                'total_records': len(self.raw_data) if (self.raw_data is not None and not self.raw_data.is_empty()) else 0,
                'timestamp': datetime.now().isoformat(),
                'dbscan': {
                    'n_clusters': 0,
                    'n_noise': 0,
                    'cluster_sizes': {}
                },
                'hdbscan': {
                    'n_clusters': 0,
                    'n_noise': 0,
                    'cluster_sizes': {}
                }
            }
            
            # DBSCAN statistics
            if self.dbscan_labels is not None:
                dbscan_n_clusters = len(set(self.dbscan_labels)) - (1 if -1 in self.dbscan_labels else 0)
                dbscan_n_noise = list(self.dbscan_labels).count(-1)
                
                summary['dbscan']['n_clusters'] = int(dbscan_n_clusters)
                summary['dbscan']['n_noise'] = int(dbscan_n_noise)
                
                for cluster_id in sorted(set(self.dbscan_labels)):
                    count = list(self.dbscan_labels).count(cluster_id)
                    summary['dbscan']['cluster_sizes'][int(cluster_id)] = int(count)
            
            # HDBSCAN statistics
            if self.hdbscan_labels is not None:
                hdbscan_n_clusters = len(set(self.hdbscan_labels)) - (1 if -1 in self.hdbscan_labels else 0)
                hdbscan_n_noise = list(self.hdbscan_labels).count(-1)
                
                summary['hdbscan']['n_clusters'] = int(hdbscan_n_clusters)
                summary['hdbscan']['n_noise'] = int(hdbscan_n_noise)
                
                for cluster_id in sorted(set(self.hdbscan_labels)):
                    count = list(self.hdbscan_labels).count(cluster_id)
                    summary['hdbscan']['cluster_sizes'][int(cluster_id)] = int(count)
            
            return summary
        
        except Exception as e:
            logger.error(f"Error generating summary statistics: {e}")
            return {}
    
    def assign_risk_level(self, magnitude: float, depth: float, cluster_size: int) -> str:
        """
        Assign risk level using weighted scoring based on earthquake/cluster characteristics
        
        Scoring factors (equally weighted):
        - Cluster size (35%): Higher density = higher risk
        - Magnitude (35%): Stronger earthquakes = higher risk  
        - Depth (30%): Shallow earthquakes = higher risk
        
        Score ranges:
        - < 1.8 → LOW (isolated, low magnitude)
        - 1.8-4.0 → MEDIUM (moderate activity)
        - 4.0-6.5 → HIGH (significant activity)
        - ≥ 6.5 → VERY_HIGH (major seismic zone)
        
        Args:
            magnitude: Earthquake magnitude
            depth: Earthquake depth in km
            cluster_size: Number of earthquakes in cluster
        
        Returns:
            Risk level: 'LOW', 'MEDIUM', 'HIGH', or 'VERY_HIGH'
        """
        
        # Normalize factors to 0-10 scale with moderate scaling
        # Cluster size: log scale (1→0, 10→1, 100→2, 1000→3)
        cluster_score = min(10, np.log10(max(1, cluster_size)) * 2.0)
        
        # Magnitude: less aggressive scale (3.0→0, 7.0→3.33)
        # Most earthquakes are 4-5, so this spreads them across lower scores
        magnitude_score = max(0, min(10, (magnitude - 3.0) / 1.2))
        
        # Depth: inverse scale (300km→0, 0km→10) 
        # Shallow = more dangerous
        depth_score = max(0, min(10, (300 - depth) / 30))
        
        # Weighted sum: 35% cluster + 35% magnitude + 30% depth
        risk_score = (cluster_score * 0.35) + (magnitude_score * 0.35) + (depth_score * 0.30)
        
        # Assign level based on score with new thresholds
        if risk_score >= self.RISK_THRESHOLDS['VERY_HIGH']['score_min']:
            return 'VERY_HIGH'
        elif risk_score >= self.RISK_THRESHOLDS['HIGH']['score_min']:
            return 'HIGH'
        elif risk_score >= self.RISK_THRESHOLDS['MEDIUM']['score_min']:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def update_risk_labels(self) -> bool:
        """
        Read HDBSCAN cluster results and assign risk labels
        Updates earthquakes_hdbscan_clusters with risk_label column
        
        Returns:
            True if successful, False otherwise
        """
        conn = None
        cursor = None
        try:
            # Connect to database
            conn = self.connect_postgres()
            if not conn:
                logger.error("Failed to connect to PostgreSQL for risk labeling")
                return False
            
            cursor = conn.cursor()
            
            # Add risk_label column if not exists
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name='earthquakes_hdbscan_clusters' AND column_name='risk_label'
            """)
            
            if not cursor.fetchone():
                logger.info("Adding risk_label column to earthquakes_hdbscan_clusters...")
                cursor.execute("""
                    ALTER TABLE earthquakes_hdbscan_clusters
                    ADD COLUMN risk_label VARCHAR(20) DEFAULT 'MEDIUM'
                """)
                conn.commit()
                logger.info("✓ Column risk_label added")
            
            # Read HDBSCAN clusters with sizes
            cursor.execute("""
                SELECT id, magnitude, depth, cluster_id
                FROM earthquakes_hdbscan_clusters
                ORDER BY cluster_id
            """)
            rows = cursor.fetchall()
            
            if not rows:
                logger.warning("No HDBSCAN clusters to assign risk labels")
                return False
            
            # Calculate cluster sizes
            cluster_sizes = {}
            for row in rows:
                cluster_id = row[3]
                if cluster_id not in cluster_sizes:
                    cluster_sizes[cluster_id] = 0
                cluster_sizes[cluster_id] += 1
            
            # Assign risk labels
            risk_labels = []
            for row in rows:
                earthquake_id = row[0]
                magnitude = row[1]
                depth = row[2]
                cluster_id = row[3]
                cluster_size = cluster_sizes.get(cluster_id, 1)
                
                risk_label = self.assign_risk_level(magnitude, depth, cluster_size)
                risk_labels.append((risk_label, earthquake_id))
            
            # Update database
            logger.info(f"Updating {len(risk_labels)} records with risk labels...")
            cursor.executemany(
                """
                    UPDATE earthquakes_hdbscan_clusters
                    SET risk_label = %s
                    WHERE id = %s
                """,
                risk_labels
            )
            conn.commit()
            
            logger.info(f"✓ Updated {len(risk_labels)} earthquake records with risk labels")
            
            # Log risk distribution
            from collections import Counter
            risk_counts = Counter([item[0] for item in risk_labels])
            logger.info("\nRISK LEVEL DISTRIBUTION:")
            for risk_level in ['VERY_HIGH', 'HIGH', 'MEDIUM', 'LOW']:
                count = risk_counts.get(risk_level, 0)
                percentage = (count / len(risk_labels)) * 100 if risk_labels else 0
                logger.info(f"  {risk_level:12s}: {count:6d} earthquakes ({percentage:5.1f}%)")
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating risk labels: {e}")
            import traceback
            traceback.print_exc()
            if conn:
                conn.rollback()
            return False
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def run_full_pipeline(self) -> Dict:
        """
        Run the complete clustering pipeline.
        
        Returns:
            Dictionary with pipeline results and summary statistics
        """
        logger.info(f"="*70)
        logger.info(f"EARTHQUAKE CLUSTERING PIPELINE")
        logger.info(f"="*70)
        
        try:
            # Step 1: Read data
            logger.info(f"\nStep 1: Reading earthquake data from PostgreSQL...")
            self.raw_data = self.read_earthquake_data()
            if self.raw_data is None or len(self.raw_data) == 0:
                logger.error(f"Failed to read earthquake data")
                return {'status': 'FAILED', 'error': 'No earthquake data available'}
            
            # Step 2: Prepare features
            logger.info(f"\nStep 2: Preparing features...")
            if not self.prepare_features():
                return {'status': 'FAILED', 'error': 'Feature preparation failed'}
            
            # Step 3: Create cluster tables
            logger.info(f"\nStep 3: Creating cluster tables...")
            if not self.create_cluster_tables():
                return {'status': 'FAILED', 'error': 'Table creation failed'}
            
            # Step 4: Run DBSCAN
            logger.info(f"\nStep 4: Running DBSCAN clustering...")
            if not self.run_dbscan():
                return {'status': 'FAILED', 'error': 'DBSCAN clustering failed'}
            
            # Step 5: Save DBSCAN results
            logger.info(f"\nStep 5: Saving DBSCAN results...")
            if not self.save_dbscan_clusters():
                return {'status': 'FAILED', 'error': 'DBSCAN save failed'}
            
            # Step 6: Run HDBSCAN
            logger.info(f"\nStep 6: Running HDBSCAN clustering...")
            if not self.run_hdbscan():
                return {'status': 'FAILED', 'error': 'HDBSCAN clustering failed'}
            
            # Step 7: Save HDBSCAN results
            logger.info(f"\nStep 7: Saving HDBSCAN results...")
            if not self.save_hdbscan_clusters():
                return {'status': 'FAILED', 'error': 'HDBSCAN save failed'}
            
            # Step 8: Assign risk labels
            logger.info(f"\nStep 8: Assigning risk labels to earthquakes...")
            if not self.update_risk_labels():
                logger.warning("Risk label assignment failed, continuing...")
            
            # Step 9: Generate summary
            logger.info(f"\nStep 9: Generating summary statistics...")
            summary = self.get_summary_statistics()
            
            logger.info(f"\n" + "="*70)
            logger.info(f"CLUSTERING PIPELINE SUMMARY")
            logger.info(f"="*70)
            logger.info(f"Total Records Processed: {summary['total_records']:,}")
            logger.info(f"\nDBSCAN Results:")
            logger.info(f"  - Number of clusters: {summary['dbscan']['n_clusters']}")
            logger.info(f"  - Number of noise points: {summary['dbscan']['n_noise']}")
            logger.info(f"\nHDBSCAN Results:")
            logger.info(f"  - Number of clusters: {summary['hdbscan']['n_clusters']}")
            logger.info(f"  - Number of noise points: {summary['hdbscan']['n_noise']}")
            logger.info(f"="*70)
            
            return {
                'status': 'SUCCESS',
                'summary': summary,
                'total_records': summary['total_records']
            }
        
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            return {'status': 'FAILED', 'error': str(e)}


def run_earthquake_clustering(**context) -> Dict:
    """
    Airflow task function to run the earthquake clustering pipeline.
    
    Args:
        context: Airflow context object
    
    Returns:
        Dictionary with pipeline results
    """
    logger.info(f"Starting earthquake clustering task...")
    
    # Create clusterer instance
    clusterer = HDBSCANEarthquakeClusterer(
        postgres_host='postgres',
        postgres_user='postgres',
        postgres_password='seismo123',
        postgres_db='seismo_sphere',
        dbscan_eps=0.5,
        dbscan_min_samples=5,
        hdbscan_min_cluster_size=10,
        srid=4326,
        batch_size=1000
    )
    
    # Run pipeline
    result = clusterer.run_full_pipeline()
    
    # Push result to XCom for downstream tasks
    if 'ti' in context:
        context['ti'].xcom_push(key='clustering_result', value=result)
    
    logger.info(f"Earthquake clustering task completed with status: {result.get('status')}")
    
    return result
