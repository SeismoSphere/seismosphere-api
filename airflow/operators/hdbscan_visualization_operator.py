import polars as pl
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.colors import ListedColormap, Normalize
import seaborn as sns
import folium
from folium import plugins
import contextily as ctx

logger = logging.getLogger(__name__)

class ClusteringVisualizer:
    POSTGRES_CONFIG = {
        'host': 'postgres',
        'user': 'postgres',
        'password': 'seismo123',
        'database': 'seismo_sphere',
        'port': 5432
    }
    
    OUTPUT_DIR = Path('/opt/airflow/data/bigdata')
    DPI = 300
    FONT_SIZE = 10
    
    REGION_PATTERNS = {
        'Sumatra': {'lat_range': (-6, 6), 'lon_range': (95, 105)},
        'Java': {'lat_range': (-7, -5), 'lon_range': (105, 115)},
        'Philippines': {'lat_range': (5, 20), 'lon_range': (120, 130)},
        'Taiwan': {'lat_range': (20, 25), 'lon_range': (119, 122)},
        'Banda': {'lat_range': (-8, -4), 'lon_range': (125, 135)},
        'Sulawesi': {'lat_range': (-3, 3), 'lon_range': (119, 125)},
        'Irian': {'lat_range': (-5, 0), 'lon_range': (130, 140)},
        'NorthernJava': {'lat_range': (-4, -2), 'lon_range': (110, 120)},
        'SouthernSumatra': {'lat_range': (-8, -4), 'lon_range': (100, 108)},
        'TaiwanStrait': {'lat_range': (22, 26), 'lon_range': (119, 122)},
    }
    
    def __init__(self):
        self.output_dir = self.OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"ClusteringVisualizer initialized with output dir: {self.output_dir}")
        self.conn = None
        self.cursor = None
        self.dbscan_data = None
        self.hdbscan_data = None
        self.cluster_summaries = {}
    
    def connect_postgres(self) -> bool:
        try:
            self.conn = psycopg2.connect(**self.POSTGRES_CONFIG)
            self.cursor = self.conn.cursor()
            logger.info("Successfully connected to PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
    
    def disconnect_postgres(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Disconnected from PostgreSQL")
    
    def read_cluster_results(self) -> Tuple[Optional[pl.DataFrame], Optional[pl.DataFrame]]:
        try:
            if not self.conn:
                return None, None
            
            dbscan_query = """
            SELECT id, latitude, longitude, magnitude, depth, cluster_id, clustering_timestamp
            FROM earthquakes_dbscan_clusters
            ORDER BY cluster_id, id
            """
            dbscan_df = pl.read_database(dbscan_query, connection=self.conn)
            logger.info(f"Read {len(dbscan_df)} DBSCAN cluster points")
            
            hdbscan_query = """
            SELECT id, latitude, longitude, magnitude, depth, cluster_id, hdbscan_probability, risk_label, clustering_timestamp
            FROM earthquakes_hdbscan_clusters
            ORDER BY cluster_id, id
            """
            hdbscan_df = pl.read_database(hdbscan_query, connection=self.conn)
            logger.info(f"Read {len(hdbscan_df)} HDBSCAN cluster points with risk labels")
            
            if 'hdbscan_probability' in hdbscan_df.columns:
                hdbscan_df = hdbscan_df.rename({'hdbscan_probability': 'probability'})
            
            self.dbscan_data = dbscan_df
            self.hdbscan_data = hdbscan_df
            
            return dbscan_df, hdbscan_df
            
        except Exception as e:
            logger.error(f"Error reading cluster results: {e}")
            return None, None
    
    def _get_region(self, lat: float, lon: float) -> str:
        for region, bounds in self.REGION_PATTERNS.items():
            lat_range = bounds['lat_range']
            lon_range = bounds['lon_range']
            
            if (lat_range[0] <= lat <= lat_range[1] and 
                lon_range[0] <= lon <= lon_range[1]):
                return region
        
        if lat > 15:
            return 'Northern'
        elif lat < -5:
            return 'Southern'
        else:
            return 'Central'
    
    def _get_magnitude_level(self, magnitude: float) -> str:
        if magnitude < 4.0:
            return 'Low'
        elif magnitude < 5.5:
            return 'Medium'
        else:
            return 'High'
    
    def _get_depth_level(self, depth: float) -> str:
        if depth < 70:
            return 'Shallow'
        elif depth < 300:
            return 'Moderate'
        else:
            return 'Deep'
    
    def assign_cluster_names(self, df: pl.DataFrame, algorithm: str) -> Dict[int, str]:
        cluster_names = {}
        
        clusters_grouped = df.group_by('cluster_id').agg([
            pl.col('latitude').mean().alias('center_lat'),
            pl.col('longitude').mean().alias('center_lon'),
            pl.col('magnitude').mean().alias('avg_mag'),
            pl.col('magnitude').max().alias('max_mag'),
            pl.col('depth').mean().alias('avg_depth'),
            pl.col('id').count().alias('count')
        ])
        
        for row in clusters_grouped.iter_rows(named=True):
            cluster_id = row['cluster_id']
            
            if algorithm == 'HDBSCAN' and cluster_id == -1:
                cluster_names[-1] = 'Noise_Unclassified'
                continue
            
            center_lat = row['center_lat']
            center_lon = row['center_lon']
            avg_mag = row['avg_mag']
            avg_depth = row['avg_depth']
            
            region = self._get_region(center_lat, center_lon)
            mag_level = self._get_magnitude_level(avg_mag)
            depth_level = self._get_depth_level(avg_depth)
            
            if algorithm == 'DBSCAN':
                cluster_name_id = f"C{cluster_id}"
            else:
                cluster_name_id = f"C{cluster_id}"
            
            cluster_name = f"{region}_{mag_level}_{depth_level}_{cluster_name_id}"
            cluster_names[cluster_id] = cluster_name
            
            logger.info(f"{algorithm} Cluster {cluster_id}: {cluster_name} "
                       f"(n={row['count']}, mag={avg_mag:.2f}, depth={avg_depth:.1f}km)")
        
        return cluster_names
    
    def _get_distinct_colors(self, n_colors: int) -> List[Tuple[float, float, float]]:
        if n_colors <= 10:
            cmap = plt.cm.get_cmap('tab10')
            colors = [cmap(i % 10) for i in range(n_colors)]
        elif n_colors <= 20:
            cmap = plt.cm.get_cmap('tab20')
            colors = [cmap(i % 20) for i in range(n_colors)]
        else:
            colors = plt.cm.get_cmap('hsv')(np.linspace(0, 0.95, n_colors))
        
        return colors
    
    def create_heatmap_dbscan(self) -> Path:
        if self.dbscan_data is None or len(self.dbscan_data) == 0:
            logger.warning("No DBSCAN data available")
            return None
        
        try:
            df = self.dbscan_data.to_pandas()
            cluster_names = self.assign_cluster_names(self.dbscan_data, 'DBSCAN')
            
            fig, ax = plt.subplots(figsize=(16, 12), dpi=self.DPI)
            
            unique_clusters = sorted(df['cluster_id'].unique())
            n_clusters = len(unique_clusters)
            colors = self._get_distinct_colors(n_clusters)
            
            from pyproj import Transformer
            transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
            x, y = transformer.transform(df['longitude'].values, df['latitude'].values)
            df['x_mercator'] = x
            df['y_mercator'] = y
            
            for idx, cluster_id in enumerate(unique_clusters):
                cluster_data = df[df['cluster_id'].astype(int) == int(cluster_id)]
                cluster_name = cluster_names.get(cluster_id, f"Cluster {cluster_id}")
                
                sizes = (cluster_data['magnitude'] - cluster_data['magnitude'].min() + 1) * 30
                
                scatter = ax.scatter(
                    cluster_data['x_mercator'],
                    cluster_data['y_mercator'],
                    s=sizes,
                    c=[colors[idx]],
                    alpha=0.7,
                    edgecolors='black',
                    linewidth=0.3,
                    label=cluster_name,
                    zorder=5
                )
            
            try:
                ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, 
                               attribution_size=8, zorder=1)
            except Exception as e:
                logger.warning(f"Could not add map tiles: {e}")
            
            ax.set_xlabel('Longitude', fontsize=self.FONT_SIZE + 2, fontweight='bold')
            ax.set_ylabel('Latitude', fontsize=self.FONT_SIZE + 2, fontweight='bold')
            ax.set_title('DBSCAN Earthquake Clustering Heatmap - Indonesia\n(Size: Magnitude, Background: OpenStreetMap)', 
                        fontsize=self.FONT_SIZE + 4, fontweight='bold', pad=20)
            
            ax.legend(loc='best', fontsize=self.FONT_SIZE, title='Clusters', 
                     title_fontsize=self.FONT_SIZE + 1, framealpha=0.95)
            
            plt.tight_layout()
            
            output_path = self.output_dir / 'clustering_dbscan_heatmap.png'
            plt.savefig(output_path, dpi=self.DPI, bbox_inches='tight')
            logger.info(f"DBSCAN heatmap with map background saved: {output_path}")
            
            plt.close()
            return output_path
            
        except Exception as e:
            logger.error(f"Error creating DBSCAN heatmap: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def create_heatmap_hdbscan(self, exclude_noise: bool = False) -> Path:
        if self.hdbscan_data is None or len(self.hdbscan_data) == 0:
            logger.warning("No HDBSCAN data available")
            return None
        
        try:
            df = self.hdbscan_data.to_pandas()
            cluster_names = self.assign_cluster_names(self.hdbscan_data, 'HDBSCAN')
            
            fig, ax = plt.subplots(figsize=(14, 10), dpi=self.DPI)
            
            from pyproj import Transformer
            transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
            x, y = transformer.transform(df['longitude'].values, df['latitude'].values)
            df['x_mercator'] = x
            df['y_mercator'] = y
            
            noise_mask = df['cluster_id'].astype(int) == -1
            noise_data = df[noise_mask]
            clustered_data = df[~noise_mask]
            
            if exclude_noise:
                plot_data = clustered_data
                title_suffix = "(Clustered Points Only)"
            else:
                plot_data = df
                title_suffix = "(Including Noise)"
            
            if len(clustered_data) > 0:
                unique_clusters = sorted([int(c) for c in clustered_data['cluster_id'].unique()])
                n_clusters = len(unique_clusters)
                cmap = plt.cm.get_cmap('tab20b' if n_clusters > 10 else 'tab20')
                norm = Normalize(vmin=0, vmax=n_clusters - 1)
                
                for idx, cluster_id in enumerate(unique_clusters):
                    cluster_data = clustered_data[clustered_data['cluster_id'] == cluster_id]
                    cluster_name = cluster_names.get(cluster_id, f"Cluster {cluster_id}")
                    
                    sizes = (cluster_data['magnitude'] - cluster_data['magnitude'].min() + 1) * 30
                    
                    alpha_values = (cluster_data['probability'] * 0.4 + 0.3).values
                    sizes_values = sizes.values
                    
                    for i, (_, row) in enumerate(cluster_data.iterrows()):
                        ax.scatter(
                            row['x_mercator'],
                            row['y_mercator'],
                            s=sizes_values[i],
                            c=[cmap(norm(idx))],
                            alpha=alpha_values[i],
                            edgecolors='black',
                            linewidth=0.3,
                            zorder=5
                        )
            
            if not exclude_noise and len(noise_data) > 0:
                sizes = (noise_data['magnitude'] - noise_data['magnitude'].min() + 1) * 30
                ax.scatter(
                    noise_data['x_mercator'],
                    noise_data['y_mercator'],
                    s=sizes,
                    c='gray',
                    marker='o',
                    alpha=0.5,
                    edgecolors='black',
                    linewidth=0.3,
                    zorder=5
                )
            
            try:
                ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, 
                               attribution_size=8, zorder=1)
            except Exception as e:
                logger.warning(f"Could not add map tiles: {e}")
            
            ax.set_xlabel('Longitude', fontsize=self.FONT_SIZE + 2, fontweight='bold')
            ax.set_ylabel('Latitude', fontsize=self.FONT_SIZE + 2, fontweight='bold')
            ax.set_title(f'HDBSCAN Earthquake Clustering Heatmap - Indonesia {title_suffix}\n(Size: Magnitude, Transparency: Probability, Background: OpenStreetMap)', 
                        fontsize=self.FONT_SIZE + 3, fontweight='bold', pad=20)
            
            plt.tight_layout()
            
            if exclude_noise:
                output_path = self.output_dir / 'clustering_hdbscan_heatmap_no_noise.png'
            else:
                output_path = self.output_dir / 'clustering_hdbscan_heatmap.png'
            
            plt.savefig(output_path, dpi=self.DPI, bbox_inches='tight')
            logger.info(f"HDBSCAN heatmap with map background saved: {output_path}")
            
            plt.close()
            return output_path
            
        except Exception as e:
            logger.error(f"Error creating HDBSCAN heatmap: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def create_folium_map_dbscan(self) -> Path:
        if self.dbscan_data is None or len(self.dbscan_data) == 0:
            logger.warning("No DBSCAN data available for folium map")
            return None
        
        try:
            df = self.dbscan_data.to_pandas()
            cluster_names = self.assign_cluster_names(self.dbscan_data, 'DBSCAN')
            
            center_lat = df['latitude'].mean()
            center_lon = df['longitude'].mean()
            
            m = folium.Map(
                location=[center_lat, center_lon],
                zoom_start=5,
                tiles='OpenStreetMap'
            )
            
            unique_clusters = sorted(df['cluster_id'].unique())
            n_clusters = len(unique_clusters)
            colors = self._get_distinct_colors(n_clusters)
            color_map = {cluster_id: colors[idx] for idx, cluster_id in enumerate(unique_clusters)}
            
            for idx, row in df.iterrows():
                cluster_id = row['cluster_id']
                cluster_name = cluster_names.get(cluster_id, f"Cluster {cluster_id}")
                color = color_map[cluster_id]
                
                radius = (row['magnitude'] - df['magnitude'].min() + 1) * 5 * 1000  # convert to meters
                
                popup_text = f"""
                <b>{cluster_name}</b><br>
                Magnitude: {row['magnitude']:.2f}<br>
                Depth: {row['depth']:.2f} km<br>
                Lat: {row['latitude']:.4f}<br>
                Lon: {row['longitude']:.4f}
                """
                
                folium.CircleMarker(
                    location=[row['latitude'], row['longitude']],
                    radius=5,
                    popup=folium.Popup(popup_text, max_width=300),
                    tooltip=cluster_name,
                    color=color,
                    fill=True,
                    fillColor=color,
                    fillOpacity=0.6,
                    weight=2
                ).add_to(m)
            
            legend_html = '''
            <div style="position: fixed; 
                     bottom: 50px; right: 50px; width: 250px; height: auto;
                     background-color: white; z-index:9999; font-size:14px;
                     border:2px solid grey; border-radius: 5px; padding: 10px">
            <p style="margin: 0 0 10px 0; font-weight: bold;">DBSCAN Clusters</p>
            '''
            
            for cluster_id in sorted(unique_clusters):
                cluster_name = cluster_names.get(cluster_id, f"Cluster {cluster_id}")
                color = color_map[cluster_id]
                legend_html += f'<p style="margin: 5px 0;"><span style="background-color: {color}; padding: 3px 10px; border-radius: 3px;">&nbsp;</span> {cluster_name}</p>'
            
            legend_html += '</div>'
            m.get_root().html.add_child(folium.Element(legend_html))
            
            output_path = self.output_dir / 'clustering_dbscan_map.html'
            m.save(str(output_path))
            logger.info(f"DBSCAN folium map saved: {output_path}")
            
            return output_path
            
        except Exception as e:
            logger.error(f"Error creating DBSCAN folium map: {e}")
            return None
    

    def create_folium_map_hdbscan(self) -> Path:
        if self.hdbscan_data is None or len(self.hdbscan_data) == 0:
            logger.warning("No HDBSCAN data available for folium map")
            return None
        
        try:
            df = self.hdbscan_data.to_pandas()
            cluster_names = self.assign_cluster_names(self.hdbscan_data, 'HDBSCAN')
            
            center_lat = df['latitude'].mean()
            center_lon = df['longitude'].mean()
            
            m = folium.Map(
                location=[center_lat, center_lon],
                zoom_start=5,
                tiles='OpenStreetMap'
            )
            
            folium.TileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
                           attr='Esri',
                           name='Satellite',
                           overlay=True).add_to(m)
            
            unique_clusters = sorted([int(c) for c in df['cluster_id'].unique() if int(c) != -1])
            n_clusters = len(unique_clusters)
            colors = self._get_distinct_colors(n_clusters)
            color_map = {cluster_id: colors[idx] for idx, cluster_id in enumerate(unique_clusters)}
            color_map[-1] = 'gray'  
            
            for idx, row in df.iterrows():
                cluster_id = int(row['cluster_id'])
                cluster_name = cluster_names.get(cluster_id, f"Cluster {cluster_id}")
                color = color_map[cluster_id]
                
                alpha = row.get('probability', 0.5) * 0.4 + 0.3
                
                radius = (row['magnitude'] - df['magnitude'].min() + 1) * 3
                
                popup_text = f"""
                <b>{cluster_name}</b><br>
                Magnitude: {row['magnitude']:.2f}<br>
                Depth: {row['depth']:.2f} km<br>
                Probability: {row.get('probability', 0):.2%}<br>
                Lat: {row['latitude']:.4f}<br>
                Lon: {row['longitude']:.4f}
                """
                
                marker_color = 'red' if cluster_id == -1 else 'blue'
                marker_icon = 'x' if cluster_id == -1 else 'circle'
                
                folium.CircleMarker(
                    location=[row['latitude'], row['longitude']],
                    radius=radius,
                    popup=folium.Popup(popup_text, max_width=300),
                    tooltip=cluster_name,
                    color=color,
                    fill=True,
                    fillColor=color,
                    fillOpacity=0.6,
                    weight=2 if cluster_id != -1 else 1
                ).add_to(m)
            
            legend_html = '''
            <div style="position: fixed; 
                     bottom: 50px; right: 50px; width: 250px; height: auto;
                     background-color: white; z-index:9999; font-size:14px;
                     border:2px solid grey; border-radius: 5px; padding: 10px;
                     max-height: 400px; overflow-y: auto;">
            <p style="margin: 0 0 10px 0; font-weight: bold;">HDBSCAN Clusters</p>
            '''
            
            for cluster_id in unique_clusters:
                cluster_name = cluster_names.get(cluster_id, f"Cluster {cluster_id}")
                color = color_map[cluster_id]
                count = len(df[df['cluster_id'].astype(int) == int(cluster_id)])
                legend_html += f'<p style="margin: 5px 0;"><span style="background-color: {color}; padding: 3px 10px; border-radius: 3px;">&nbsp;</span> {cluster_name} ({count})</p>'
            
            noise_count = len(df[df['cluster_id'].astype(int) == -1])
            if noise_count > 0:
                legend_html += f'<p style="margin: 5px 0;"><span style="background-color: gray; padding: 3px 10px; border-radius: 3px;">&nbsp;</span> Noise Points ({noise_count})</p>'
            
            legend_html += '</div>'
            m.get_root().html.add_child(folium.Element(legend_html))
            
            folium.LayerControl().add_to(m)
            
            output_path = self.output_dir / 'clustering_hdbscan_map.html'
            m.save(str(output_path))
            logger.info(f"HDBSCAN folium map saved: {output_path}")
            
            return output_path
            
        except Exception as e:
            logger.error(f"Error creating HDBSCAN folium map: {e}")
            return None

    def create_risk_profiling_summary_figure(self) -> Path:
        if self.hdbscan_data is None or len(self.hdbscan_data) == 0:
            logger.warning("No HDBSCAN data available for risk profiling summary")
            return None

        try:
            df = self.hdbscan_data.to_pandas()

            if 'risk_label' not in df.columns:
                logger.warning("Risk label column not found in HDBSCAN data")
                return None

            df['risk_label'] = df['risk_label'].fillna('UNKNOWN')
            risk_order = ['VERY_HIGH', 'HIGH', 'MEDIUM', 'LOW']
            label_colors = {
                'VERY_HIGH': '#7f0000',
                'HIGH': '#d94801',
                'MEDIUM': '#fdae6b',
                'LOW': '#31a354',
                'UNKNOWN': '#636363'
            }

            total_records = len(df)
            risk_counts = {label: int((df['risk_label'] == label).sum()) for label in risk_order}
            unknown_count = int((df['risk_label'] == 'UNKNOWN').sum())
            if unknown_count > 0:
                risk_counts['UNKNOWN'] = unknown_count

            risk_percentages = {
                label: (count / total_records * 100) if total_records else 0.0
                for label, count in risk_counts.items()
            }

            fig, ax_bar = plt.subplots(figsize=(12, 8), dpi=self.DPI)
            fig.patch.set_facecolor('white')
            ax_bar.set_facecolor('white')

            ordered_counts = [risk_counts.get(label, 0) for label in risk_order]
            ordered_percentages = [risk_percentages.get(label, 0.0) for label in risk_order]
            bars = ax_bar.bar(
                risk_order,
                ordered_counts,
                color=[label_colors[label] for label in risk_order],
                edgecolor='#2b2b2b',
                linewidth=1.0
            )

            for bar, count, percentage in zip(bars, ordered_counts, ordered_percentages):
                ax_bar.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + max(ordered_counts) * 0.02 if ordered_counts else 0.5,
                    f'{count}\n({percentage:.1f}%)',
                    ha='center',
                    va='bottom',
                    fontsize=11,
                    fontweight='bold'
                )

            ax_bar.set_title('Distribusi Label Risiko', fontsize=self.FONT_SIZE + 7, fontweight='bold', pad=16)
            ax_bar.set_xlabel('Label Risiko', fontsize=self.FONT_SIZE + 2, fontweight='bold')
            ax_bar.set_ylabel('Jumlah Data', fontsize=self.FONT_SIZE + 2, fontweight='bold')
            ax_bar.grid(axis='y', alpha=0.25, linestyle='--')
            ax_bar.spines['top'].set_visible(False)
            ax_bar.spines['right'].set_visible(False)

            if unknown_count > 0:
                ax_bar.text(
                    0.98,
                    0.95,
                    f'UNKNOWN: {unknown_count} ({risk_percentages.get("UNKNOWN", 0.0):.1f}%)',
                    transform=ax_bar.transAxes,
                    ha='right',
                    va='top',
                    fontsize=10,
                    color='#444444'
                )

            fig.suptitle(
                'Distribusi Label Risiko',
                fontsize=self.FONT_SIZE + 10,
                fontweight='bold',
                y=0.98
            )

            plt.tight_layout(rect=[0, 0, 1, 0.95])

            output_path = self.output_dir / 'risk_profiling_summary.png'
            plt.savefig(output_path, dpi=self.DPI, bbox_inches='tight')
            logger.info(f"Risk profiling summary figure saved: {output_path}")

            plt.close()
            return output_path

        except Exception as e:
            logger.error(f"Error creating risk profiling summary figure: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def create_cluster_summary_table(self) -> bool:
        if not self.conn:
            logger.error("No database connection")
            return False
        
        try:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS cluster_summaries (
                id SERIAL PRIMARY KEY,
                cluster_id INTEGER NOT NULL,
                algorithm VARCHAR(50) NOT NULL,
                cluster_name VARCHAR(255),
                point_count INTEGER,
                avg_magnitude FLOAT,
                avg_depth FLOAT,
                min_lat FLOAT,
                max_lat FLOAT,
                min_lon FLOAT,
                max_lon FLOAT,
                center_lat FLOAT,
                center_lon FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (cluster_id, algorithm)
            )
            """
            self.cursor.execute(create_table_query)
            logger.info("cluster_summaries table created/verified")
            
            self.cursor.execute("DELETE FROM cluster_summaries")
            
            if self.dbscan_data is not None and len(self.dbscan_data) > 0:
                dbscan_names = self.assign_cluster_names(self.dbscan_data, 'DBSCAN')
                
                clusters_stats = self.dbscan_data.group_by('cluster_id').agg([
                    pl.col('latitude').mean().alias('center_lat'),
                    pl.col('latitude').min().alias('min_lat'),
                    pl.col('latitude').max().alias('max_lat'),
                    pl.col('longitude').mean().alias('center_lon'),
                    pl.col('longitude').min().alias('min_lon'),
                    pl.col('longitude').max().alias('max_lon'),
                    pl.col('magnitude').mean().alias('avg_magnitude'),
                    pl.col('depth').mean().alias('avg_depth'),
                    pl.col('id').count().alias('point_count')
                ])
                
                dbscan_records = []
                for row in clusters_stats.iter_rows(named=True):
                    cluster_id = row['cluster_id']
                    cluster_name = dbscan_names.get(cluster_id, f"DBSCAN_{cluster_id}")
                    
                    dbscan_records.append((
                        cluster_id,
                        'DBSCAN',
                        cluster_name,
                        int(row['point_count']),
                        float(row['avg_magnitude']),
                        float(row['avg_depth']),
                        float(row['min_lat']),
                        float(row['max_lat']),
                        float(row['min_lon']),
                        float(row['max_lon']),
                        float(row['center_lat']),
                        float(row['center_lon'])
                    ))
                
                if dbscan_records:
                    insert_query = """
                    INSERT INTO cluster_summaries 
                    (cluster_id, algorithm, cluster_name, point_count, avg_magnitude, avg_depth,
                     min_lat, max_lat, min_lon, max_lon, center_lat, center_lon)
                    VALUES %s
                    """
                    execute_values(self.cursor, insert_query, dbscan_records)
                    logger.info(f"Inserted {len(dbscan_records)} DBSCAN cluster summaries")
            
            if self.hdbscan_data is not None and len(self.hdbscan_data) > 0:
                hdbscan_names = self.assign_cluster_names(self.hdbscan_data, 'HDBSCAN')
                
                clusters_stats = self.hdbscan_data.group_by('cluster_id').agg([
                    pl.col('latitude').mean().alias('center_lat'),
                    pl.col('latitude').min().alias('min_lat'),
                    pl.col('latitude').max().alias('max_lat'),
                    pl.col('longitude').mean().alias('center_lon'),
                    pl.col('longitude').min().alias('min_lon'),
                    pl.col('longitude').max().alias('max_lon'),
                    pl.col('magnitude').mean().alias('avg_magnitude'),
                    pl.col('depth').mean().alias('avg_depth'),
                    pl.col('id').count().alias('point_count')
                ])
                
                hdbscan_records = []
                for row in clusters_stats.iter_rows(named=True):
                    cluster_id = row['cluster_id']
                    cluster_name = hdbscan_names.get(cluster_id, 
                                                      'Noise' if cluster_id == -1 else f"HDBSCAN_{cluster_id}")
                    
                    hdbscan_records.append((
                        cluster_id,
                        'HDBSCAN',
                        cluster_name,
                        int(row['point_count']),
                        float(row['avg_magnitude']),
                        float(row['avg_depth']),
                        float(row['min_lat']),
                        float(row['max_lat']),
                        float(row['min_lon']),
                        float(row['max_lon']),
                        float(row['center_lat']),
                        float(row['center_lon'])
                    ))
                
                if hdbscan_records:
                    execute_values(self.cursor, insert_query, hdbscan_records)
                    logger.info(f"Inserted {len(hdbscan_records)} HDBSCAN cluster summaries")
            
            self.conn.commit()
            logger.info("Cluster summaries table updated successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error creating cluster summary table: {e}")
            self.conn.rollback()
            return False
    
    def run_full_visualization_pipeline(self) -> Dict[str, any]:
        logger.info("="*70)
        logger.info("EARTHQUAKE CLUSTER VISUALIZATION PIPELINE")
        logger.info("="*70)
        
        summary = {
            'status': 'failed',
            'dbscan_heatmap': None,
            'dbscan_map': None,
            'hdbscan_heatmap': None,
            'hdbscan_heatmap_no_noise': None,
            'hdbscan_map': None,
            'risk_profiling_summary': None,
            'cluster_summaries_created': False,
            'message': ''
        }
        
        try:
            if not self.connect_postgres():
                summary['message'] = 'Failed to connect to PostgreSQL'
                return summary
            
            dbscan_df, hdbscan_df = self.read_cluster_results()
            
            if dbscan_df is None and hdbscan_df is None:
                summary['message'] = 'No cluster data found'
                return summary
            
            if hdbscan_df is None or len(hdbscan_df) == 0:
                summary['message'] = 'No HDBSCAN cluster data found'
                return summary
            
            logger.info("\nGenerating HDBSCAN heatmaps...")
            
            logger.info("Creating heatmap WITH noise points...")
            hdbscan_path = self.create_heatmap_hdbscan(exclude_noise=False)
            if hdbscan_path:
                summary['hdbscan_heatmap'] = str(hdbscan_path)
                logger.info(f"✓ HDBSCAN heatmap (with noise) created: {hdbscan_path}")
            else:
                logger.warning("Failed to create HDBSCAN heatmap with noise")
            
            logger.info("Creating heatmap WITHOUT noise points...")
            hdbscan_path_no_noise = self.create_heatmap_hdbscan(exclude_noise=True)
            if hdbscan_path_no_noise:
                summary['hdbscan_heatmap_no_noise'] = str(hdbscan_path_no_noise)
                logger.info(f"✓ HDBSCAN heatmap (no noise) created: {hdbscan_path_no_noise}")
            else:
                logger.warning("Failed to create HDBSCAN heatmap without noise")
            
            logger.info("Generating HDBSCAN interactive map...")
            hdbscan_map_path = self.create_folium_map_hdbscan()
            if hdbscan_map_path:
                summary['hdbscan_map'] = str(hdbscan_map_path)
                logger.info(f"✓ HDBSCAN map created: {hdbscan_map_path}")
            else:
                logger.warning("Failed to create HDBSCAN map (continuing...)")

            logger.info("Creating risk profiling summary figure...")
            risk_profile_path = self.create_risk_profiling_summary_figure()
            if risk_profile_path:
                summary['risk_profiling_summary'] = str(risk_profile_path)
                logger.info(f"✓ Risk profiling summary created: {risk_profile_path}")
            else:
                logger.warning("Failed to create risk profiling summary figure")
            
            logger.info("\nCreating cluster summary table...")
            if self.create_cluster_summary_table():
                summary['cluster_summaries_created'] = True
                logger.info("✓ Cluster summaries table created/updated")
            
            summary['status'] = 'success'
            summary['message'] = 'Visualization pipeline completed successfully'
            
            logger.info("\n" + "="*70)
            logger.info("VISUALIZATION PIPELINE COMPLETE!")
            logger.info("="*70)
            
            return summary
            
        except Exception as e:
            logger.error(f"Error in visualization pipeline: {e}")
            summary['message'] = str(e)
            return summary
        
        finally:
            self.disconnect_postgres()


def run_visualization_task(**context):
    try:
        logger.info("Starting earthquake cluster visualization task...")
        
        visualizer = ClusteringVisualizer()
        result = visualizer.run_full_visualization_pipeline()
        
        context['ti'].xcom_push(key='visualization_status', value=result['status'])
        context['ti'].xcom_push(key='dbscan_heatmap', value=result['dbscan_heatmap'])
        context['ti'].xcom_push(key='dbscan_map', value=result['dbscan_map'])
        context['ti'].xcom_push(key='hdbscan_heatmap', value=result['hdbscan_heatmap'])
        context['ti'].xcom_push(key='hdbscan_heatmap_no_noise', value=result['hdbscan_heatmap_no_noise'])
        context['ti'].xcom_push(key='hdbscan_map', value=result['hdbscan_map'])
        context['ti'].xcom_push(key='risk_profiling_summary', value=result['risk_profiling_summary'])
        context['ti'].xcom_push(key='cluster_summaries_created', value=result['cluster_summaries_created'])
        context['ti'].xcom_push(key='visualization_message', value=result['message'])
        
        if result['status'] == 'success':
            logger.info("Visualization task completed successfully")
            return result
        else:
            raise Exception(f"Visualization failed: {result['message']}")
    
    except Exception as e:
        logger.error(f"Error in visualization task: {e}")
        context['ti'].xcom_push(key='visualization_status', value='failed')
        context['ti'].xcom_push(key='visualization_error', value=str(e))
        raise
