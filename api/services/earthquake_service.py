from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

import os
import polars as pl
import psycopg2

class EarthquakeService:
    def __init__(self) -> None:
        self.base_dir = Path(__file__).resolve().parents[2]
        self.data_dir = self.base_dir / "data" / "bigdata"
        self.excluded_image_names = {
            "clustering_hdbscan_heatmap_no_noise.png",
            "clustering_hdbscan_heatmap.png",
        }
        self.postgres_config = {
            "host": os.getenv("POSTGRES_HOST", "postgres"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "seismo123"),
            "dbname": os.getenv("POSTGRES_DB", "seismo_sphere"),
        }

    def _resolve_dataset_path(self, dataset: str) -> Path:
        normalized = dataset.lower().strip()
        if normalized in {"processed", "clean", "cleaned"}:
            return self.data_dir / "processed_earthquakes.parquet"
        if normalized in {"raw", "ingested"}:
            return self.data_dir / "raw_earthquakes.parquet"
        raise ValueError(f"Unsupported dataset: {dataset}")

    def _resolve_table_name(self, dataset: str) -> str:
        normalized = dataset.lower().strip()
        if normalized in {"processed", "clean", "cleaned"}:
            return "earthquakes"
        if normalized in {"raw", "ingested"}:
            return "earthquakes"
        if normalized in {"dbscan", "clusters_dbscan"}:
            return "earthquakes_dbscan_clusters"
        if normalized in {"hdbscan", "clusters_hdbscan"}:
            return "earthquakes_hdbscan_clusters"
        if normalized in {"cluster_summaries", "clusters_summary"}:
            return "cluster_summaries"
        if normalized in {"model_evaluation_results", "model_evaluations"}:
            return "model_evaluation_results"
        raise ValueError(f"Unsupported dataset: {dataset}")

    def _fetch_table(self, table_name: str) -> pl.DataFrame:
        query_map = {
            "earthquakes": """
                SELECT id, time, latitude, longitude, depth, magnitude, place,
                       datetime, location, nearest_event_km, event_density_100km,
                       seismic_zone, grid_cell_id, centroid_distance_km, spatial_risk_score,
                       created_at
                FROM earthquakes
            """,
            "earthquakes_dbscan_clusters": """
                SELECT id, datetime, latitude, longitude, magnitude, depth,
                       cluster_id, clustering_timestamp
                FROM earthquakes_dbscan_clusters
            """,
            "earthquakes_hdbscan_clusters": """
                SELECT id, datetime, latitude, longitude, magnitude, depth,
                       cluster_id, hdbscan_probability AS probability,
                       clustering_timestamp
                FROM earthquakes_hdbscan_clusters
            """,
            "cluster_summaries": """
                SELECT *
                FROM cluster_summaries
            """,
            "model_evaluation_results": """
                SELECT id, model_name, accuracy, precision, recall, f1_score,
                       test_samples, evaluation_timestamp
                FROM model_evaluation_results
            """,
        }

        query = query_map.get(table_name)
        if query is None:
            raise ValueError(f"Unsupported table: {table_name}")

        with psycopg2.connect(**self.postgres_config) as conn:
            frame = pl.read_database(query, connection=conn)

        return frame

    @lru_cache(maxsize=4)
    def _load_dataset(self, dataset: str) -> pl.DataFrame:
        table_name = self._resolve_table_name(dataset)

        try:
            frame = self._fetch_table(table_name)
        except Exception:
            dataset_path = self._resolve_dataset_path(dataset)
            if not dataset_path.exists():
                raise FileNotFoundError(f"Dataset not found: {dataset_path}")

            frame = pl.read_parquet(dataset_path)

        if "time" in frame.columns:
            try:
                frame = frame.with_columns(pl.col("time").cast(pl.Datetime, strict=False))
            except Exception:
                pass

        return frame

    @staticmethod
    def _to_records(frame: pl.DataFrame) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = frame.to_dicts()
        for record in records:
            for key, value in list(record.items()):
                if hasattr(value, "isoformat"):
                    record[key] = value.isoformat()
        return records

    def list_available_sources(self) -> List[str]:
        available_sources: List[str] = ["postgres:earthquakes"]

        for filename in ["processed_earthquakes.parquet", "raw_earthquakes.parquet"]:
            file_path = self.data_dir / filename
            if file_path.exists():
                available_sources.append(filename)

        return available_sources

    def _fetch_risk_distribution_summary(self) -> Optional[Dict[str, Any]]:
        query = "SELECT scope, total_records, distribution FROM risk_distribution_summary"

        try:
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    rows = cur.fetchall()
        except Exception:
            return None

        if not rows:
            return None

        return {
            scope: {"total_records": total_records, "distribution": distribution}
            for scope, total_records, distribution in rows
        }

    def _fetch_classification_split_summary(self) -> Optional[Dict[str, Any]]:
        query = """
            SELECT total_samples, train_count, train_percentage, test_count, test_percentage,
                   train_distribution, test_distribution, updated_at
            FROM classification_split_summary
            WHERE run_id = 'latest'
        """

        try:
            with psycopg2.connect(**self.postgres_config) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    row = cur.fetchone()
        except Exception:
            return None

        if not row:
            return None

        (
            total_samples, train_count, train_percentage, test_count, test_percentage,
            train_distribution, test_distribution, updated_at,
        ) = row

        return {
            "total_samples": total_samples,
            "train": {
                "count": train_count,
                "percentage": train_percentage,
                "distribution": train_distribution,
            },
            "test": {
                "count": test_count,
                "percentage": test_percentage,
                "distribution": test_distribution,
            },
            "updated_at": updated_at.isoformat() if hasattr(updated_at, "isoformat") else updated_at,
        }

    def get_summary(self, dataset: str = "processed") -> Dict[str, Any]:
        frame = self._load_dataset(dataset)

        summary: Dict[str, Any] = {
            "dataset": dataset,
            "total_records": frame.height,
            "available_columns": frame.columns,
        }

        if "id" in frame.columns:
            summary["unique_ids"] = int(frame.get_column("id").n_unique())

        if "time" in frame.columns and frame.height > 0:
            time_values = frame.get_column("time")
            time_min = time_values.min()
            time_max = time_values.max()
            summary["date_range"] = {
                "start": time_min.isoformat() if hasattr(time_min, "isoformat") else time_min,
                "end": time_max.isoformat() if hasattr(time_max, "isoformat") else time_max,
            }

        for column in ["magnitude", "depth"]:
            if column in frame.columns and frame.height > 0:
                column_values = frame.get_column(column)
                summary[column] = {
                    "min": float(column_values.min()),
                    "max": float(column_values.max()),
                    "mean": float(column_values.mean()),
                    "median": float(column_values.median()),
                }

        if "cluster_id" in frame.columns:
            summary["cluster_counts"] = (
                frame.group_by("cluster_id")
                .agg(pl.len().alias("count"))
                .sort("count", descending=True)
                .to_dicts()
            )

        risk_distribution = self._fetch_risk_distribution_summary()
        if risk_distribution:
            summary["risk_distribution"] = risk_distribution

        classification_split = self._fetch_classification_split_summary()
        if classification_split:
            summary["classification_split"] = classification_split

        return summary

    def list_earthquakes(
        self,
        dataset: str = "processed",
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> Dict[str, Any]:
        frame = self._load_dataset(dataset)

        sort_columns = [column for column in ["magnitude", "time"] if column in frame.columns]
        if sort_columns:
            frame = frame.sort(sort_columns, descending=[True] * len(sort_columns))

        total_records = frame.height
        page = frame.slice(offset, limit) if limit is not None else frame.slice(offset)

        return {
            "dataset": dataset,
            "total_records": total_records,
            "limit": limit,
            "offset": offset,
            "items": self._to_records(page),
        }

    def get_earthquake_by_id(self, earthquake_id: str, dataset: str = "processed") -> Optional[Dict[str, Any]]:
        frame = self._load_dataset(dataset)

        id_column = None
        for candidate in ["id", "earthquake_id", "event_id"]:
            if candidate in frame.columns:
                id_column = candidate
                break

        if id_column is None:
            return None

        matches = frame.filter(pl.col(id_column).cast(pl.Utf8, strict=False) == earthquake_id)
        if matches.height == 0:
            return None

        return self._to_records(matches.head(1))[0]

    def list_cluster_results(
        self,
        dataset: str = "hdbscan",
        limit: Optional[int] = None,
        offset: int = 0,
        exclude_noise: bool = False,
    ) -> Dict[str, Any]:
        frame = self._load_dataset(dataset)

        if exclude_noise and "cluster_id" in frame.columns:
            frame = frame.filter(pl.col("cluster_id") != -1)

        total_records = frame.height
        page = frame.slice(offset, limit) if limit is not None else frame.slice(offset)

        return {
            "dataset": dataset,
            "exclude_noise": exclude_noise,
            "total_records": total_records,
            "limit": limit,
            "offset": offset,
            "items": self._to_records(page),
        }

    def list_model_evaluations(self) -> Dict[str, Any]:
        frame = self._load_dataset("model_evaluation_results")
        return {
            "dataset": "model_evaluation_results",
            "total_records": frame.height,
            "items": self._to_records(frame),
        }

    def list_cluster_summaries(self) -> Dict[str, Any]:
        frame = self._load_dataset("cluster_summaries")
        return {
            "dataset": "cluster_summaries",
            "total_records": frame.height,
            "items": self._to_records(frame),
        }

    def list_visualization_images(self) -> List[Dict[str, str]]:
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {self.data_dir}")

        image_items: List[Dict[str, str]] = []
        for image_path in sorted(self.data_dir.glob("*.png")):
            if image_path.name in self.excluded_image_names:
                continue

            image_items.append(
                {
                    "name": image_path.name,
                    "title": image_path.stem.replace("_", " ").title(),
                }
            )

        return image_items

    def resolve_visualization_image_path(self, image_name: str) -> Path:
        if image_name in self.excluded_image_names:
            raise FileNotFoundError("Requested image is not available")

        candidate = (self.data_dir / image_name).resolve()
        base_resolved = self.data_dir.resolve()

        if base_resolved not in candidate.parents:
            raise ValueError("Invalid image path")

        if not candidate.exists() or not candidate.is_file():
            raise FileNotFoundError(f"Image not found: {image_name}")

        if candidate.suffix.lower() != ".png":
            raise ValueError("Unsupported image type")

        return candidate
