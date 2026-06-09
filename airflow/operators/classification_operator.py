import polars as pl
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple, List
import pickle
import json

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    confusion_matrix, classification_report, roc_auc_score
)

import matplotlib.pyplot as plt
import seaborn as sns

logger = logging.getLogger(__name__)


class EarthquakeClusterClassifier:
    POSTGRES_CONFIG = {
        'host': 'postgres',
        'user': 'postgres',
        'password': 'seismo123',
        'database': 'seismo_sphere',
        'port': 5432
    }
    
    OUTPUT_DIR = Path('/opt/airflow/data/bigdata')
    MODELS_DIR = Path('/opt/airflow/data/models')
    
    def __init__(self):
        self.output_dir = self.OUTPUT_DIR
        self.models_dir = self.MODELS_DIR
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"EarthquakeClusterClassifier initialized")
        logger.info(f"  Output dir: {self.output_dir}")
        logger.info(f"  Models dir: {self.models_dir}")
        
        self.conn = None
        self.rf_model = None
        self.xgb_model = None
        self.scaler = StandardScaler()
        self.label_encoder = LabelEncoder()
        self.results = {}
    
    def connect_postgres(self) -> bool:
        try:
            self.conn = psycopg2.connect(**self.POSTGRES_CONFIG)
            logger.info("Connected to PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
    
    def disconnect_postgres(self):
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from PostgreSQL")
    
    def read_training_data(self) -> Optional[pl.DataFrame]:
        try:
            if not self.conn:
                return None
            
            query = """
            SELECT 
                ec.id,
                ec.datetime,
                ec.latitude,
                ec.longitude,
                ec.magnitude,
                ec.depth,
                ec.cluster_id,
                ec.hdbscan_probability,
                ec.risk_label,
                e.nearest_event_km,
                e.event_density_100km,
                e.spatial_risk_score
            FROM earthquakes_hdbscan_clusters ec
            JOIN earthquakes e ON ec.id = e.id
            WHERE ec.risk_label IS NOT NULL
            ORDER BY ec.datetime DESC
            """
            
            df = pl.read_database(query, connection=self.conn)
            logger.info(f"Read {len(df):,} earthquake records for classification training")
            logger.info(f"  (Includes cluster-assigned AND noise points)")
            
            risk_dist = df.group_by('risk_label').agg(pl.col('id').count().alias('count')).sort('count', descending=True)
            logger.info("Risk label distribution:")
            for row in risk_dist.iter_rows(named=True):
                pct = (row['count'] / len(df)) * 100
                logger.info(f"  {row['risk_label']:12s}: {row['count']:6d} ({pct:5.1f}%)")
            
            return df
        
        except Exception as e:
            logger.error(f"Error reading training data: {e}")
            return None
    
    def prepare_features(self, df: pl.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        try:
            logger.info("Preparing features for classification...")
            
            feature_cols = [
                'magnitude',
                'depth',
                'nearest_event_km',
                'event_density_100km',
                'hdbscan_probability',
                'latitude',
                'longitude'
            ]
            
            X = df.select(feature_cols).to_numpy()
            
            y = df['risk_label'].to_numpy()
            y_encoded = self.label_encoder.fit_transform(y)
            
            X_scaled = self.scaler.fit_transform(X)
            
            logger.info(f"Features prepared:")
            logger.info(f"  Shape: {X_scaled.shape}")
            logger.info(f"  Features: {', '.join(feature_cols)}")
            logger.info(f"  Target classes: {list(self.label_encoder.classes_)}")
            
            return X_scaled, y_encoded
        
        except Exception as e:
            logger.error(f"Error preparing features: {e}")
            raise
    
    def train_random_forest(self, X_train: np.ndarray, y_train: np.ndarray) -> Dict:
        try:
            logger.info("Training Random Forest classifier...")
            
            self.rf_model = RandomForestClassifier(
                n_estimators=100,
                max_depth=15,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=42,
                n_jobs=-1,
                class_weight='balanced'
            )
            
            self.rf_model.fit(X_train, y_train)
            logger.info("✓ Random Forest training completed")
            
            feature_names = [
                'magnitude', 'depth', 'nearest_event_km',
                'event_density_100km', 'hdbscan_probability',
                'latitude', 'longitude'
            ]
            importances = self.rf_model.feature_importances_
            
            for fname, importance in sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True):
                logger.info(f"  {fname:25s}: {importance:.4f}")
            
            return {'model': self.rf_model, 'feature_importance': dict(zip(feature_names, importances))}
        
        except Exception as e:
            logger.error(f"Error training Random Forest: {e}")
            raise
    
    def train_xgboost(self, X_train: np.ndarray, y_train: np.ndarray) -> Dict:
        try:
            logger.info("Training XGBoost classifier...")
            
            self.xgb_model = XGBClassifier(
                n_estimators=100,
                max_depth=7,
                learning_rate=0.1,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                n_jobs=-1,
                scale_pos_weight=1,
                eval_metric='mlogloss'
            )
            
            self.xgb_model.fit(X_train, y_train)
            logger.info("✓ XGBoost training completed")
            
            feature_names = [
                'magnitude', 'depth', 'nearest_event_km',
                'event_density_100km', 'hdbscan_probability',
                'latitude', 'longitude'
            ]
            importances = self.xgb_model.feature_importances_
            
            for fname, importance in sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True):
                logger.info(f"  {fname:25s}: {importance:.4f}")
            
            return {'model': self.xgb_model, 'feature_importance': dict(zip(feature_names, importances))}
        
        except Exception as e:
            logger.error(f"Error training XGBoost: {e}")
            raise
    
    def evaluate_model(self, model, X_test: np.ndarray, y_test: np.ndarray, model_name: str) -> Dict:
        try:
            logger.info(f"\nEvaluating {model_name}...")
            
            y_pred = model.predict(X_test)
            
            accuracy = accuracy_score(y_test, y_pred)
            precision_weighted = precision_score(y_test, y_pred, average='weighted', zero_division=0)
            recall_weighted = recall_score(y_test, y_pred, average='weighted', zero_division=0)
            f1_weighted = f1_score(y_test, y_pred, average='weighted', zero_division=0)
            
            class_report = classification_report(
                y_test, y_pred,
                target_names=self.label_encoder.classes_,
                output_dict=True,
                zero_division=0
            )
            
            logger.info(f"{model_name} Results:")
            logger.info(f"  Accuracy:        {accuracy:.4f}")
            logger.info(f"  Precision (w):   {precision_weighted:.4f}")
            logger.info(f"  Recall (w):      {recall_weighted:.4f}")
            logger.info(f"  F1-Score (w):    {f1_weighted:.4f}")
            logger.info(f"\n  Per-Class Metrics:")
            
            for class_name in self.label_encoder.classes_:
                if class_name in class_report:
                    metrics = class_report[class_name]
                    count = int(metrics.get('support', 0))
                    prec = metrics.get('precision', 0)
                    rec = metrics.get('recall', 0)
                    f1_c = metrics.get('f1-score', 0)
                    logger.info(f"    {class_name:12s}: Precision={prec:.4f}, Recall={rec:.4f}, F1={f1_c:.4f}, Count={count}")
            
            all_labels = list(range(len(self.label_encoder.classes_)))
            cm = confusion_matrix(y_test, y_pred, labels=all_labels)
            
            return {
                'model_name': model_name,
                'accuracy': accuracy,
                'precision': precision_weighted,
                'recall': recall_weighted,
                'f1_score': f1_weighted,
                'confusion_matrix': cm,
                'y_pred': y_pred,
                'y_test': y_test,
                'classification_report': class_report
            }
        
        except Exception as e:
            logger.error(f"Error evaluating {model_name}: {e}")
            raise
    
    def create_comparison_plots(self, rf_results: Dict, xgb_results: Dict) -> Dict[str, Path]:
        try:
            logger.info("Creating comparison plots (separate)...")
            output_paths = {}
            
            fig, ax = plt.subplots(figsize=(12, 6), dpi=300)
            metrics = ['Accuracy', 'Precision', 'Recall', 'F1-Score']
            rf_values = [
                rf_results['accuracy'],
                rf_results['precision'],
                rf_results['recall'],
                rf_results['f1_score']
            ]
            xgb_values = [
                xgb_results['accuracy'],
                xgb_results['precision'],
                xgb_results['recall'],
                xgb_results['f1_score']
            ]
            
            x = np.arange(len(metrics))
            width = 0.35
            
            bars1 = ax.bar(x - width/2, rf_values, width, label='Random Forest', color='#2E86AB', alpha=0.8)
            bars2 = ax.bar(x + width/2, xgb_values, width, label='XGBoost', color='#A23B72', alpha=0.8)
            
            ax.set_xlabel('Evaluation Metrics', fontweight='bold', fontsize=12)
            ax.set_ylabel('Score', fontweight='bold', fontsize=12)
            ax.set_title('Model Performance Comparison: Random Forest vs XGBoost', fontweight='bold', fontsize=14)
            ax.set_xticks(x)
            ax.set_xticklabels(metrics)
            ax.legend(fontsize=11)
            ax.grid(axis='y', alpha=0.3)
            ax.set_ylim([0.95, 1.0])
            
            for bars in [bars1, bars2]:
                for bar in bars:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                           f'{height:.4f}', ha='center', va='bottom', fontsize=9)
            
            plt.tight_layout()
            metrics_path = self.output_dir / 'classification_metrics_comparison.png'
            plt.savefig(metrics_path, dpi=300, bbox_inches='tight')
            logger.info(f"✓ Metrics comparison plot saved: {metrics_path}")
            output_paths['metrics'] = metrics_path
            plt.close()
            
            fig, ax = plt.subplots(figsize=(10, 8), dpi=300)
            cm_rf = rf_results['confusion_matrix']
            sns.heatmap(cm_rf, annot=True, fmt='d', cmap='Blues', ax=ax,
                       xticklabels=self.label_encoder.classes_,
                       yticklabels=self.label_encoder.classes_,
                       cbar_kws={'label': 'Count'}, annot_kws={'fontsize': 11})
            ax.set_title('Random Forest - Confusion Matrix', fontweight='bold', fontsize=14)
            ax.set_ylabel('True Label', fontweight='bold', fontsize=12)
            ax.set_xlabel('Predicted Label', fontweight='bold', fontsize=12)
            plt.tight_layout()
            rf_cm_path = self.output_dir / 'classification_rf_confusion_matrix.png'
            plt.savefig(rf_cm_path, dpi=300, bbox_inches='tight')
            logger.info(f"✓ RF confusion matrix saved: {rf_cm_path}")
            output_paths['rf_cm'] = rf_cm_path
            plt.close()
            
            fig, ax = plt.subplots(figsize=(10, 8), dpi=300)
            cm_xgb = xgb_results['confusion_matrix']
            sns.heatmap(cm_xgb, annot=True, fmt='d', cmap='Purples', ax=ax,
                       xticklabels=self.label_encoder.classes_,
                       yticklabels=self.label_encoder.classes_,
                       cbar_kws={'label': 'Count'}, annot_kws={'fontsize': 11})
            ax.set_title('XGBoost - Confusion Matrix', fontweight='bold', fontsize=14)
            ax.set_ylabel('True Label', fontweight='bold', fontsize=12)
            ax.set_xlabel('Predicted Label', fontweight='bold', fontsize=12)
            plt.tight_layout()
            xgb_cm_path = self.output_dir / 'classification_xgb_confusion_matrix.png'
            plt.savefig(xgb_cm_path, dpi=300, bbox_inches='tight')
            logger.info(f"✓ XGBoost confusion matrix saved: {xgb_cm_path}")
            output_paths['xgb_cm'] = xgb_cm_path
            plt.close()
            
            logger.info(f"✓ All comparison plots generated separately")
            return output_paths
        
        except Exception as e:
            logger.error(f"Error creating plots: {e}")
            raise
    
    def create_earthquake_distribution_chart(self, df: pl.DataFrame) -> Optional[Path]:
        try:
            logger.info("Creating earthquake distribution by year chart...")
            
            if 'datetime' not in df.columns:
                logger.warning(f"No datetime column found. Available columns: {df.columns}")
                return None
            
            df_with_year = df.with_columns(
                pl.col('datetime').cast(pl.Date).dt.year().alias('year')
            )
            
            year_dist = df_with_year.group_by('year').agg(
                pl.col('id').count().alias('count')
            ).sort('year')
            
            year_data = year_dist.to_pandas()
            
            logger.info(f"Earthquake distribution by year (Total: {len(df):,} records):")
            total_records = 0
            for _, row in year_data.iterrows():
                year_val = int(row['year'])
                count_val = int(row['count'])
                total_records += count_val
                logger.info(f"  Year {year_val}: {count_val:6d} earthquakes")
            logger.info(f"  Total: {total_records:,} earthquakes")
            
            fig, ax = plt.subplots(figsize=(14, 7), dpi=300)
            
            years = year_data['year'].astype(int).astype(str)
            counts = year_data['count']
            
            bars = ax.bar(years, counts, color='#2E86AB', alpha=0.8, edgecolor='#1B3A52', linewidth=1.5)
            
            ax.set_xlabel('Year (Data Ingestion Timeline)', fontweight='bold', fontsize=12)
            ax.set_ylabel('Number of Earthquake Records', fontweight='bold', fontsize=12)
            ax.set_title('Earthquake Records Distribution by Year\nAsia Region (USGS Data)', 
                        fontweight='bold', fontsize=14, pad=20)
            ax.grid(axis='y', alpha=0.3, linestyle='--')
            
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height):,}', ha='center', va='bottom', 
                       fontsize=10, fontweight='bold')
            
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
            
            if len(years) > 10:
                plt.xticks(rotation=45, ha='right')
            
            stats_text = f'Total Records: {len(df):,}\nYear Range: {int(year_data["year"].min())}-{int(year_data["year"].max())}'
            ax.text(0.98, 0.97, stats_text, transform=ax.transAxes, 
                   fontsize=10, verticalalignment='top', horizontalalignment='right',
                   bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
            
            plt.tight_layout()
            output_path = self.output_dir / 'earthquake_distribution_by_year.png'
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            logger.info(f"✓ Year distribution chart saved: {output_path}")
            plt.close()
            
            return output_path
        
        except Exception as e:
            logger.error(f"Error creating year distribution chart: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def save_models(self, rf_model, xgb_model) -> bool:
        try:
            logger.info("Saving models...")
            
            rf_path = self.models_dir / 'random_forest_classifier.pkl'
            with open(rf_path, 'wb') as f:
                pickle.dump(rf_model, f)
            logger.info(f"✓ Saved Random Forest: {rf_path}")
            
            xgb_path = self.models_dir / 'xgboost_classifier.pkl'
            with open(xgb_path, 'wb') as f:
                pickle.dump(xgb_model, f)
            logger.info(f"✓ Saved XGBoost: {xgb_path}")
            
            scaler_path = self.models_dir / 'feature_scaler.pkl'
            with open(scaler_path, 'wb') as f:
                pickle.dump(self.scaler, f)
            logger.info(f"✓ Saved scaler: {scaler_path}")
            
            encoder_path = self.models_dir / 'label_encoder.pkl'
            with open(encoder_path, 'wb') as f:
                pickle.dump(self.label_encoder, f)
            logger.info(f"✓ Saved label encoder: {encoder_path}")
            
            return True
        
        except Exception as e:
            logger.error(f"Error saving models: {e}")
            return False
    
    def save_results_to_postgres(self, rf_results: Dict, xgb_results: Dict) -> bool:
        try:
            logger.info("Saving results to PostgreSQL...")
            
            if not self.conn:
                self.connect_postgres()
            
            cur = self.conn.cursor()
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS model_evaluation_results (
                    id SERIAL PRIMARY KEY,
                    model_name VARCHAR(50) NOT NULL,
                    accuracy FLOAT NOT NULL,
                    precision FLOAT NOT NULL,
                    recall FLOAT NOT NULL,
                    f1_score FLOAT NOT NULL,
                    test_samples INTEGER NOT NULL,
                    evaluation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(model_name)
                )
            """)
            
            results_data = [
                ('Random Forest', rf_results['accuracy'], rf_results['precision'],
                 rf_results['recall'], rf_results['f1_score'], len(rf_results['y_test'])),
                ('XGBoost', xgb_results['accuracy'], xgb_results['precision'],
                 xgb_results['recall'], xgb_results['f1_score'], len(xgb_results['y_test']))
            ]
            
            execute_values(
                cur,
                """
                INSERT INTO model_evaluation_results (model_name, accuracy, precision, recall, f1_score, test_samples)
                VALUES %s
                ON CONFLICT (model_name) DO UPDATE SET
                    accuracy = EXCLUDED.accuracy,
                    precision = EXCLUDED.precision,
                    recall = EXCLUDED.recall,
                    f1_score = EXCLUDED.f1_score,
                    test_samples = EXCLUDED.test_samples,
                    evaluation_timestamp = CURRENT_TIMESTAMP
                """,
                results_data
            )
            
            self.conn.commit()
            logger.info("✓ Results saved to PostgreSQL")
            return True
        
        except Exception as e:
            logger.error(f"Error saving to PostgreSQL: {e}")
            return False
        finally:
            if cur:
                cur.close()
    
    def run_classification_pipeline(self) -> Dict:
        try:
            logger.info("="*70)
            logger.info("EARTHQUAKE CLUSTER CLASSIFICATION PIPELINE - START")
            logger.info("="*70)
            
            if not self.connect_postgres():
                return {'status': 'FAILED', 'error': 'Database connection failed'}
            
            logger.info("\n[STEP 1/5] Reading training data...")
            df = self.read_training_data()
            if df is None or len(df) == 0:
                return {'status': 'FAILED', 'error': 'No training data available'}
            
            logger.info("\n[STEP 2/5] Preparing features...")
            X, y = self.prepare_features(df)
            
            logger.info("\n[STEP 3/5] Splitting data (80-20 with stratification)...")
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42, stratify=y
            )
            logger.info(f"  Total samples: {len(X):,}")
            logger.info(f"  Train set: {len(X_train):,} samples (80%)")
            logger.info(f"  Test set:  {len(X_test):,} samples (20%)")
            
            logger.info(f"\n  Class distribution in TRAINING set:")
            for i, class_name in enumerate(self.label_encoder.classes_):
                train_count = np.sum(y_train == i)
                train_pct = (train_count / len(y_train)) * 100
                logger.info(f"    {class_name:12s}: {train_count:6d} ({train_pct:5.1f}%)")
            
            logger.info(f"\n  Class distribution in TEST set:")
            for i, class_name in enumerate(self.label_encoder.classes_):
                test_count = np.sum(y_test == i)
                test_pct = (test_count / len(y_test)) * 100
                logger.info(f"    {class_name:12s}: {test_count:6d} ({test_pct:5.1f}%)")
            
            logger.info("\n[STEP 4/5] Training classification models...")
            self.train_random_forest(X_train, y_train)
            self.train_xgboost(X_train, y_train)
            
            logger.info("\n[STEP 5/5] Evaluating models...")
            rf_results = self.evaluate_model(self.rf_model, X_test, y_test, 'Random Forest')
            xgb_results = self.evaluate_model(self.xgb_model, X_test, y_test, 'XGBoost')
            
            logger.info("\n[STEP 6/7] Creating comparison visualizations...")
            plot_paths = self.create_comparison_plots(rf_results, xgb_results)
            
            logger.info("\n[STEP 7/8] Creating earthquake distribution by year...")
            year_dist_path = self.create_earthquake_distribution_chart(df)
            
            logger.info("\n[STEP 8/9] Saving trained models...")
            self.save_models(self.rf_model, self.xgb_model)
            
            self.save_results_to_postgres(rf_results, xgb_results)
            
            logger.info("\n[STEP 9/9] Finalizing results...")
            
            optimal_model = 'Random Forest' if rf_results['f1_score'] >= xgb_results['f1_score'] else 'XGBoost'
            
            logger.info("\n" + "="*70)
            logger.info("CLASSIFICATION PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info("="*70)
            logger.info(f"Random Forest F1-Score:  {rf_results['f1_score']:.4f}")
            logger.info(f"XGBoost F1-Score:        {xgb_results['f1_score']:.4f}")
            logger.info(f"\n✓ OPTIMAL MODEL: {optimal_model}")
            logger.info(f"✓ Metrics comparison plot saved: {plot_paths['metrics']}")
            logger.info(f"✓ RF confusion matrix saved: {plot_paths['rf_cm']}")
            logger.info(f"✓ XGBoost confusion matrix saved: {plot_paths['xgb_cm']}")
            if year_dist_path:
                logger.info(f"✓ Year distribution chart saved: {year_dist_path}")
            logger.info("="*70 + "\n")
            
            return {
                'status': 'SUCCESS',
                'optimal_model': optimal_model,
                'rf_f1_score': rf_results['f1_score'],
                'xgb_f1_score': xgb_results['f1_score'],
                'metrics_plot': str(plot_paths['metrics']),
                'rf_cm_plot': str(plot_paths['rf_cm']),
                'xgb_cm_plot': str(plot_paths['xgb_cm']),
                'year_dist_plot': str(year_dist_path) if year_dist_path else None,
                'models_saved': str(self.models_dir)
            }
        
        except Exception as e:
            logger.error(f"Classification pipeline failed: {e}")
            import traceback
            traceback.print_exc()
            return {'status': 'FAILED', 'error': str(e)}
        
        finally:
            self.disconnect_postgres()


def run_classification_task(**context):
    try:
        logger.info("Starting earthquake cluster classification task...")
        
        classifier = EarthquakeClusterClassifier()
        result = classifier.run_classification_pipeline()
        
        if 'ti' in context:
            context['ti'].xcom_push(key='classification_status', value=result['status'])
            context['ti'].xcom_push(key='optimal_model', value=result.get('optimal_model'))
            context['ti'].xcom_push(key='rf_f1_score', value=result.get('rf_f1_score'))
            context['ti'].xcom_push(key='xgb_f1_score', value=result.get('xgb_f1_score'))
            context['ti'].xcom_push(key='metrics_plot', value=result.get('metrics_plot'))
            context['ti'].xcom_push(key='rf_cm_plot', value=result.get('rf_cm_plot'))
            context['ti'].xcom_push(key='xgb_cm_plot', value=result.get('xgb_cm_plot'))
            context['ti'].xcom_push(key='year_dist_plot', value=result.get('year_dist_plot'))
        
        if result['status'] == 'SUCCESS':
            logger.info("Classification task completed successfully")
            return result
        else:
            raise Exception(f"Classification failed: {result.get('error')}")
    
    except Exception as e:
        logger.error(f"Error in classification task: {e}")
        if 'ti' in context:
            context['ti'].xcom_push(key='classification_status', value='failed')
            context['ti'].xcom_push(key='classification_error', value=str(e))
        raise
