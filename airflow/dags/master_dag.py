from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys

sys.path.insert(0, '/opt/airflow/operators')
from polars_ingestion_operator import PolarsEarthquakeIngestor

default_args = {
    'owner': 'seismosphere',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'email': ['admin@seismosphere.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def ingest_yearly_data(**context):
    print("="*70)
    print("🚀 EARTHQUAKE DATA INGESTION")
    print("="*70)
    
    ingestor = PolarsEarthquakeIngestor(
        output_dir='/opt/airflow/data/bigdata',
        min_magnitude=0.0,
        region='asia'
    )
    
    print(f"\n📅 Collecting 1 year of earthquake data...")
    print(f"🌏 Region: Asia")
    print(f"📊 Min Magnitude: No filter (all magnitudes included)")
    print(f"💾 Output Format: Parquet only\n")
    
    df = ingestor.ingest_last_year()
    if len(df) == 0:
        print("⚠️  No data collected")
        raise ValueError("No earthquake data collected")
    
    parquet_filename = f'raw_earthquakes.parquet'
    parquet_path = ingestor.save_to_parquet(df, filename=parquet_filename)
    summary = ingestor.get_data_summary(df)
    
    print("\n" + "="*70)
    print("📊 INGESTION SUMMARY")
    print("="*70)
    print(f"📈 Total Records: {summary['total_records']:,}")
    print(f"🆔 Unique IDs: {summary['unique_earthquakes']:,}")
    print(f"📅 Date Range: {summary['date_range']['start']} to {summary['date_range']['end']}")
    print(f"🌊 Magnitude: {summary['magnitude']['min']:.1f} - {summary['magnitude']['max']:.1f}")
    print(f"   Mean: {summary['magnitude']['mean']:.2f}, Median: {summary['magnitude']['median']:.2f}")
    print(f"🏔️  Depth: {summary['depth']['min']:.1f} - {summary['depth']['max']:.1f} km")
    print(f"   Mean: {summary['depth']['mean']:.2f} km")
    print(f"💾 Output: {parquet_path}")
    print("="*70)
    
    context['ti'].xcom_push(key='summary', value=summary)
    context['ti'].xcom_push(key='parquet_path', value=str(parquet_path))
    context['ti'].xcom_push(key='total_records', value=summary['total_records'])
    
    print("\n✅ DATA INGESTION COMPLETE!")
    print("📝 Note: Spatial processing, clustering, and prediction will be added in future phases")
    
    return str(parquet_path)

with DAG(
    'master_earthquake_pipeline',
    default_args=default_args,
    description='Earthquake data ingestion pipeline - Phase 1',
    schedule_interval='0 5 1 1 *',
    catchup=False,
    tags=['earthquake', 'ingestion', 'phase-1'],
) as dag:
    
    task_ingestion = PythonOperator(
        task_id='data_ingestion',
        python_callable=ingest_yearly_data,
        provide_context=True
    )
