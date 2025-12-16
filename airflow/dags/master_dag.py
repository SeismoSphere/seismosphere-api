from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys

sys.path.insert(0, '/opt/airflow/operators')
from polars_ingestion_operator import PolarsEarthquakeIngestor
from polars_preprocessing_operator import preprocess_earthquakes

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

def ingest_2years_data(**context):
    print("="*70)
    print("EARTHQUAKE DATA INGESTION")
    print("="*70)
    
    ingestor = PolarsEarthquakeIngestor(
        output_dir='/opt/airflow/data/bigdata',
        min_magnitude=0.0,
        region='asia'
    )
    
    print(f"\nCollecting 2 years of earthquake data...")
    print(f"Region: Asia")
    print(f"Min Magnitude: No filter (all magnitudes included)")
    print(f"Output Format: Parquet only\n")
    
    df = ingestor.ingest_last_2_years()
    if len(df) == 0:
        print("No data collected")
        raise ValueError("No earthquake data collected")
    
    parquet_filename = f'raw_earthquakes.parquet'
    parquet_path = ingestor.save_to_parquet(df, filename=parquet_filename)
    summary = ingestor.get_data_summary(df)
    
    print("\n" + "="*70)
    print("INGESTION SUMMARY")
    print("="*70)
    print(f"Total Records: {summary['total_records']:,}")
    print(f"Unique IDs: {summary['unique_earthquakes']:,}")
    print(f"Date Range: {summary['date_range']['start']} to {summary['date_range']['end']}")
    print(f"Magnitude: {summary['magnitude']['min']:.1f} - {summary['magnitude']['max']:.1f}")
    print(f"   Mean: {summary['magnitude']['mean']:.2f}, Median: {summary['magnitude']['median']:.2f}")
    print(f"   Min Magnitude ({summary['magnitude']['min']:.1f}): {summary['magnitude']['min_location']}")
    print(f"   Max Magnitude ({summary['magnitude']['max']:.1f}): {summary['magnitude']['max_location']}")
    print(f"Depth: {summary['depth']['min']:.1f} - {summary['depth']['max']:.1f} km")
    print(f"   Mean: {summary['depth']['mean']:.2f} km")
    print(f"Output: {parquet_path}")
    print("="*70)
    
    context['ti'].xcom_push(key='summary', value=summary)
    context['ti'].xcom_push(key='parquet_path', value=str(parquet_path))
    context['ti'].xcom_push(key='total_records', value=summary['total_records'])
    
    print("\nRAW DATA INGESTION COMPLETE!")
    
    return str(parquet_path)

with DAG(
    'master_earthquake_pipeline',
    default_args=default_args,
    description='Earthquake data pipeline: Ingestion (2 years) + Preprocessing + PostgreSQL',
    schedule_interval='0 5 1 1 *',
    catchup=False,
    tags=['earthquake', 'ingestion', 'preprocessing', 'postgres'],
) as dag:
    
    task_ingestion = PythonOperator(
        task_id='ingest_raw_data',
        python_callable=ingest_2years_data,
        provide_context=True
    )
    
    task_preprocessing = PythonOperator(
        task_id='preprocess_and_load',
        python_callable=preprocess_earthquakes,
        provide_context=True
    )

    task_ingestion >> task_preprocessing
