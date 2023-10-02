from datetime import datetime
from airflow import DAG
#from airflow.operators.bash_operator import BashOperator
from airflow.operators.bash import BashOperator


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 2),  # Adjust the start date
    'retries': 1,
}

# Create a DAG instance
dag = DAG(
    'local_to_gcs_transfer',
    default_args=default_args,
    schedule_interval=None,  # You can set a schedule if needed
    catchup=False,  # If True, backfilling will be enabled
    tags=['example'],
)

# Define the source directory and Google Cloud Storage bucket names
source_directory = 'D:\local_to_gcs_files'
destination_bucket = 'gcp_bigquery_gcs_census_sync_demo'
gcs_path = 'src_files'

# Define a BashOperator to copy files from local to GCS
local_to_gcs_task = BashOperator(
    task_id='local_to_gcs',
    bash_command=f'gsutil -m cp {source_directory}/*.txt {source_directory}/*.csv gs://{destination_bucket}/{gcs_path}/',
    dag=dag,
)

# Set task dependencies if needed
# local_to_gcs_task >> ...

if __name__ == "__main__":
    dag.cli()
