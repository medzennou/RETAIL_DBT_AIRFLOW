from airflow.decorators import dag, task
from datetime import datetime

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
#from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType


@dag(
	start_date=datetime(2023,1,1),
	schedule = None,
	catchup = False,
	tags = ['retail']
)

def retail():
	upload_csv_to_gcs = LocalFilesystemToGCSOperator(
		task_id = 'upload_csv_to_gcs',
		src='include/dataset/online_retail.csv',
        dst='raw/online_retail.csv',
		bucket='retail-de-gcp-2024-02-03',
		gcp_conn_id="gcp",
		mime_type="text/csv"
	)

	gcs_to_raw = aql.load_file(
        task_id='gcs_to_raw',
        input_file=File(
            'gs://retail-de-gcp-2024-02-03/raw/online_retail.csv',
            conn_id='gcp',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='raw_invoices',
            conn_id='gcp',
            metadata=Metadata(schema='retail')
        ),
        use_native_support=False,
    )

	upload_csv_to_gcs >> gcs_to_raw
retail()

