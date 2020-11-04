from google.cloud import bigquery
from google.oauth2 import service_account

# TODO(developer): Set key_path to the path to the service account key
#                  file.
key_path = "D://projects/Events/code/eloquent-glow-286906-cead14f1fa45.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id,)


# TODO(developer): Set table_id to the ID of the table to create.
table_id = "eloquent-glow-286906.Sample.test_file_for_upload"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("date", "STRING"),
    ],
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
   # write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
)
# uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"
#uri = "gs://kronos_poc/supermarkets/1. Forecast VS Actual/POC Forecast export 03-08 to 23-08 (2).csv"



uri = "gs://events_bucket_gcs/test_file_for_upload.csv"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.


# =============================================================================
# 
# file_upload = "D:\projects\Events\code\test_file_for_upload.csv"
# 
# f = pd.read_csv('test_file_for_upload.csv')  
# 
# df_p = df.to_parquet(f)
# 
# df_p
# 
# load_job = client.load_table_from_file(
#     file_upload, table_id, job_config=job_config
# )  # Make an API request.
# 
# =============================================================================
load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))