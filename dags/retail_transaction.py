from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import pandas as pd
import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

POSTGRES_CONN_ID='postgres_default'

default_args={
    'owner':'airflow',
    'start_date':days_ago(1)
}

#DAG
with DAG(dag_id='retail_transaction_pipeline',
         default_args=default_args,
         schedule_interval='@hourly',
         catchup=False) as dags:
    @task()
    def get_latest_transfer_timestamp():
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Query last transfer
        last_transfer_query = "SELECT last_transfer FROM TransferRecord ORDER BY last_transfer DESC LIMIT 1;"
        cursor.execute(last_transfer_query)
        result = cursor.fetchone()
        last_transfer = result[0] if result else None

        cursor.close()
        return last_transfer



    @task()
    def extract_retail_transaction_db(last_transfer: datetime.datetime):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Query data from RetailTransactions where created_at or updated_at > last_transfer
        if last_transfer:
            query = "SELECT * FROM RetailTransactions WHERE created_at > %s or updated_at > %s;"
            cursor.execute(query, (last_transfer, last_transfer))
        else:
            query = "SELECT * FROM RetailTransactions"
            cursor.execute(query)

        # Fetch all rows and store them in a DataFrame
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=column_names)

        cursor.close()
        return df
    

    @task()
    def extract_retail_transaction_archive_db(last_transfer: datetime.datetime):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        # Query data from RetailTransactionsArchive
        if last_transfer:
            query = "SELECT id FROM RetailTransactionsArchive WHERE deleted_at_archive > %s;"
            cursor.execute(query, (last_transfer,))
        else:
            query = "SELECT id FROM RetailTransactionsArchive;"
            cursor.execute(query)
        
        # Fetch all rows and store them in a DataFrame
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=column_names)

        cursor.close()
        return df
    
    @task()
    def transform_load_datawarehouse(df: pd.DataFrame):
        # Define Google Cloud credentials
        credentials = service_account.Credentials.from_service_account_file(
            'my-project-2-7a9ff-6588929d47cd.json'
        )
        project_id = "my-project-2-7a9ff"
        
        # Define BigQuery dataset and table
        dataset_id = "lion_parcel"
        table_id = "RetailTransactions"
        staging_table_id = "RetailTransactions_Staging"

        # Initialize BigQuery client
        client = bigquery.Client(credentials=credentials, project=project_id)

        # List of columns to convert to datetime
        date_columns = ['created_at', 'updated_at', 'deleted_at']

        if df.shape[0] > 0:
            # Apply pd.to_datetime to each column in the list
            df[date_columns] = df[date_columns].apply(pd.to_datetime)

            # List of columns to convert to int
            int_columns = ['id', 'customer_id']

            # Apply astype(int) to each column in the list
            df[int_columns] = df[int_columns].apply(pd.to_numeric, errors='coerce').astype('Int64')

            # List of columns to convert to string
            string_columns = ['last_status', 'pos_origin', 'pos_destination']

            # Apply astype(str) to each column in the list
            df[string_columns] = df[string_columns].astype(str)

            # Upload the DataFrame to a staging table
            df.to_gbq(
                destination_table=f"{dataset_id}.{staging_table_id}",
                project_id=project_id,
                if_exists="replace",  # Replace staging table on each run
                credentials=credentials
            )

            # Define the SQL query for merging
            merge_query = f"""
            MERGE `{project_id}.{dataset_id}.{table_id}` AS target
            USING `{project_id}.{dataset_id}.{staging_table_id}` AS source
            ON target.id = source.id
            WHEN MATCHED THEN
                UPDATE SET
                    target.customer_id = source.customer_id,
                    target.last_status = source.last_status,
                    target.pos_origin = source.pos_origin,
                    target.pos_destination = source.pos_destination,
                    target.created_at = source.created_at,
                    target.updated_at = source.updated_at,
                    target.deleted_at = source.deleted_at
            WHEN NOT MATCHED THEN
                INSERT (id, customer_id, last_status, pos_origin, pos_destination, created_at, updated_at, deleted_at)
                VALUES (source.id, source.customer_id, source.last_status, source.pos_origin, source.pos_destination, source.created_at, source.updated_at, source.deleted_at);
            """

            # Step 3: Execute the merge query
            query_job = client.query(merge_query)
            query_job.result()  # Wait for the query to finish

            # Step 4: Optionally clean up the staging table
            client.delete_table(f"{dataset_id}.{staging_table_id}", not_found_ok=True)
            print(f"Staging table {staging_table_id} deleted.")

    @task()
    def sync_deleted_data(df: pd.DataFrame):
        # Define Google Cloud credentials
        credentials = service_account.Credentials.from_service_account_file('my-project-2-7a9ff-6588929d47cd.json')
        project_id = "my-project-2-7a9ff"
        
        # Define BigQuery dataset and table
        dataset_id = "lion_parcel"
        table_id = "RetailTransactions"

        # Initialize BigQuery client
        client = bigquery.Client(project=project_id, credentials=credentials)

        if df.shape[0] > 0:

            # Convert the DataFrame 'id' column to a list of ids to delete
            ids_to_delete = df['id'].tolist()

            # Create the delete query
            # Make sure to format the ids correctly for inclusion in the query
            delete_query = f"""
                DELETE FROM `{dataset_id}.{table_id}`
                WHERE id IN UNNEST(@ids);
            """
            
            # Set up the query parameters
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("ids", "INT64", ids_to_delete)
                ]
            )

            # Run the delete query
            query_job = client.query(delete_query, job_config=job_config)

            # Wait for the query to complete
            query_job.result()
    
    @task()
    def mark_latest_transfer_record_success(cur_date: datetime.datetime):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # TansferRecord Success
        query = "INSERT INTO TransferRecord (last_transfer) VALUES (%s);"
        
        cursor.execute(query, (cur_date, ))

        conn.commit()
        cursor.close()


    # Define task dependencies
    current_datetime = datetime.datetime.now()
    last_transfer = get_latest_transfer_timestamp()
    retail_transaction = extract_retail_transaction_db(last_transfer)
    retail_transaction_archive = extract_retail_transaction_archive_db(last_transfer)
    task3 = transform_load_datawarehouse(retail_transaction)
    task4 = sync_deleted_data(retail_transaction_archive)
    task5 = mark_latest_transfer_record_success(current_datetime)

    task3 >> task5
    task4 >> task5