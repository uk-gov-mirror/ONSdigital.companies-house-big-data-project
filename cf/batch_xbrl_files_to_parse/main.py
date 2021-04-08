import base64
import gcsfs
import os
from google.cloud import pubsub_v1, bigquery

def callback(future):
    message_id = future.result()
    print(message_id)

def mk_bq_table(bq_location, schema="parsed_data_schema.txt"):
        """
        Function to create a BigQuery table in a specified location with a
        schema specified by a txt file.

        Arguments:
            bq_location:    Location of BigQuery table, in form
                            "<dataset>.<table_name>"
            schema:         File path of schema specified as txt file
        Returns:
            None
        Raises:
            None
        """
        # Set up a BigQuery client
        client = bigquery.Client("ons-companies-house-dev")

        # Check if table exists
        try:
            client.get_table(bq_location)
            table_exists = True
        except:
            table_exists = False

        if table_exists:
            raise ValueError("Table already exists, please remove and retry")
        

        # Create the table using the command line
        bq_string = "bq mk --table " + bq_location + " " + schema
        os.popen(bq_string).read()
        
        # Remove environment variables
        os.environ.clear()

def batch_files(event, context):

    fs = gcsfs.GCSFileSystem(cache_timeout=0)

    xbrl_directory = event["xbrl_directory"]

    bq_location = "xbrl_parsed_data"
    csv_location = "ons-companies-house-dev-test-parsed-csv-data/cloud_functions_test"

    all_files = fs.ls(xbrl_directory)

    min_batch_size = len(all_files)//1400

    n = 350

    if n < min_batch_size:
        raise ValueError(
            "Batch size is too small (will exceed BQ max uploads)"
    )
    # Extract the relevant date information from the directory name
    folder_month = "".join(xbrl_directory.split("/")[-1].split("-")[1:])[0:-4]
    folder_year = "".join(xbrl_directory.split("/")[-1].split("-")[1:])[-4:]

    # Define the location where to export results to BigQuery
    table_export = bq_location + "." + folder_month + "-" + folder_year

    # Create a BigQuery table
    mk_bq_table(table_export)

    batched_files = [all_files[i*n : (i+1)*n] for i in range((len(all_files) + n - 1)//n)]

    print(f"Parsing files in {len(batched_files)} batches")
    ps_batching_settings = pubsub_v1.types.BatchSettings(
        max_messages=1000
    )
    publisher = pubsub_v1.PublisherClient(batch_settings=ps_batching_settings)
    topic_path = publisher.topic_path("ons-companies-house-dev", "xbrl_parser_batches")
    
    for batch in batched_files:
        data = str(batch).encode("utf-8")
        future = publisher.publish(
            topic_path, data, xbrl_directory=xbrl_directory, table_export=table_export, csv_location=csv_location
        )
        future.add_done_callback(callback)

