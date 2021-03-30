import base64
import gcsfs

from xbrl_parser import XbrlParser

def parse_batch(event, context):
    """
    Parses a given list of files (as pub/sub message data) and
    saves the result to a BigQuery table and as a .csv

    Arguments:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    Returns:
        None
    Raises:
        None
    """
    # Create parser class instance
    parser = XbrlParser()

    # Extract list of files from message data
    files = eval(base64.b64decode(event['data']).decode('utf-8'))

    # Obtain the relevant attributes from the pub/sub message
    xbrl_directory = event["attributes"]["xbrl_directory"]
    bq_location = event["attributes"]["bq_location"]
    csv_location = event["attributes"]["csv_location"]

    # Parse the batch of files
    parser.parse_files(files, xbrl_directory, bq_location, csv_location)


if __name__ == "__main__":
    fs = gcsfs.GCSFileSystem()

    files = [x.split("/")[-1] for x in fs.ls("ons-companies-house-dev-xbrl-unpacked-data/cloud_functions_test/Accounts_Monthly_Data-February2021")[0:10]]

    print(files)

    event = {
        "data": base64.b64encode((str(files)).encode("utf-8")),
        "attributes":{
            "xbrl_directory":"ons-companies-house-dev-xbrl-unpacked-data/cloud_functions_test/Accounts_Monthly_Data-February2021",
            "bq_location":"xbrl_parsed_data",
            "csv_location":"ons-companies-house-dev-test-parsed-csv-data/cloud_functions_test"
        }
    }
    print(event)
