import base64
import gcsfs

from xbrl_parser import XbrlParser

def parse_batch(event, context):

    parser = XbrlParser()

    files = eval(base64.b64decode(event['data']).decode('utf-8'))
    print(files)

    xbrl_directory = event["attributes"]["xbrl_directory"]
    bq_location = event["attributes"]["bq_location"]
    csv_location = event["attributes"]["csv_location"]

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
    parse_batch(event, 0)
