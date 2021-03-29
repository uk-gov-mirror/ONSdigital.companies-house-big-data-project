import base64

from xbrl_parser import XbrlParser

def parse_batch(event, context):

    parser = XbrlParser()

    files = eval(base64.b64decode(event['data']).decode('utf-8'))

    xbrl_directory = event["xbrl_directory"]
    bq_location = event["bq_location"]
    csv_location = event["csv_location"]

    parser.parse_files(files, xbrl_directory, bq_location, csv_location)

    