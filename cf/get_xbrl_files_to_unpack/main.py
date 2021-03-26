import base64
import gcsfs
import zipfile
from google.cloud import pubsub_v1

def callback(future):
    message_id = future.result()
    print(message_id)

def get_xbrl_files(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Sends all files to be unpacked within a .zip file as pubsub messages.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    zip_path = event["attributes"]["zip_path"]
    fs = gcsfs.GCSFileSystem(cache_timeout=0)

    save_directory = "ons-companies-house-dev-xbrl-unpacked-data/cloud_functions_test/test_" + (zip_path.split("/")[-1]).split(".")[0]

    batching_settings = pubsub_v1.types.BatchSettings(
        max_messages=1000
    )
    publisher = pubsub_v1.PublisherClient(batch_settings=batching_settings)
    topic_path = publisher.topic_path("ons-companies-house-dev", "xbrl_files_to_unpack")

    with zipfile.ZipFile(fs.open(zip_path), 'r') as zip_ref:
      names = zip_ref.namelist()[-1000:] 
      for i, contentfilename in enumerate(names):
        upload_path = save_directory + "/" + contentfilename
        data = "Xbrl file to download: {}".format(contentfilename).encode("utf-8")
        future = publisher.publish(
          topic_path, data, xbrl_path=contentfilename, export_path=upload_path, zip_path=zip_path
        )
        future.add_done_callback(callback)