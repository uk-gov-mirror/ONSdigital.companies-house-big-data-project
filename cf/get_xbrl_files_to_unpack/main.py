import base64
import gcsfs
import zipfile
from google.cloud import pubsub_v1

def callback(future):
    """
    Function to allow the callback of the publishing 
    of a pub/sub message to be handled outside of the main
    function.

    Arguments
        future: publisher.publish object for publishing a message
                to a topic
    Returns:
        None
    Raises:
        None
    """
    message_id = future.result()
    
    
def get_xbrl_files(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Sends all files to be unpacked within a .zip file as pubsub messages.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # Extract desired attributes from the message payload
    zip_path = event["attributes"]["zip_path"]

    # Create a GCSFS object
    fs = gcsfs.GCSFileSystem(cache_timeout=0)

    # Check the specified directory is valid
    if not fs.exists(zip_path):
        raise ValueError(
            f"Directory {zip_path} does not exist"
    )

    # Specify the directory where unpacked files should be saved
    save_directory = "ons-companies-house-dev-xbrl-unpacked-data/cloud_functions_test/" + (zip_path.split("/")[-1]).split(".")[0]

    
    # Configure batching settings to optimise publishing efficiency
    batching_settings = pubsub_v1.types.BatchSettings(
        max_messages=1000
    )

    # Create publisher object allowing function to publish messages to the given topic
    publisher = pubsub_v1.PublisherClient(batch_settings=batching_settings)
    topic_path = publisher.topic_path("ons-companies-house-dev", "xbrl_files_to_unpack")
    
    # Set the message batch size
    n = 1000

    with zipfile.ZipFile(fs.open(zip_path), 'r') as zip_ref:
      
      # Compile all files within the .zip and separate them into batches of
      # size n
      zip_list = zip_ref.namelist()
      names = [zip_list[i*n : (i+1)*n] for i in range((len(zip_list) + n - 1)//n)]

      print(f"Unpacking {len(zip_list)} files using batches of size {n}")
      
      # For each batch, publish a message with the list of files to be unpacked
      for i, contentfilename in enumerate(names):
        data = str(contentfilename).encode("utf-8")
        future = publisher.publish(
          topic_path, data, save_directory=save_directory, zip_path=zip_path
        )
        future.add_done_callback(callback)