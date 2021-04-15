import base64
import gcsfs
import zipfile
import time
import logging

def unpack_xbrl_file(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Unpack a list of files specified in the pub/sub message from 
    a given .zip file
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # Create a GCSFS object
    fs = gcsfs.GCSFileSystem(cache_timeout=0)

    # Extract relevant variables from the message payload
    xbrl_list = eval(base64.b64decode(event['data']).decode('utf-8'))

    zip_path = event["attributes"]["zip_path"]
    save_directory = event["attributes"]["save_directory"]
    
    with zipfile.ZipFile(fs.open(zip_path), 'r') as zip_ref:
      # For each file listed, download it to the specified location
      for xbrl_path in xbrl_list:
        upload_path = save_directory + "/" + xbrl_path

        # Attempt to read the relevant file to be unpacked
        try:
            content_file = zip_ref.read(xbrl_path)
        except:
            logging.warn(f"Unable to open {xbrl_path}")
            continue

        # Attempt to write the file to the desired location
        try:
            with fs.open(upload_path, 'wb') as f:
                f.write(content_file)
            fs.setxattrs(
                upload_path,
                content_type="text/"+xbrl_path.split(".")[-1]
                )
        except:
            logging.warn(f"Unable to write to {upload_path}")
            continue
