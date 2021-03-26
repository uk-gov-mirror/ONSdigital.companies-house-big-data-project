import base64
import gcsfs
import zipfile
import time

def unpack_xbrl_file(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    fs = gcsfs.GCSFileSystem(cache_timeout=0)

    xbrl_list = eval(base64.b64decode(event['data']).decode('utf-8'))

    zip_path = event["attributes"]["zip_path"]
    save_directory = event["attributes"]["save_directory"]
    
    with zipfile.ZipFile(fs.open(zip_path), 'r') as zip_ref:
      for xbrl_path in xbrl_list:
        print(f"unpacking {xbrl_path} from {zip_path}")
        upload_path = save_directory + "/" + xbrl_path
        content_file = zip_ref.read(xbrl_path)
        with fs.open(upload_path, 'wb') as f:
            f.write(content_file)
        fs.setxattrs(
            upload_path,
            content_type="text/"+xbrl_path.split(".")[-1]
            )
