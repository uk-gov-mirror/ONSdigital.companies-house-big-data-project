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

    zip_path = event["attributes"]["zip_path"]
    xbrl_path = event["attributes"]["xbrl_path"]
    upload_path = event["attributes"]["export_path"]

    print(f"Unpacking {xbrl_path} from zip")

    overhead_t0 = time.time()
    
    with zipfile.ZipFile(fs.open(zip_path), 'r') as zip_ref:
      content_file = zip_ref.read(xbrl_path)
      overhead = time.time() - overhead_t0
      process_t0 = time.time()
      with fs.open(upload_path, 'wb') as f:
        f.write(content_file)
      fs.setxattrs(
        upload_path,
        content_type="text/"+xbrl_path.split(".")[-1]
        )
    process = time.time() - process_t0
    print(f"Overhead: {overhead}s, Process: {process}s")
