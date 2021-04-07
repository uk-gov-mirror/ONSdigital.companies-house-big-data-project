import logging
import time
from google.cloud import logging as cloudlogging

def send_test_logs(event, context):

    logging.info("This is an info")
    logging.warning("This is a warning")
    logging.critical("This is critical")
    logging.error("This is an error")

    t0 = time.time()
    log_client = cloudlogging.Client()
    log_handler = log_client.get_default_handler()
    cloud_logger = logging.getLogger("cloudLogger")
    cloud_logger.setLevel(logging.INFO)
    cloud_logger.addHandler(log_handler)

    t = time.time() - t0
    print(f"Logging setup took {t} seconds")

    t0 = time.time()
    cloud_logger.info("CL: This is info")
    t = time.time() - t0
    print(f"LogOut took {t} s")

    cloud_logger.error("CL: This is an error")
    cloud_logger.critical("CL: This is critical")
    cloud_logger.debug("CL: This a debug")
    
    raise TypeError(
        "This is to test error reporting"
    )

