import logging
import time
from google.cloud import logging as cloudlogging

def send_test_logs(event, context):

    logging.warning("This is a warning")
    logging.critical("This is critical")

    t0 = time.time()
    log_client = cloudlogging.Client()
    log_handler = log_client.get_default_handler()
    cloud_logger = logging.getLogger("cloudLogger")
    cloud_logger.setLevel(logging.INFO)
    cloud_logger.addHandler(log_handler)

    t = time.time() - t0
    print(f"Logging setup took {t} seconds")

    t0 = time.time()
    cloud_logger.info("This is info")
    t = time.time() - t0
    print(f"LogOut took {t} s")

    raise RuntimeError(
        "This is to test error reporting"
    )

