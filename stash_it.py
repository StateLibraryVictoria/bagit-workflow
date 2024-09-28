from src.helper_functions import *
import time

logger = logging.getLogger(__name__)

TRANSFER_DIR = os.getenv("TRANSFER_DIR")
ARCHIVE_DIR = os.getenv("ARCHIVE_DIR")
LOGGING_DIR = os.getenv("LOGGING_DIR")

def main():
    logfilename = f"{time.strftime('%Y%m%d')}_stash-it_transfer.log"
    logfile = os.path.join(LOGGING_DIR, logfilename)
    logging.basicConfig(filename=logfile, level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # check that directories are connected.
    if not os.path.exists(LOGGING_DIR):
        raise Exception("Not connected to logging directory.")
    elif not os.path.exists(TRANSFER_DIR) or not os.path.exists(ARCHIVE_DIR):
        logger.error(f"Error connecting to storage and transfer locations.")
        raise Exception("Input and output directories not correctly configured.")
    
    at_transfer = os.listdir(TRANSFER_DIR)
    ok_files = [x for x in at_transfer if x.endswith(".ok")]

    if len(ok_files) == 0:
        logger.info("No trigger files staged in transfer directory.")
    else:
        for file in ok_files:
            tf = TriggerFile(os.path.join(TRANSFER_DIR, file))
            tf.validate()

main()