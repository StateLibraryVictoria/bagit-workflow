from src.helper_functions import *
import bagit
import time
import subprocess

logger = logging.getLogger(__name__)

TRANSFER_DIR = os.getenv("TRANSFER_DIR")
ARCHIVE_DIR = os.getenv("ARCHIVE_DIR")
LOGGING_DIR = os.getenv("LOGGING_DIR")


def main():
    valid_transfers = []
    valid_metadata = {}

    logfilename = f"{time.strftime('%Y%m%d')}_stash-it_transfer.log"
    logfile = os.path.join(LOGGING_DIR, logfilename)
    logging.basicConfig(
        filename=logfile,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

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
        logger.info(f"Transfers to process: {len(ok_files)}")
        for file in ok_files:
            tf = TriggerFile(os.path.join(TRANSFER_DIR, file))
            folder = tf.get_directory()
            if tf.validate():
                valid_transfers.append(folder)
                valid_metadata.update({folder: tf.get_metadata()})

    mc = MetadataChecker()

    for folder in valid_transfers:
        # generate and add a random uuid as External-Identifier
        metadata = valid_metadata.get(folder)
        try:
            metadata = mc.validate(metadata)
            try:
                bag = bagit.Bag(folder)
                logger.info(f"Processing existing bag at: {folder}")
                for key in metadata.keys():
                    bag.info[key] = metadata.get(key)
                bag.save()
            except Exception as e:
                logger.info(f"Making new bag at: {folder}")
                bag = bagit.make_bag(folder, bag_info=metadata)
            if bag.is_valid():
                subprocess.run(["rsync", "-vrlt", folder, ARCHIVE_DIR])
            output_bag = os.path.join(ARCHIVE_DIR, folder)
            output_bag = bagit.Bag(output_bag)
            output_bag.validate()
        except ValueError as e:
            logger.error(f"Error validating metadata: {e}")
        # check it's not already a bag:


main()
