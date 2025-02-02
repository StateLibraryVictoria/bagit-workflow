from src.helper_functions import *
from src.database_functions import *
import bagit
import time
import sqlite3
import sys

logger = logging.getLogger(__name__)


def main():
    # load variables
    config = load_config()
    logging_dir = config.get("LOGGING_DIR")
    transfer_dir = config.get("TRANSFER_DIR")
    archive_dir = config.get("ARCHIVE_DIR")
    database = config.get("DATABASE")

    valid_transfers = []

    logfilename = f"{time.strftime('%Y%m%d')}_bagit_transfer.log"
    logfile = os.path.join(logging_dir, logfilename)
    logging.basicConfig(
        filename=logfile,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # check that directories are connected.
    for dir in [transfer_dir, archive_dir]:
        if not os.path.exists(dir):
            logger.error(f"Directory: {dir} does not exist.")
            sys.exit()

    # Get the .ok files at the transfer directory
    at_transfer = os.listdir(transfer_dir)
    ok_files = [x for x in at_transfer if x.endswith(".ok")]

    if len(ok_files) == 0:
        logger.info("No trigger files staged in transfer directory.")
        sys.exit()
    else:
        id_parser = load_id_parser()
        logger.info(f"Transfers to process: {len(ok_files)}")
        for file in ok_files:
            tf = TriggerFile(os.path.join(transfer_dir, file), id_parser)
            if tf.validate():
                valid_transfers.append(tf)

    # set up database
    try:
        configure_transfer_db(database)
    except sqlite3.OperationalError as e:
        print(f"Error configuring database: {e}")

    with get_db_connection(database) as con:
        cur = con.cursor()

        for tf in valid_transfers:
            transfer_start = datetime.now()
            # generate and add a random uuid as External-Identifier
            metadata = tf.get_metadata()
            folder = tf.get_directory()
            if metadata is not None:
                # make a bag
                try:
                    bag = tf.make_bag()
                except Exception as e:
                    logger.error(f"Error processing bag: {e}")
                    continue

                # check if bag is valid before moving.
                if bag.is_valid():
                    # Primary id for filing
                    primary_id = guess_primary_id(bag.info[PRIMARY_ID])

                    # Hash manifest for dedupe
                    manifest_hash = compute_manifest_hash(folder)

                    # check the transfer is unique
                    results = cur.execute(
                        "SELECT * FROM transfers WHERE ManifestSHA256Hash=:id",
                        {"id": manifest_hash},
                    )
                    identical_folders = results.fetchall()
                    if len(identical_folders) > 0:
                        logger.error(
                            f"Manifest hash conflict: folder {folder} with transaction id {identical_folders[0][0]} and folder title {identical_folders[0][1]}"
                        )
                        tf.set_error("Folder is a duplicate.")
                        continue

                    # Check output directory
                    try:
                        count = get_count_collections_processed(primary_id, database)
                    except Exception as e:
                        continue
                    count += 1
                    # Copy to output directory
                    output_folder = os.path.join(
                        os.path.basename(os.path.normpath(primary_id)), f"t{count}"
                    )
                    output_dir = os.path.join(archive_dir, output_folder)

                    if not os.path.exists(output_dir):
                        os.makedirs(output_dir)

                    copy_time = timed_rsync_copy(folder, output_dir)

                output_bag = bagit.Bag(output_dir)

                # check copied bag is valid and if so update database
                if output_bag.is_valid():
                    try:
                        insert_transfer(
                            output_dir,
                            bag,
                            primary_id,
                            manifest_hash,
                            transfer_start,
                            datetime.now(),
                            database,
                        )
                        tf.cleanup_transfer()
                    except Exception as e:
                        logger.error(
                            f"Failed to insert transfer for folder {folder} with Collection Identifier {primary_id}: {e}"
                        )
                        tf.set_error(
                            f"DATABASE WRITE ERROR -- Failed to insert transfer for folder {folder} with Collection Identifier {primary_id}: {e}"
                        )
                        continue
                else:
                    logger.error(
                        "Transferred bag was invalid. Removing transferred data."
                    )
                    tf.set_error(
                        f"Transferred bag {folder} was invalid. Removing transferred data."
                    )
                    os.rmdir(output_dir)
            else:
                logger.error(f"Error moving bag: {e}")


if __name__ == "__main__":
    main()
