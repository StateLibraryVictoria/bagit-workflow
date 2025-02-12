from src.helper_functions import *
from src.database_functions import *
import sys
import bagit
import time
import sqlite3

logger = logging.getLogger(__name__)


def main():
    # load variables
    config = load_config()
    logging_dir = config.get("LOGGING_DIR")
    transfer_dir = config.get("TRANSFER_DIR")
    archive_dir = config.get("ARCHIVE_DIR")
    appraisal_dir = config.get("APPRAISAL_DIR")
    database = config.get("DATABASE")

    for variable in [logging_dir, transfer_dir, archive_dir, appraisal_dir, database]:
        if variable == None:
            sys.exit()

    runfile_check(transfer_dir)

    valid_transfers = []

    logfilename = f"{time.strftime('%Y%m%d')}_bagit_transfer.log"
    logfile = os.path.join(logging_dir, logfilename)
    logging.basicConfig(
        filename=logfile,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # check that directories are connected.
    for dir in [transfer_dir, archive_dir, appraisal_dir]:
        if not os.path.exists(dir):
            logger.error(f"Directory: {dir} does not exist.")
            runfile_cleanup(transfer_dir)

    # Get the .ok files at the transfer directory
    at_transfer = os.listdir(transfer_dir)
    ok_files = [x for x in at_transfer if x.endswith(".ok")]

    if len(ok_files) == 0:
        logger.info("No trigger files staged in transfer directory.")
        runfile_cleanup(transfer_dir)
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
                    tf.set_error(f"Error processing bag: {e}")
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
                            f"Manifest hash conflict: transfer with UUID {identical_folders[0][2]} and "
                            + f"transaction id {identical_folders[0][0]} and original folder title {identical_folders[0][9]} matches this transfer."
                        )
                        tf.set_error(
                            f"Folder is a duplicate -- an identical transfer has been identified with UUID {identical_folders[0][2]} "
                            + f"and transaction id {identical_folders[0][0]} and original folder title {identical_folders[0][9]}."
                        )
                        continue

                    # Get transfer index
                    try:
                        count = get_count_collections_processed(primary_id, database)
                    except Exception as e:
                        continue
                    count += 1

                    # Build output folder path
                    output_folder = os.path.join(
                        os.path.basename(os.path.normpath(primary_id)), f"t{count}"
                    )

                    # this is the archive directory relative location written to db
                    output_dir = os.path.join(archive_dir, output_folder)

                    # Test for existing directory
                    logger.info("Testing to see if output folder exists.")
                    if not os.path.exists(output_dir):
                        logger.info(f"Making the output directory {output_dir}")
                        os.makedirs(output_dir)
                    else:
                        logger.error(
                            f"Output directory {output_dir} already exists. Skipping."
                        )
                        tf.set_error(f"Output directory {output_dir} already exists.")
                        continue

                # copy folder to output directory
                process_transfer(folder, output_dir)

                output_bag = bagit.Bag(output_dir)

                # check copied bag is valid and if so update database
                try:
                    output_bag.validate()
                    try:
                        insert_transfer(
                            output_folder,
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
                except bagit.BagValidationError as e:
                    logger.error(
                        "Transferred bag was invalid. Removing transferred data."
                    )
                    tf.set_error(
                        f"Transferred bag {folder} was invalid. Removing transferred data... \n Error recorded: {e}"
                    )
                    os.rmdir(output_dir)
                    continue
            else:
                logger.error(
                    f"Error moving bag: metadata could not be generated or read."
                )
                tf.set_error(
                    f"Error moving bag to preservation directory: metadata could not be generated or read."
                )
    runfile_cleanup(transfer_dir)


if __name__ == "__main__":
    main()
