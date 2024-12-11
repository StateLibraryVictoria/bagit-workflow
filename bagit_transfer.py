from src.helper_functions import *
import bagit
import time
import subprocess
import sqlite3
import hashlib
import sys
import shutil
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# metadata tags
PRIMARY_ID = "External-Identifier"
UUID_ID = "Internal-Sender-Identifier"
CONTACT = "Contact-Name"
EXTERNAL_DESCRIPTION = "External-Description"

def load_config():
    config = {
        "TRANSFER_DIR": os.getenv("TRANSFER_DIR"),
        "ARCHIVE_DIR": os.getenv("ARCHIVE_DIR"),
        "LOGGING_DIR": os.getenv("LOGGING_DIR"),
        "DATABASE": os.getenv("DATABASE"),
    }
    return config


@contextmanager
def get_db_connection(db_path):
    con = sqlite3.connect(db_path)
    try:
        yield con
    except sqlite3.DatabaseError as e:
        con.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        con.commit()
        con.close()


def configure_db(database_path):
    with get_db_connection(database_path) as con:
        cur = con.cursor()
        try:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS Collections(CollectionIdentifier PRIMARY KEY, Count INT DEFAULT 1)"
            )
        except sqlite3.OperationalError as e:
            logger.error(f"Error creating table collections: {e}")
            raise
        try:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS Transfers(TransferID INTEGER PRIMARY KEY AUTOINCREMENT, CollectionIdentifier, BagUUID, TransferDate, PayloadOxum, ManifestSHA256Hash, TransferTimeSeconds)"
            )
        except sqlite3.OperationalError as e:
            logger.error(f"Error creating table transfers: {e}")
            raise


def insert_transfer(folder, bag: bagit.Bag, manifest_hash, copy_time, db_path):
    with get_db_connection(db_path) as con:
        cur = con.cursor()
        try:
            cur.execute(
                "INSERT INTO transfers (CollectionIdentifier, BagUUID, TransferDate, PayloadOxum, ManifestSHA256Hash, TransferTimeSeconds) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    folder,
                    bag.info[UUID_ID],  # UUID field
                    time.strftime("%Y%m%d"),
                    bag.info["Payload-Oxum"],
                    manifest_hash,
                    copy_time,
                ),
            )
        except sqlite3.DatabaseError as e:
            logger.error(f"Error inserting transfer record: {e}")
            raise  # Reraise the exception to handle it outside if necessary
        try:
            cur.execute(
                "INSERT INTO collections(CollectionIdentifier) VALUES(:id) ON CONFLICT (CollectionIdentifier) DO UPDATE SET count = count + 1",
                {"id": folder},
            )
        except sqlite3.DatabaseError as e:
            logger.error(f"Error inserting collections record: {e}")
            raise  # Reraise the exception to handle it outside if necessary


def get_count_collections_processed(primary_id, db_path):
    with get_db_connection(db_path) as con:
        cur = con.cursor()
        try:
            res = cur.execute(
                "SELECT * FROM collections WHERE CollectionIdentifier=:id",
                {"id": primary_id},
            )
            results = res.fetchall()
            if len(results) == 0:
                return 0
            if len(results) > 1:
                raise ValueError(
                    "Database count parsing error - only one identifier entry should exist."
                )
            else:
                return results[0][1]
        except sqlite3.DatabaseError as e:
            logger.error(f"Error getting count processed from id: {e}")
            raise


def timed_rsync_copy(folder, output_dir):
    start = time.perf_counter()
    # may need to add bandwith limit --bwlimit=1000
    try:
        result = subprocess.run(
            ["rsync", "-vrlt", "--checksum", f"{folder}/", output_dir], check=True
        )
        logger.info(result.stdout)
        if result.stderr:
            logger.error(result.stderr)
    except subprocess.CalledProcessError as e:
        logger.error(f"rsync failed for folder {folder} to {output_dir}")
        logger.error(f"Error: {e}")
        raise
    return time.perf_counter() - start


def compute_manifest_hash(folder):
    hash_sha256 = hashlib.sha256()
    file_path = os.path.join(folder, "manifest-sha256.txt")

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):  # Read in 4 KB chunks
            hash_sha256.update(chunk)

    return hash_sha256.hexdigest()


def cleanup_transfer(folder):
    shutil.rmtree(folder)
    if os.path.isfile(f"{folder}.ok"):
        os.remove(f"{folder}.ok")


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
        logger.info(f"Transfers to process: {len(ok_files)}")
        for file in ok_files:
            tf = TriggerFile(os.path.join(transfer_dir, file))
            if tf.validate():
                valid_transfers.append(tf)

    # set up database
    try:
        configure_db(database)
    except sqlite3.OperationalError as e:
        print(f"Error configuring database: {e}")

    with get_db_connection(database) as con:
        cur = con.cursor()

        for tf in valid_transfers:
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
                        count = get_count_collections_processed(folder, database)
                    except Exception as e:
                        continue
                    count += 1
                    # Copy to output directory
                    output_folder = os.path.join(
                        os.path.basename(os.path.normpath(folder)), f"t{count}"
                    )
                    output_dir = os.path.join(archive_dir, output_folder)

                    if not os.path.exists(output_dir):
                        os.makedirs(output_dir)

                    copy_time = timed_rsync_copy(folder, output_dir)

                output_bag = bagit.Bag(output_dir)

                # check copied bag is valid and if so update database
                if output_bag.is_valid():
                    try:
                        insert_transfer(folder, bag, manifest_hash, copy_time, database)
                        cleanup_transfer(folder)
                    except Exception as e:
                        logger.error(
                            f"Failed to insert transfer for folder {folder}: {e}"
                        )
                        continue
                else:
                    logger.error(
                        "Transferred bag was invalid. Removing transferred data."
                    )
                    os.rmdir(output_dir)
            else:
                logger.error(f"Error moving bag: {e}")


if __name__ == "__main__":
    main()
