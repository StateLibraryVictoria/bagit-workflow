import logging
import sqlite3
from contextlib import contextmanager
from time import strftime
from shutil import move
import os

database = os.getenv("DATABASE")
archive_dir = os.getenv("ARCHIVE_DIR")
logging_dir = os.getenv("LOGGING_DIR")
logfile_name = os.path.join(
    logging_dir, f"update_collection_id_{strftime('%Y%m%d%H%M%S')}.log"
)

bag_id = os.getenv("BAG_UUID")
old_id = os.getenv("OLD_ID")
new_id = os.getenv("NEW_ID")


def configure_logging(filename: str, debug_status: bool) -> None:
    if debug_status:
        print("Debugging Run.")
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(filename), logging.StreamHandler()],
        )
        sqlite3.enable_callback_tracebacks(True)
    else:
        logging.basicConfig(
            filename=filename,
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )


configure_logging(logfile_name, True)


@contextmanager
def get_db_connection(db_path):
    con = sqlite3.connect(db_path)
    try:
        yield con
    except sqlite3.DatabaseError as e:
        con.rollback()
        logging.error(f"Database error: {e}")
        raise
    finally:
        con.commit()
        con.close()


def main():
    if not os.path.exists(archive_dir):
        logging.error("Archive directory does not exist.")
        return

    transfer_data = None

    with get_db_connection(database) as con:
        cur = con.cursor()
        res = cur.execute(
            "SELECT CollectionIdentifier, OutcomeFolderTitle FROM Transfers WHERE BagUUID=:id",
            {"id": bag_id},
        )
        transfer_data = res.fetchall()
        current_id = transfer_data[0][0]
        current_path = transfer_data[0][1]
        if current_id != old_id:
            logging.error(
                f"Collection identifier for bag does not match old id {old_id}. Id returned: {current_id}"
            )
            raise ValueError("Failed to match identifiers.")

        # get data from Collection table
        res = cur.execute(
            "SELECT count from collections WHERE CollectionIdentifier=:id",
            {"id": old_id},
        )
        results = res.fetchall()
        old_count = results[0][0]

        res = cur.execute(
            "SELECT count from collections WHERE CollectionIdentifier=:id",
            {"id": new_id},
        )
        results = res.fetchall()
        if len(results) == 0:
            new_count = 0
        else:
            new_count = int(results[0][0])

        new_path = f"{new_id}\\t{new_count+1}"
        old_filepath = os.path.join(archive_dir, current_path)
        new_filepath = os.path.join(archive_dir, new_path)
        logging.info(f"Process will set new identifier for bag {bag_id}")
        logging.info(f"Change count for old id from {old_count} to {old_count - 1}")
        logging.info(f"Change count for new id from {new_count} to {new_count + 1}")
        logging.info(f"Move collection from {current_path} to {new_path}")
        logging.info(f"Update CollectionIdentifer from {old_id} to {new_id}")
        logging.info(f"Update OutcomeFolderTitle from {old_filepath} to {new_filepath}")
        logging.info(f"Archive directory: {archive_dir}")
        res = cur.execute(
            "SELECT * from transfers where CollectionIdentifier=:id AND BagUUID<>:bagID",
            {"id": old_id, "bagID": bag_id},
        )
        results = res.fetchall()
        logging.info("The following entries will remain unchanged:")
        logging.info(results)
        print("Enter Y to proceed with transformation:")
        answer = input()
        if answer != "Y":
            logging.info("Change cancelled.")
            return
        logging.info("Change confirmed.")

        # update collections table
        cur.execute(
            "UPDATE collections SET count =? WHERE CollectionIdentifier =?",
            (int(old_count - 1), old_id),
        )
        cur.execute(
            "INSERT INTO collections(CollectionIdentifier) VALUES(:id) ON CONFLICT (CollectionIdentifier) DO UPDATE SET count = count + 1",
            {"id": new_id},
        )

        # update transfers table
        cur.execute(
            "UPDATE transfers SET CollectionIdentifier =?, OutcomeFolderTitle =? WHERE BagUUID =?",
            (new_id, new_path, bag_id),
        )

        # move files

        move(old_filepath, new_filepath)


if __name__ == "__main__":
    main()
