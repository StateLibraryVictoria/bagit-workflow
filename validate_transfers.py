import bagit
from datetime import datetime, timezone
import os
import sqlite3
from helper_functions import *
from database_functions import *

logger = logging.getLogger(__name__)

# metadata tags
PRIMARY_ID = "External-Identifier"
UUID_ID = "Internal-Sender-Identifier"
CONTACT = "Contact-Name"
EXTERNAL_DESCRIPTION = "External-Description"

def load_config():
    config = {
        "ARCHIVE_DIR": os.getenv("ARCHIVE_DIR"),
        "LOGGING_DIR": os.getenv("LOGGING_DIR"),
        "VALIDATION_DB": os.getenv("VALIDATION_DB"),
        "DATABASE": os.getenv("DATABASE"),
    }
    return config


def main():
    # load variables
    config = load_config()
    logging_dir = config.get("LOGGING_DIR")
    archive_dir = config.get("ARCHIVE_DIR")
    validation_db = config.get("VALIDATION_DB")
    transfer_db = config.get("DATABASE")

    try:
        configure_validation_db(validation_db)
    except sqlite3.OperationalError as e:
        print(f"Error configuring database: {e}")

    with get_db_connection(validation_db) as con:
        cur = con.cursor()

    # get list of transfers
    collections = os.listdir(archive_dir)


    start_validation_action = datetime.now(timezone.utc())
    # create the ValidationAction entry here. Get the Primary key to pass to the next function
    validation_action_id = str(uuid.uuid4())

    for collection in collections:
        transfers = os.listdir(collection)
        for transfer in transfers:
            validation_start_time = datetime.now(timezone.utc())
            bag_path = os.path.join(archive_dir,collection,transfer)
            try:
                bag = bagit.Bag(bag_path)
            except Exception as e:
                logger.error(f"Error validating bag {bag_path}: {e}")
                # update database with failure
                continue
            baguuid = bag.info[UUID_ID]
            try:
                bag.validate()
                errors = None
            except bagit.BagValidationError as e:
                errors = f"{e}"
            validation_end_time = datetime.now(timezone.utc())
            # ValidationActionId, BagUUID, Outcome, Errors, BagPath, StartTime, EndTime
            # update both databases
            insert_validation_outcome(validation_action_id, baguuid, str(errors==None), errors, bag_path, validation_start_time, validation_end_time, validation_db)
    
    end_validation_action = datetime.now(timezone.utc())
    # ValidationActionId, CountBagsValidated, CountBagsWithErrors, StartTime, EndTime, Status [completed, failed]
    # add end time to validation action event.


if __name__ == "__main__":
    main()
