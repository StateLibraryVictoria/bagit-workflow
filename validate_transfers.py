import bagit
from datetime import datetime
import os
import sqlite3
from src.helper_functions import *
from src.database_functions import *

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
        "REPORT_DIR": os.getenv("REPORT_DIR"),
    }
    return config


def main():
    # load variables
    config = load_config()
    logging_dir = config.get("LOGGING_DIR")
    archive_dir = config.get("ARCHIVE_DIR")
    validation_db = config.get("VALIDATION_DB")
    report_dir = config.get("REPORT_DIR")

    runfile_check(logging_dir)

    logfilename = f"{time.strftime('%Y%m%d')}_bagit_validation_action.log"
    logfile = os.path.join(logging_dir, logfilename)
    logging.basicConfig(
        filename=logfile,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    try:
        configure_validation_db(validation_db)
    except sqlite3.OperationalError as e:
        print(f"Error configuring database: {e}")
        runflie_cleanup(logging_dir)

    # get list of transfers
    collections = os.listdir(archive_dir)

    # create the ValidationAction entry here. Get the Primary key to pass to the next function
    validation_action_begin = datetime.now()
    validation_action_id = start_validation(validation_action_begin, validation_db)

    for collection in collections:
        col_dir = os.path.join(archive_dir, collection)
        if os.path.isfile(col_dir):
            continue

        transfers = os.listdir(col_dir)
        for transfer in transfers:
            transfer_dir = os.path.join(col_dir, transfer)
            if not os.path.isdir(transfer_dir):
                continue
            validation_start_time = datetime.now()
            bag_path = os.path.join(archive_dir, collection, transfer_dir)
            try:
                bag = bagit.Bag(bag_path)
            except Exception as e:
                logger.error(f"Error validating bag {bag_path}: {e}")
                # update database with failure
                continue
            try:
                baguuid = bag.info[UUID_ID]
            except KeyError as e:
                logger.error(f"Error parsing UUID from bag {bag_path}: {e}")
                baguuid = None
            try:
                bag.validate()
                logger.info(f"Validated bag at: {bag_path}")
                errors = None
            except bagit.BagValidationError as e:
                logger.warning(
                    f"Error validating bag at {bag_path} with UUID {baguuid}: {e}"
                )
                errors = f"{e}"
            uuid_error = "Bag UUID not present in bag-info.txt"
            errors = (
                errors
                if baguuid is not None
                else (uuid_error if errors is None else errors + ";" + uuid_error)
            )
            validation_end_time = datetime.now()
            # ValidationActionId, BagUUID, Outcome, Errors, BagPath, StartTime, EndTime
            # update both tables to reflect bag validation outcome.
            insert_validation_outcome(
                validation_action_id,
                baguuid,
                errors == None,
                errors,
                bag_path,
                validation_start_time,
                validation_end_time,
                validation_db,
            )

    validation_action_end = datetime.now()
    end_validation(validation_action_id, validation_action_end, validation_db)

    # build a basic report and output to html.
    report_date = time.strftime("%Y%m%d")
    html_start = html_header("Validation Report")
    html_action = return_db_query_as_html(
        validation_db,
        f"SELECT * from ValidationActions WHERE ValidationActionsId={validation_action_id}",
    )
    html_outcome = return_db_query_as_html(
        validation_db,
        f"SELECT * from ValidationOutcome WHERE ValidationActionsId={validation_action_id}",
    )

    report_file = os.path.join(report_dir, f"validation_report_{report_date}.html")
    try:
        with open(report_file, "a") as f:
            f.write(html_start)
            f.write("<body>")
            f.write("<h2>Report Overview</h2>")
            f.write(html_action)
            f.write("<h2>Validation Outcomes</h2>")
            f.write(html_outcome)
            f.write("</body></html>")
    except Exception as e:
        logger.error(f"Failed to write report file to {report_file}: {e}")

    runflie_cleanup(logging_dir)


if __name__ == "__main__":
    main()
