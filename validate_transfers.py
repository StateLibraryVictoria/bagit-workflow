from datetime import datetime
import os
import sqlite3
from src.helper_functions import *
from src.database_functions import *
from src.report_functions import *

logger = logging.getLogger(__name__)


def main():
    # load variables
    config = load_config()
    logging_dir = config.get("LOGGING_DIR")
    archive_dir = config.get("ARCHIVE_DIR")
    validation_db = config.get("VALIDATION_DB")
    transfer_db = config.get("DATABASE")
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
        runfile_cleanup(logging_dir)

    # get list of transfers
    collections = os.listdir(archive_dir)

    # add variable to track which paths have been checked in transfers db
    db_paths_checked = set()

    # create the ValidationAction entry here. Get the Primary key to pass to the next function
    validation_action_begin = datetime.now()
    validation_action_id = start_validation(validation_action_begin, validation_db)

    for collection in collections:
        col_dir = os.path.join(archive_dir, collection)

        # skip things that are just files in the top-level directory
        if os.path.isfile(col_dir):
            continue

        # get a list of subfolders
        transfers = os.listdir(col_dir)

        # iterate through each transfer
        for transfer in transfers:

            # build the path
            transfer_dir = os.path.join(col_dir, transfer)

            # start tracking validation time
            validation_start_time = datetime.now()

            # run the validation process
            try:
                validation_status = ValidationStatus(
                    transfer_db, "transfers", transfer_dir, archive_dir
                )
            except ValueError as e:
                logger.error(f"ValueError: {e}")
                continue

            # assign set variables from validation
            bag_uuid = validation_status.get_bag_uuid()
            errors = validation_status.get_error_string()
            outcome = validation_status.is_valid()

            # now the validation process is done
            validation_end_time = datetime.now()

            # update both tables to reflect bag validation outcome.
            insert_validation_outcome(
                validation_action_id,
                bag_uuid,
                outcome,
                errors,
                transfer_dir,
                validation_start_time,
                validation_end_time,
                validation_db,
            )

            # log directory as checked
            relative_path = validation_status.get_relative_path()
            logger.info(f"Checked transfer at {relative_path} with outcome {outcome}")
            db_paths_checked.add(relative_path)

    # find any transfers in the database that weren't on the filesystem
    # add a row and validation error for each
    with get_db_connection(transfer_db) as tbd:
        cur = tbd.cursor()
        params = ["? " for i in range(len(db_paths_checked))]
        query_time = datetime.now()
        select_statement = (
            "SELECT TransferID, BagUUID, OutcomeFolderTitle, OriginalFolderTitle, TransferDate, ContactName "
            + f"from transfers WHERE OutcomeFolderTitle not in ({','.join(params)})"
        )
        try:
            result = cur.execute(select_statement, list(db_paths_checked))
            matches = result.fetchall()
            if len(matches) == 0:
                logger.info("No unmatched transfers in database.")
            else:
                for match in matches:
                    insert_validation_outcome(
                        validation_action_id,
                        match[1],
                        False,
                        f"Transfer {match[0]} in database but not found on system. Submitted on {match[4]} by {match[5]} in folder {match[3]}.",
                        match[2],
                        query_time,
                        query_time,
                        validation_db,
                    )
        except Exception as e:
            logger.error(f"Error connecting to database. {e}")

    validation_action_end = datetime.now()
    end_validation(validation_action_id, validation_action_end, validation_db)

    # build a basic report and output to html.
    report = Report(ValidationReport())
    report_date = time.strftime("%Y%m%d")
    html = report.build_basic_report(validation_db, validation_action_id)

    report_file = os.path.join(report_dir, f"validation_report_{report_date}.html")
    try:
        with open(report_file, "a") as f:
            f.write(html)
            print(f"Validation report written to: {report_file}")
    except Exception as e:
        logger.error(f"Failed to write report file to {report_file}: {e}")

    runfile_cleanup(logging_dir)


if __name__ == "__main__":
    main()
