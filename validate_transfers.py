import bagit
from datetime import datetime, timezone
import os
import pandas as pd
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

    # get list of transfers
    collections = os.listdir(archive_dir)

    # create the ValidationAction entry here. Get the Primary key to pass to the next function
    validation_action_begin = datetime.now(timezone.utc)
    validation_action_id = start_validation(validation_action_begin,validation_db)
    

    for collection in collections:
        col_dir = os.path.join(archive_dir, collection)
        if os.path.isfile(col_dir):
            continue
        
        transfers = os.listdir(col_dir)
        for transfer in transfers:
            transfer_dir = os.path.join(col_dir, transfer)
            if not os.path.isdir(transfer_dir):
                continue
            validation_start_time = datetime.now(timezone.utc)
            bag_path = os.path.join(archive_dir,collection,transfer_dir)
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
                logger.warning(f"Error validating bag at {bag_path} with UUID {baguuid}: {e}")
                errors = f"{e}"
            uuid_error = "Bag UUID not present in bag-info.txt"
            errors = errors if baguuid is not None else (uuid_error if errors is None else errors +";"+ uuid_error)
            validation_end_time = datetime.now(timezone.utc)
            # ValidationActionId, BagUUID, Outcome, Errors, BagPath, StartTime, EndTime
            # update both tables to reflect bag validation outcome.
            insert_validation_outcome(validation_action_id, baguuid, errors==None, errors, bag_path, validation_start_time, validation_end_time, validation_db)
    
    validation_action_end = datetime.now(timezone.utc)
    end_validation(validation_action_id, validation_action_end, validation_db)

    # build a basic report and output to html.
    with get_db_connection(validation_db) as con:
        try:
            df = pd.read_sql_query(f"SELECT * from ValidationActions WHERE ValidationActionsId={validation_action_id}", con)
        except Exception as e:
            logger.error(f"Error reading ValidationActions from database: {e}")
        try:
            df2 = pd.read_sql_query(f"SELECT * from ValidationOutcome WHERE ValidationActionsId={validation_action_id}",con)
        except Exception as e:
            logger.error(f"Error reading ValidationOutcome database: {e}")

    html_start = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Validation Report</title>
    <style>
	body {
	  font-family: "Andale Mono", monospace;
	}
	</style>
</head>
<body>"""

    html_top=f"<h1>Validation Report: {validation_action_begin}</h1>"
    html_logfile=f"<p>More information available in {logfilename}</p>"
    html_overview = "<h2>Report Overview</h2>"
    html_detail = "<h2>Validation Outcomes</h2>"
    html_end = """</body>
</html>"""

    report_file=os.path.join(report_dir,"validation_report.html")
    try:
        with open(report_file, 'a') as f:
            f.write(html_start)
            f.write(html_top)
            f.write(html_logfile)
            f.write(html_overview)
            f.write(df.to_html())
            f.write(html_detail)
            f.write(df2.to_html())
            f.write(html_end)
    except Exception as e:
        logger.error(f"Failed to write report file to {report_file}: {e}")


if __name__ == "__main__":
    main()
