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

    with get_db_connection(validation_db) as con:
        cur = con.cursor()

    # get list of transfers
    collections = os.listdir(archive_dir)

    # create the ValidationAction entry here. Get the Primary key to pass to the next function
    validation_action_begin = datetime.now(timezone.utc())
    validation_action_id = start_validation(validation_action_begin,validation_db)
    

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
            # update both tables to reflect bag validation outcome.
            insert_validation_outcome(validation_action_id, baguuid, str(errors==None), errors, bag_path, validation_start_time, validation_end_time, validation_db)
    
    validation_action_end = datetime.now(timezone.utc())
    end_validation(validation_action_id, validation_action_end, validation_db)

    # build a basic report and output to html.
    with get_db_connection(validation_db) as con:
        df = pd.read_sql_query(f"SELECT * from ValidationActions WHERE ValidationActionsId={validation_action_id}", con)
        df2 = pd.read_sql_query(f"SELECT * from ValiationOutcome WHERE ValidationActionsId={validation_action_id}",con)

    html_start = """<!DOCTYPE html>
    <html>
        <head>
            <title>Validation Report</title>
        </head>
    <body>"""

    html_top=f"<h1>Validation Report: {start_validation}</h1>"
    html_logfile=f"<p>More information available in {logfilename}</p>"
    html_overview = "<h2>Report Overview</h2>"
    html_detail = "<h2>Validation Outcomes</h2>"
    html_end = """</body>
</html>"""

    report_file=os.path.join(report_dir,"validation_report.html")
    with open(report_file, 'a') as f:
        f.write(html_top)
        f.write(html_logfile)
        f.write(html_overview)
        f.write(df.to_html())
        f.write(html_detail)
        f.write(df2.to_html())
        f.write(html_end)


if __name__ == "__main__":
    main()
