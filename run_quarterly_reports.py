import logging
import shutil
from datetime import datetime, timedelta
from src.shared_constants import *
from src.database_functions import *
from src.helper_functions import *
from src.report_functions import *

logger = logging.getLogger(__name__)

"""
This script is configured to run quarterly reporting based on a scheduled task that should run on the following dates:
    - 1 July (Q4)
    - 1 October (Q1)
    - 1 January (Q2)
    - 1 April (Q3)
"""


def main():
    # load variables
    config = load_config()
    logging_dir = config.get("LOGGING_DIR")
    transfer_db = config.get("DATABASE")
    report_dir = config.get("REPORT_DIR")
    archive_dir = config.get("ARCHIVE_DIR")
    validation_db = config.get("VALIDATION_DB")
    access_dir = config.get("TRANSFER_DIR")

    now =  datetime.now()
    # make sure it's the first of the month
    now = datetime.strptime(now.strftime('%Y-%m-01'),'%Y-%m-%d')

    year = now.strftime('%Y')
    month = now.strftime('%m')

    start_date = now - timedelta(days=90) #this is a bit hacky but ok
    start_date = start_date.strftime('%Y-%m-01')
    print(f"Filtering report to dates after: {start_date}")

    # pandas is used to filter the between dates, which defualts to inclusive
    # as a result we need to set the end date to the final day of quater
    # this should be yesterday
    end = now - timedelta(days=1)
    end_date = end.strftime('%Y-%m-%d')

    if month=="07":
        quater = "Q4"
    elif month=="10":
        quater = "Q1"
    elif month=="01":
        quater = "Q2"
    elif month=="04":
        quater = "Q3"
    else:
        end_month = end.strftime('%m')
        quater = f"{month}-{end_month}"

    report_title = f"{year}_{quater}"

    logfilename = f"{report_title}_quarterly_reports.log"

    print(os.path.join(logging_dir, logfilename))
    logfile = os.path.join(logging_dir, logfilename)
    logging.basicConfig(
        filename=logfile,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logging.info("Started processing quarterly report for " + quater)
    logging.info(f"Dates between {start_date} and {end_date}")

    runfile_check(logging_dir)

    # Build transfer report
    report_builder = Report(TransferReport())

    html = report_builder.build_report_between(transfer_db, start_date, end_date)

    transfer_report_filename = f"{report_title}_quarterly_transfer_report.html"
    transfer_report_file = os.path.join(
        report_dir, transfer_report_filename
    )
    try:
        with open(transfer_report_file, "a") as f:
            f.write(html)
            print(f"Transfer report written to: {transfer_report_file}")
    except Exception as e:
        logger.error(f"Failed to write report file to {transfer_report_file}: {e}")

    
    # Build validation report
    try:
        configure_validation_db(validation_db)
    except sqlite3.OperationalError as e:
        print(f"Error configuring database: {e}")
        runfile_cleanup(logging_dir)

    # run validation process and get id for report
    validation_action_id = run_validation(validation_db, transfer_db, archive_dir)

    # build a basic report and output to html.
    report = Report(ValidationReport())
    html = report.build_basic_report(validation_db, validation_action_id)

    validation_report_filename = f"{report_title}_quarterly_validation_report.html"
    validation_report_file = os.path.join(report_dir, validation_report_filename)
    try:
        with open(validation_report_file, "a") as f:
            f.write(html)
            print(f"Validation report written to: {validation_report_file}")
    except Exception as e:
        logger.error(f"Failed to write report file to {validation_report_file}: {e}")

    ## Copy reports to access directory
    shutil.copy2(transfer_report_file, os.path.join(access_dir, transfer_report_filename))
    shutil.copy2(validation_report_file, os.path.join(access_dir, validation_report_filename))

    runfile_cleanup(logging_dir)


if __name__ == "__main__":
    main()
