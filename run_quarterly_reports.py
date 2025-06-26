import logging
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

    now =  datetime.now()

    year = now.strftime('%Y')
    month = now.strftime('%m')
    quater = "0"
    if month=="07":
        quater = "4"
    elif month=="10":
        quater = "1"
    elif month=="01":
        quater = "2"
    elif month=="04":
        quater = "3"

    start_date = now - timedelta(days=90) #this is a bit hacky but ok
    start_date = start_date.strftime('%Y-%m-01')
    print(f"Filtering report to dates after: {start_date}")
    logging.info(start_date)

    # pandas is used to filter the between dates, which defualts to inclusive
    # as a result we need to set the end date to the final day of quater
    # this should be yesterday
    end_date = now - timedelta(days=1)
    end_date = end_date.strftime('%Y-%m-%d')

    report_title = f"{year}_Q{quater}"

    logfilename = f"{report_title}_quarterly_reports.log"
    logfile = os.path.join(logging_dir, logfilename)
    logging.basicConfig(
        filename=logfile,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    runfile_check(logging_dir)

    # Build transfer report
    report_builder = Report(TransferReport())

    html = report_builder.build_report_between(transfer_db, start_date, end_date)

    transfer_report_file = os.path.join(
        report_dir, f"{report_title}_quarterly_transfer_report.html"
    )
    try:
        with open(transfer_report_file, "a") as f:
            f.write(html)
            print(f"Transfer report written to: {transfer_report_file}")
    except Exception as e:
        logger.error(f"Failed to write report file to {transfer_report_file}: {e}")

    runfile_cleanup(logging_dir)


if __name__ == "__main__":
    main()
