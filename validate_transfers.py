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

    # run validation process and get id for report
    validation_action_id = run_validation(validation_db, transfer_db, archive_dir)

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
