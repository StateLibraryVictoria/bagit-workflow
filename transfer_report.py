import logging
from src.shared_constants import *
from src.database_functions import *
from src.helper_functions import *
from src.report_functions import *

logger = logging.getLogger(__name__)


def main():
    # load variables
    config = load_config()
    logging_dir = config.get("LOGGING_DIR")
    transfer_db = config.get("DATABASE")
    report_dir = config.get("REPORT_DIR")

    logfilename = f"{time.strftime('%Y%m%d')}_transfer_report.log"
    logfile = os.path.join(logging_dir, logfilename)
    logging.basicConfig(
        filename=logfile,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    runfile_check(logging_dir)

    report_builder = Report(TransferReport())

    html = report_builder.build_basic_report(transfer_db)

    report_file = os.path.join(
        report_dir, f"transfer_report_{time.strftime('%Y%m%d')}.html"
    )
    try:
        with open(report_file, "a") as f:
            f.write(html)
    except Exception as e:
        logger.error(f"Failed to write report file to {report_file}: {e}")

    runfile_cleanup(logging_dir)


if __name__ == "__main__":
    main()
