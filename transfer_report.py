import logging
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
        "DATABASE": os.getenv("DATABASE"),
        "REPORT_DIR": os.getenv("REPORT_DIR"),
    }
    return config


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

    transfer_tables = ["Collections", "Transfers"]
    validation_tables = ["ValidationActions", "ValidationOutcome"]
    html = dump_database_tables_to_html(
        title="Transfer Report",
        db_paths={"transfer": transfer_db, "validation": None},
        db_tables={"transfer": transfer_tables, "validation": validation_tables},
    )

    report_file = os.path.join(
        report_dir, f"transfer_report_{time.strftime('%Y%m%d')}.html"
    )
    try:
        with open(report_file, "a") as f:
            f.write(html)
    except Exception as e:
        logger.error(f"Failed to write report file to {report_file}: {e}")


if __name__ == "__main__":
    main()