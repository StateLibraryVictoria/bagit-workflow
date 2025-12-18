import logging
from src.database_functions import *
from src.helper_functions import runfile_check, runfile_cleanup

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
    validation_db = config.get("VALIDATION_DB")
    transfer_db = config.get("DATABASE")
    report_dir = config.get("REPORT_DIR")

    logfilename = f"{time.strftime('%Y%m%d')}_database_dump.log"
    logfile = os.path.join(logging_dir, logfilename)
    logging.basicConfig(
        filename=logfile,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info(validation_db)
    logger.info(transfer_db)

    database_dir = os.path.dirname(transfer_db)
    runfile_check(database_dir)

    transfer_tables = ["Collections", "Transfers"]
    validation_tables = ["ValidationActions", "ValidationOutcome"]
    html = dump_database_tables_to_html(
        db_paths={"transfer": transfer_db, "validation": validation_db},
        db_tables={"transfer": transfer_tables, "validation": validation_tables},
    )

    report_file = os.path.join(report_dir, "full_data_dump.html")
    try:
        with open(report_file, "a") as f:
            f.write(html)
    except Exception as e:
        logger.error(f"Failed to write report file to {report_file}: {e}")

    runfile_cleanup(database_dir)


if __name__ == "__main__":
    main()
