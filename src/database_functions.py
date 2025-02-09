import bagit
import sqlite3
import time
import pandas as pd
from contextlib import contextmanager
from src.shared_constants import *
from src.helper_functions import validate_bag_at


class ValidationStatus:
    def __init__(self, db_path, table_name, transfer_path, archive_dir):
        self.db_path = db_path
        self.table_name = table_name
        self.transfer_path = transfer_path
        self.archive_dir = archive_dir
        self.bag_uuid = None
        self.errors = []
        self.valid = self._validate()

    def get_relative_path(self):
        relative_path = os.path.relpath(self.transfer_path, self.archive_dir)
        return relative_path

    def get_error_string(self) -> str:
        return ";".join(self.errors)

    def get_bag_uuid(self) -> str:
        return self.bag_uuid

    def is_valid(self):
        return self.valid

    def _validate(self) -> bool:
        if not os.path.isdir(self.transfer_path):
            raise ValueError("Transfer path must be a directory.")

        self._validate_as_bag()
        self._validate_in_database()
        if len(self.errors) == 0:
            return True
        else:
            return False

    def _validate_as_bag(self) -> None:
        baguuid, errors = validate_bag_at(self.transfer_path)
        baguuid = ";".join(baguuid)
        if self.bag_uuid is None:
            self.bag_uuid = baguuid
        else:
            if self.bag_uuid != baguuid:
                errors.append(
                    f"Bag UUID already parsed and different from current: {baguuid} verses {self.bag_uuid}"
                )
        self.errors.extend(errors)

    def _validate_in_database(self) -> None:
        relative_path = self.get_relative_path()
        with get_db_connection(self.db_path) as tbd:
            cur = tbd.cursor()
            try:
                result = cur.execute(
                    "SELECT TransferID, BagUUID from transfers WHERE OutcomeFolderTitle=?",
                    [relative_path],
                )
                matches = result.fetchall()
                if len(matches) == 0:
                    logger.error(
                        f"Bag path incorrectly recorded in database 0 matched records."
                    )
                    self.errors.append("Bag path not found in transfers database.")
                elif len(matches) == 1:
                    db_uuid = matches[0][1]
                    if self.bag_uuid != db_uuid:
                        self.errors.append(
                            f"UUID conflict in database for transfer {matches[0][0]} with UUID {db_uuid}"
                        )
                else:
                    transfers = [", ".join(match) for match in matches]
                    self.errors.append(
                        f"Too many transfers in database: {'; '.join(transfers)}"
                    )
            except Exception as e:
                logger.error(f"Error connecting to transfers database: {e}")


def get_count_collections_processed(primary_id, db_path):
    with get_db_connection(db_path) as con:
        cur = con.cursor()
        try:
            res = cur.execute(
                "SELECT * FROM collections WHERE CollectionIdentifier=:id",
                {"id": primary_id},
            )
            results = res.fetchall()
            if len(results) == 0:
                return 0
            if len(results) > 1:
                raise ValueError(
                    "Database count parsing error - only one identifier entry should exist."
                )
            else:
                return results[0][1]
        except sqlite3.DatabaseError as e:
            logger.error(f"Error getting count processed from id: {e}")
            raise


@contextmanager
def get_db_connection(db_path):
    con = sqlite3.connect(db_path)
    try:
        yield con
    except sqlite3.DatabaseError as e:
        con.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        con.commit()
        con.close()


def configure_transfer_db(database_path):
    with get_db_connection(database_path) as con:
        cur = con.cursor()
        try:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS Collections(CollectionIdentifier PRIMARY KEY, Count INT DEFAULT 1)"
            )
        except sqlite3.OperationalError as e:
            logger.error(f"Error creating table collections: {e}")
            raise
        try:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS Transfers(TransferID INTEGER PRIMARY KEY AUTOINCREMENT, CollectionIdentifier, BagUUID, TransferDate, BagDate, PayloadOxum, ManifestSHA256Hash, StartTime, EndTime, OriginalFolderTitle, OutcomeFolderTitle, ContactName, SourceOrganisation)"
            )
        except sqlite3.OperationalError as e:
            logger.error(f"Error creating table transfers: {e}")
            raise


def configure_validation_db(database_path):
    with get_db_connection(database_path) as con:
        cur = con.cursor()
        try:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS ValidationActions(ValidationActionsId INTEGER PRIMARY KEY AUTOINCREMENT, CountBagsValidated INT, CountBagsWithErrors INT, StartAction, EndAction, Status)"
            )
        except sqlite3.OperationalError as e:
            logger.error(f"Error creating table ValidationActions: {e}")
            raise
        try:  # ValidationActionId, BagUUID, Outcome, Errors, BagPath, StartTime, EndTime
            cur.execute(
                "CREATE TABLE IF NOT EXISTS ValidationOutcome(OutcomeIdentifier INTEGER PRIMARY KEY AUTOINCREMENT, ValidationActionsId, BagUUID, Outcome, Errors, BagPath, StartTime, EndTime)"
            )
        except sqlite3.OperationalError as e:
            logger.error(f"Error creating table ValidationOutcome: {e}")
            raise


def start_validation(begin_time, db_path):
    with get_db_connection(db_path) as con:
        cur = con.cursor()
        try:
            cur.execute(
                "INSERT INTO ValidationActions(CountBagsValidated, CountBagsWithErrors, StartAction, EndAction, Status) VALUES (?, ?, ?, ?, ?)",
                (0, 0, begin_time, None, "Running"),
            )
        except sqlite3.DatabaseError as e:
            logger.error(f"Error inserting record into ValidationOutcome table: {e}")
            return None
        try:
            i = cur.execute(
                "SELECT ValidationActionsId FROM ValidationActions ORDER BY ValidationActionsId DESC"
            )
            identifier = i.fetchone()[0]
            return identifier
        except sqlite3.DatabaseError as e:
            logger.error(f"Error inserting record into ValidationActions table: {e}")
            return None


def end_validation(validation_action_id, end_time, db_path):
    with get_db_connection(db_path) as con:
        cur = con.cursor()
        try:
            cur.execute(
                "UPDATE ValidationActions SET EndAction =?, Status='Complete' WHERE ValidationActionsId =?",
                (
                    end_time,
                    validation_action_id,
                ),
            )
        except sqlite3.DatabaseError as e:
            logger.error(f"Error inserting record into ValidationOutcome table: {e}")
            return None
        try:
            i = cur.execute(
                "SELECT ValidationActionsId FROM ValidationActions ORDER BY ValidationActionsId DESC"
            )
            identifier = i.fetchone()[0]
            return identifier
        except sqlite3.DatabaseError as e:
            logger.error(f"Error inserting record into ValidationActions table: {e}")
            return None


def insert_validation_outcome(
    validation_action_id,
    baguuid,
    outcome,
    errors,
    bag_path,
    validation_start_time,
    validation_end_time,
    db_path,
):
    with get_db_connection(db_path) as con:
        cur = con.cursor()
        if outcome:
            try:
                cur.execute(
                    "UPDATE ValidationActions SET CountBagsValidated = CountBagsValidated + 1 WHERE ValidationActionsId =?",
                    (validation_action_id,),
                )
            except sqlite3.DatabaseError as e:
                logger.error(
                    f"Error updating record into ValidationActions table for action {validation_action_id}: {e}"
                )
        else:
            try:
                cur.execute(
                    "UPDATE ValidationActions SET CountBagsWithErrors = CountBagsWithErrors + 1 WHERE ValidationActionsId =?",
                    (validation_action_id,),
                )
            except sqlite3.DatabaseError as e:
                logger.error(
                    f"Error updating record into ValidationActions table for action {validation_action_id}: {e}"
                )
        outcome = "Pass" if outcome else "Fail"
        try:
            cur.execute(
                "INSERT INTO ValidationOutcome(ValidationActionsId, BagUUID, Outcome, Errors, BagPath, StartTime, EndTime) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    validation_action_id,
                    baguuid,
                    outcome,
                    errors,
                    bag_path,
                    validation_start_time,
                    validation_end_time,
                ),
            )
        except sqlite3.DatabaseError as e:
            logger.error(f"Error inserting record into ValidationOutcome table: {e}")


def insert_transfer(
    output_folder,
    bag: bagit.Bag,
    primary_id,
    manifest_hash,
    start_time,
    end_time,
    db_path,
):
    """Contains all the required information to log a transfer to the database.

    Keyword arguments:
    output_folder -- final output location relative to archive directory
    bag -- the bag being transferred, which provides most of the metadata
    primary_id -- the collection identifier or preliminary identifier for the material
    manifest_hash -- a checksum value for a specific manifest file for deduplication
    start_time -- when the transfer commenced
    end_time -- when the transfer completed
    db_path -- path to the database"""
    collection_id = primary_id
    with get_db_connection(db_path) as con:
        cur = con.cursor()
        try:
            cur.execute(
                "INSERT INTO transfers (CollectionIdentifier, BagUUID, TransferDate, BagDate, PayloadOxum, ManifestSHA256Hash, StartTime, EndTime, OriginalFolderTitle, OutcomeFolderTitle, ContactName, SourceOrganisation) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    collection_id,
                    bag.info[UUID_ID],  # UUID field
                    time.strftime("%Y-%m-%d"),
                    bag.info[BAGGING_DATE],
                    bag.info["Payload-Oxum"],
                    manifest_hash,
                    start_time,
                    end_time,
                    bag.info.get(EXTERNAL_DESCRIPTION, "Not recorded"),
                    output_folder,
                    bag.info.get(CONTACT, "Not recorded"),
                    bag.info.get(SOURCE_ORGANIZATION, "Not recorded"),
                ),
            )
        except sqlite3.DatabaseError as e:
            logger.error(f"Error inserting transfer record: {e}")
            raise  # Reraise the exception to handle it outside if necessary
        try:
            cur.execute(
                "INSERT INTO collections(CollectionIdentifier) VALUES(:id) ON CONFLICT (CollectionIdentifier) DO UPDATE SET count = count + 1",
                {"id": collection_id},
            )
        except sqlite3.DatabaseError as e:
            logger.error(f"Error inserting collections record: {e}")
            raise  # Reraise the exception to handle it outside if necessary


def html_header(title: str):
    header = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>{title}</title>
    <style>
	body {{
	  font-family: "Andale Mono", monospace;
	}}
	</style>
</head>"""
    return header


def dump_database_tables_to_html(
    title: str = "Data Archive Report",
    db_paths: dict = {"transfer": None, "validation": None},
    db_tables: dict = {"transfer": [], "validation": []},
) -> str:
    """Outputs specified transfer and validation databases tables as HTML.
    Requires the table names to be specifically entered.
    """
    html_start = html_header(title)
    html_body = "<body>"
    for database in ["transfer", "validation"]:
        if db_paths.get(database) is not None:
            tables = db_tables.get(database)
            if tables is not None:
                html_body += f"<h2>Records of {database}</h2>"
                with get_db_connection(db_paths.get(database)) as con:
                    for table in tables:
                        df = pd.read_sql_query(f"SELECT * from {table}", con)
                        html_body += f"<h2>Contents of table {table}</h2>"
                        html_body += df.to_html()

    html_end = """</body>
</html>"""
    html = html_start + html_body + html_end
    return html


def return_db_query_as_html(db_path: str, sql_query: str):
    with get_db_connection(db_path) as con:
        df = pd.read_sql_query(sql_query, con)
    return df.to_html()
