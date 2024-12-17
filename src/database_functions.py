import bagit
import sqlite3
import time
import pandas as pd
from contextlib import contextmanager
from src.shared_constants import *


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
                "CREATE TABLE IF NOT EXISTS Transfers(TransferID INTEGER PRIMARY KEY AUTOINCREMENT, CollectionIdentifier, BagUUID, TransferDate, PayloadOxum, ManifestSHA256Hash, TransferTimeSeconds)"
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
                (
                    0,
                    0, 
                    begin_time,
                    None,
                    "Running"
                ),
            )
        except sqlite3.DatabaseError as e:
            logger.error(f"Error inserting record into ValidationOutcome table: {e}")
            return None
        try:
            i = cur.execute("SELECT ValidationActionsId FROM ValidationActions ORDER BY ValidationActionsId DESC")
            identifier = i.fetchone()[0]
            return identifier
        except sqlite3.DatabaseError as e:
            logger.error(f"Error inserting record into ValidationActions table: {e}")
            return None
        
def end_validation(validation_action_id, end_time, db_path):
    with get_db_connection(db_path) as con:
        cur = con.cursor()
        try:
            cur.execute("UPDATE ValidationActions SET EndAction =?, Status='Complete' WHERE ValidationActionsId =?", 
                            (end_time, validation_action_id,))
        except sqlite3.DatabaseError as e:
            logger.error(f"Error inserting record into ValidationOutcome table: {e}")
            return None
        try:
            i = cur.execute("SELECT ValidationActionsId FROM ValidationActions ORDER BY ValidationActionsId DESC")
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
                cur.execute("UPDATE ValidationActions SET CountBagsValidated = CountBagsValidated + 1 WHERE ValidationActionsId =?", 
                            (validation_action_id,))
            except sqlite3.DatabaseError as e:
                logger.error(f"Error updating record into ValidationActions table for action {validation_action_id}: {e}")
        else: 
            try:
                cur.execute("UPDATE ValidationActions SET CountBagsWithErrors = CountBagsWithErrors + 1 WHERE ValidationActionsId =?", 
                            (validation_action_id,))
            except sqlite3.DatabaseError as e:
                logger.error(f"Error updating record into ValidationActions table for action {validation_action_id}: {e}")
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
    folder, bag: bagit.Bag, primary_id, manifest_hash, copy_time, db_path
):
    collection_id = primary_id
    with get_db_connection(db_path) as con:
        cur = con.cursor()
        try:
            cur.execute(
                "INSERT INTO transfers (CollectionIdentifier, BagUUID, TransferDate, PayloadOxum, ManifestSHA256Hash, TransferTimeSeconds) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    collection_id,
                    bag.info[UUID_ID],  # UUID field
                    time.strftime("%Y%m%d"),
                    bag.info["Payload-Oxum"],
                    manifest_hash,
                    copy_time,
                ),
            )
        except sqlite3.DatabaseError as e:
            logger.error(f"Error inserting transfer record: {e}")
            raise  # Reraise the exception to handle it outside if necessary
        try:
            cur.execute(
                "INSERT INTO collections(CollectionIdentifier) VALUES(:id) ON CONFLICT (CollectionIdentifier) DO UPDATE SET count = count + 1",
                {"id": folder},
            )
        except sqlite3.DatabaseError as e:
            logger.error(f"Error inserting collections record: {e}")
            raise  # Reraise the exception to handle it outside if necessary

def dump_database_tables_to_html(db_paths: dict={"transfer" :None, "validation" :None}, db_tables: dict={"transfer":[],"validation":[]}) -> str:
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
    html_body = ""
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

