
from src.database_functions import *
from src.helper_functions import compute_manifest_hash
import pytest


@pytest.fixture
def existing_bag(tmp_path):
    dir = tmp_path / "test_bag"
    dir.mkdir()
    file = dir / "file.txt"
    with open(file, "w") as f:
        f.write("Text in file.")
    bag = bagit.make_bag(
        dir,
        {
            "Source-Organization": "Home",
            "Contact-Name": "Name",
            "External-Description": "A test metadata package for a valid transfer.",
            "External-Identifier": "RA-9999-99",
            "Internal-Sender-Identifier": "ce2c5343-0f5c-45e1-9cd1-5e10e748efef",
        },
    )
    yield bag



@pytest.fixture(scope="function")
def database_path(tmp_path):
    dir = tmp_path / "database"
    dir.mkdir()
    database = dir / "database.db"
    yield database

@pytest.fixture()
def validation_db(tmp_path):
    dir = tmp_path / "validation_db"
    dir.mkdir()
    database = dir / "database.db"
    yield database



# test_configure_transfer_db
def test_configure_transfer_db(database_path):
    configure_transfer_db(database_path)
    db = sqlite3.connect(database_path)
    cur = db.cursor()
    result = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table';"
    ).fetchall()
    tables = sorted(list(zip(*result))[0])
    assert tables == ["Collections", "Transfers", "sqlite_sequence"]


def test_configure_transfer_db_twice_is_fine(database_path):
    configure_transfer_db(database_path)
    configure_transfer_db(database_path)
    db = sqlite3.connect(database_path)
    cur = db.cursor()
    result = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table';"
    ).fetchall()
    tables = sorted(list(zip(*result))[0])
    assert tables == ["Collections", "Transfers", "sqlite_sequence"]


# test_insert_transfer
def test_insert_transfer_valid_data_right_length_transfers(existing_bag, database_path):
    hash = compute_manifest_hash(str(existing_bag))
    configure_transfer_db(database_path)
    insert_transfer(str(existing_bag), existing_bag, "RA-9999-99", hash, 2, database_path)
    db = sqlite3.connect(database_path)
    cur = db.cursor()
    result = cur.execute("SELECT * FROM Transfers;").fetchall()
    assert len(result[0]) == 7


def test_insert_transfer_valid_data_right_columns_transfers(
    existing_bag, database_path
):
    hash = compute_manifest_hash(str(existing_bag))
    configure_transfer_db(database_path)
    insert_transfer(str(existing_bag), existing_bag, "RA-9999-99", hash, 2, database_path)
    db = sqlite3.connect(database_path)
    cur = db.cursor()
    result = cur.execute("SELECT * FROM Transfers;").fetchall()
    columns = [x[0] for x in cur.description]
    assert columns == [
        "TransferID",
        "CollectionIdentifier",
        "BagUUID",
        "TransferDate",
        "PayloadOxum",
        "ManifestSHA256Hash",
        "TransferTimeSeconds",
    ]


# test_insert_transfer
def test_insert_transfer_valid_data_right_length_collections(
    existing_bag, database_path
):
    hash = compute_manifest_hash(str(existing_bag))
    configure_transfer_db(database_path)
    insert_transfer(str(existing_bag), existing_bag, "RA-9999-99", hash, 2, database_path)
    db = sqlite3.connect(database_path)
    cur = db.cursor()
    result = cur.execute("SELECT * FROM Collections;").fetchall()
    assert len(result[0]) == 2


def test_insert_transfer_valid_data_right_columns_collections(
    existing_bag, database_path
):
    hash = compute_manifest_hash(str(existing_bag))
    configure_transfer_db(database_path)
    insert_transfer(str(existing_bag), existing_bag, "RA-9999-99", hash, 2, database_path)
    db = sqlite3.connect(database_path)
    cur = db.cursor()
    result = cur.execute("SELECT * FROM Collections;").fetchall()
    columns = [x[0] for x in cur.description]
    assert columns == [
        "CollectionIdentifier",
        "Count",
    ]

    ## validation

def test_validation_db_return_primary_key(validation_db):
    configure_validation_db(validation_db)
    time = "now"
    validation_action_id = start_validation(time, validation_db)
    validation_action_id_2 = start_validation(time, validation_db)
    assert validation_action_id == 1 and validation_action_id_2 == 2

def test_add_validation_outcome_updates_action_pass(validation_db):
    configure_validation_db(validation_db)
    time = "now"
    validation_action_id = start_validation(time, validation_db)
    insert_validation_outcome(validation_action_id, "1234",True,None,"bag/path","now","later",validation_db)
    db = sqlite3.connect(validation_db)
    cur = db.cursor()
    result_pass = cur.execute("SELECT * FROM ValidationActions WHERE ValidationActionsId=?", (validation_action_id,)).fetchone()[1]
    result_fail = cur.execute("SELECT * FROM ValidationActions WHERE ValidationActionsId=?", (validation_action_id,)).fetchone()[2]
    assert result_pass == 1 and result_fail == 0

def test_add_validation_outcome_updates_action_fail(validation_db):
    configure_validation_db(validation_db)
    time = "now"
    validation_action_id = start_validation(time, validation_db)
    insert_validation_outcome(validation_action_id, "1234",False,None,"bag/path","now","later",validation_db)
    db = sqlite3.connect(validation_db)
    cur = db.cursor()
    result_pass = cur.execute("SELECT * FROM ValidationActions WHERE ValidationActionsId=?", (validation_action_id,)).fetchone()[1]
    result_fail = cur.execute("SELECT * FROM ValidationActions WHERE ValidationActionsId=?", (validation_action_id,)).fetchone()[2]
    assert result_pass == 0 and result_fail == 1