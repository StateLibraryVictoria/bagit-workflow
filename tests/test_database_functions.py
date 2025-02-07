from src.database_functions import *
from src.helper_functions import compute_manifest_hash
import pytest

SET_UUID_ID = "ce2c5343-0f5c-45e1-9cd1-5e10e748efef"
TRANSFERS = "transfers"  # table name
BAG_DIR = "test_bag"


@pytest.fixture
def existing_bag(tmp_path):
    dir = tmp_path / BAG_DIR
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
            "Internal-Sender-Identifier": SET_UUID_ID,
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
def transfers_db_with_entry(tmp_path, existing_bag):
    dir = tmp_path / "database"
    dir.mkdir()
    database = dir / "transfer.db"
    configure_transfer_db(database)
    hash = compute_manifest_hash(str(existing_bag))
    insert_transfer(
        BAG_DIR,
        existing_bag,
        "RA-9999-99",
        hash,
        datetime.now(),
        datetime.now(),
        database,
    )
    yield database


@pytest.fixture()
def validation_db(tmp_path):
    dir = tmp_path / "validation_db"
    dir.mkdir()
    database = dir / "database.db"
    yield database


@pytest.fixture()
def configured_validation_db_started(validation_db):
    configure_validation_db(validation_db)
    time = "now"
    validation_action_id = start_validation(time, validation_db)
    yield (validation_db, validation_action_id)


# test Transfer class
def test_init_ValidationStatus_valid(transfers_db_with_entry, existing_bag, tmp_path):
    validation_status = ValidationStatus(
        transfers_db_with_entry, TRANSFERS, str(existing_bag), tmp_path
    )
    print(validation_status.get_relative_path())
    outcome = validation_status.is_valid()
    assert outcome == True


def test_no_db_match_ValidationStatus_errors(
    transfers_db_with_entry, existing_bag, tmp_path
):
    validation_status = ValidationStatus(
        transfers_db_with_entry, TRANSFERS, str(existing_bag), BAG_DIR
    )
    errors = validation_status.get_error_string()
    assert errors == "Bag path not found in transfers database."


def test_no_db_match_ValidationStatus_outcome(
    transfers_db_with_entry, existing_bag, tmp_path
):
    validation_status = ValidationStatus(
        transfers_db_with_entry, TRANSFERS, str(existing_bag), BAG_DIR
    )
    outcome = validation_status.is_valid()
    assert outcome == False


def test_no_uuid_ValidatioNStatus_outcome(
    transfers_db_with_entry, existing_bag, tmp_path
):
    existing_bag.info[UUID_ID] = [None]
    existing_bag.save()
    validation_status = ValidationStatus(
        transfers_db_with_entry, TRANSFERS, str(existing_bag), tmp_path
    )
    outcome = validation_status.is_valid()
    assert outcome == False


def test_no_uuid_ValidatioNStatus_errors(
    transfers_db_with_entry, existing_bag, tmp_path
):
    existing_bag.info[UUID_ID] = [None]
    existing_bag.save()
    validation_status = ValidationStatus(
        transfers_db_with_entry, TRANSFERS, str(existing_bag), tmp_path
    )
    errors = validation_status.get_error_string()
    assert (
        errors
        == f"Bag UUID not present in bag-info.txt;UUID conflict in database for transfer 1 with UUID {SET_UUID_ID}"
    )


def test_invalid_bag_ValidationStatus(transfers_db_with_entry, existing_bag, tmp_path):
    os.remove(os.path.join(str(existing_bag), "bag-info.txt"))
    validation_status = ValidationStatus(
        transfers_db_with_entry, TRANSFERS, str(existing_bag), tmp_path
    )
    outcome = validation_status.is_valid()
    assert outcome == False


def test_ValidationStatus_get_uuid(transfers_db_with_entry, existing_bag, tmp_path):
    validation_status = ValidationStatus(
        transfers_db_with_entry, TRANSFERS, str(existing_bag), tmp_path
    )
    outcome = validation_status.get_bag_uuid()
    assert outcome == SET_UUID_ID


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
    start = datetime.now()
    hash = compute_manifest_hash(str(existing_bag))
    configure_transfer_db(database_path)
    insert_transfer(
        str(existing_bag),
        existing_bag,
        "RA-9999-99",
        hash,
        start,
        datetime.now(),
        database_path,
    )
    db = sqlite3.connect(database_path)
    cur = db.cursor()
    result = cur.execute("SELECT * FROM Transfers;").fetchall()
    assert len(result[0]) == 13


def test_insert_transfer_valid_data_right_columns_transfers(
    existing_bag, database_path
):
    start = datetime.now()
    hash = compute_manifest_hash(str(existing_bag))
    configure_transfer_db(database_path)
    insert_transfer(
        str(existing_bag),
        existing_bag,
        "RA-9999-99",
        hash,
        start,
        datetime.now(),
        database_path,
    )
    db = sqlite3.connect(database_path)
    cur = db.cursor()
    result = cur.execute("SELECT * FROM Transfers;").fetchall()
    columns = [x[0] for x in cur.description]
    assert columns == [
        "TransferID",
        "CollectionIdentifier",
        "BagUUID",
        "TransferDate",
        "BagDate",
        "PayloadOxum",
        "ManifestSHA256Hash",
        "StartTime",
        "EndTime",
        "OriginalFolderTitle",
        "OutcomeFolderTitle",
        "ContactName",
        "SourceOrganisation",
    ]


# test_insert_transfer
def test_insert_transfer_valid_data_right_length_collections(
    existing_bag, database_path
):
    start = datetime.now()
    hash = compute_manifest_hash(str(existing_bag))
    configure_transfer_db(database_path)
    insert_transfer(
        str(existing_bag),
        existing_bag,
        "RA-9999-99",
        hash,
        start,
        datetime.now(),
        database_path,
    )
    db = sqlite3.connect(database_path)
    cur = db.cursor()
    result = cur.execute("SELECT * FROM Collections;").fetchall()
    assert len(result[0]) == 2


def test_insert_transfer_valid_data_right_columns_collections(
    existing_bag, database_path
):
    start = datetime.now()
    hash = compute_manifest_hash(str(existing_bag))
    configure_transfer_db(database_path)
    insert_transfer(
        str(existing_bag),
        existing_bag,
        "RA-9999-99",
        hash,
        start,
        datetime.now(),
        database_path,
    )
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
    insert_validation_outcome(
        validation_action_id,
        "1234",
        True,
        None,
        "bag/path",
        "now",
        "later",
        validation_db,
    )
    db = sqlite3.connect(validation_db)
    cur = db.cursor()
    result_pass = cur.execute(
        "SELECT * FROM ValidationActions WHERE ValidationActionsId=?",
        (validation_action_id,),
    ).fetchone()[1]
    result_fail = cur.execute(
        "SELECT * FROM ValidationActions WHERE ValidationActionsId=?",
        (validation_action_id,),
    ).fetchone()[2]
    assert result_pass == 1 and result_fail == 0


def test_add_validation_outcome_updates_action_fail(validation_db):
    configure_validation_db(validation_db)
    time = "now"
    validation_action_id = start_validation(time, validation_db)
    insert_validation_outcome(
        validation_action_id,
        "1234",
        False,
        None,
        "bag/path",
        "now",
        "later",
        validation_db,
    )
    db = sqlite3.connect(validation_db)
    cur = db.cursor()
    result_pass = cur.execute(
        "SELECT * FROM ValidationActions WHERE ValidationActionsId=?",
        (validation_action_id,),
    ).fetchone()[1]
    result_fail = cur.execute(
        "SELECT * FROM ValidationActions WHERE ValidationActionsId=?",
        (validation_action_id,),
    ).fetchone()[2]
    assert result_pass == 0 and result_fail == 1


def test_add_validation_outcome_updates_outcome(validation_db):
    configure_validation_db(validation_db)
    time = "now"
    validation_action_id = start_validation(time, validation_db)
    insert_validation_outcome(
        validation_action_id,
        "2222",
        True,
        None,
        "bag/path/1",
        "now",
        "later",
        validation_db,
    )
    insert_validation_outcome(
        validation_action_id,
        "1234",
        False,
        "Error validating bag",
        "bag/path",
        "now",
        "later",
        validation_db,
    )
    db = sqlite3.connect(validation_db)
    cur = db.cursor()
    # OutcomeIdentifier, ValidationActionsId, BagUUID, Outcome, Errors, BagPath, StartTime, EndTime
    outcomes = cur.execute("SELECT * FROM ValidationOutcome;").fetchall()
    print(outcomes[0])
    assert outcomes == [
        (1, 1, "2222", "Pass", None, "bag/path/1", "now", "later"),
        (2, 1, "1234", "Fail", "Error validating bag", "bag/path", "now", "later"),
    ]


def test_end_validation_sets_status_to_complete(configured_validation_db_started):
    validation_action_id = configured_validation_db_started[1]
    end_validation(validation_action_id, "now", configured_validation_db_started[0])
    db = sqlite3.connect(configured_validation_db_started[0])
    cur = db.cursor()
    # ValidationActionsId INTEGER PRIMARY KEY AUTOINCREMENT, CountBagsValidated INT, CountBagsWithErrors INT, StartAction, EndAction, Status
    outcomes = cur.execute("SELECT * FROM ValidationActions;").fetchall()
    assert len(outcomes) == 1 and outcomes[0][5] == "Complete"
