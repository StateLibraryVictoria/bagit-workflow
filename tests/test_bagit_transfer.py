from bagit_transfer import *
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


@pytest.fixture
def manifest_filepath(tmp_path):
    dir = tmp_path / "hash"
    dir.mkdir()
    file = dir / "manifest-sha256.txt"
    with open(file, "w") as f:
        f.write(
            "This file should actually contain a manifest of hashes for each file in a bag."
        )
    yield dir


@pytest.fixture(scope="function")
def database_path(tmp_path):
    dir = tmp_path / "database"
    dir.mkdir()
    database = dir / "database.db"
    yield database


@pytest.fixture
def invalid_trigger_path(tmp_path):
    file = tmp_path / "invalid_trigger.ok"
    dir = tmp_path / "invalid_trigger"
    dir.mkdir()
    file2 = dir / "data.txt"
    yield tmp_path


# test_configure_db
def test_configure_db(database_path):
    configure_db(database_path)
    db = sqlite3.connect(database_path)
    cur = db.cursor()
    result = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table';"
    ).fetchall()
    tables = sorted(list(zip(*result))[0])
    assert tables == ["Collections", "Transfers", "sqlite_sequence"]


def test_configure_db_twice_is_fine(database_path):
    configure_db(database_path)
    configure_db(database_path)
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
    configure_db(database_path)
    insert_transfer(str(existing_bag), existing_bag, hash, 2, database_path)
    db = sqlite3.connect(database_path)
    cur = db.cursor()
    result = cur.execute("SELECT * FROM Transfers;").fetchall()
    assert len(result[0]) == 7


def test_insert_transfer_valid_data_right_columns_transfers(
    existing_bag, database_path
):
    hash = compute_manifest_hash(str(existing_bag))
    configure_db(database_path)
    insert_transfer(str(existing_bag), existing_bag, hash, 2, database_path)
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
    configure_db(database_path)
    insert_transfer(str(existing_bag), existing_bag, hash, 2, database_path)
    db = sqlite3.connect(database_path)
    cur = db.cursor()
    result = cur.execute("SELECT * FROM Collections;").fetchall()
    assert len(result[0]) == 2


def test_insert_transfer_valid_data_right_columns_collections(
    existing_bag, database_path
):
    hash = compute_manifest_hash(str(existing_bag))
    configure_db(database_path)
    insert_transfer(str(existing_bag), existing_bag, hash, 2, database_path)
    db = sqlite3.connect(database_path)
    cur = db.cursor()
    result = cur.execute("SELECT * FROM Collections;").fetchall()
    columns = [x[0] for x in cur.description]
    assert columns == [
        "CollectionIdentifier",
        "Count",
    ]


# test_timed_rsync_copy
def test_timed_rsync_copy_takes_time(existing_bag, tmp_path):
    time = timed_rsync_copy(existing_bag, tmp_path)
    assert 1 > time > 0


def test_timed_rsync_copy_succesfully_copies_bag(existing_bag, tmp_path):
    timed_rsync_copy(existing_bag, tmp_path)
    new_bag = bagit.Bag(os.path.join(tmp_path, str(existing_bag)))
    assert new_bag.validate()


# test_compute_manifest_hash
def test_compute_manifest_hash(manifest_filepath):
    expected = (
        "C2AC092D01314AB876B109F50B74F34B7285D428D2AC504E0EA22F7960AF110D".lower()
    )
    hash = compute_manifest_hash(manifest_filepath)
    assert hash == expected


# test_cleanup_transfer
def test_cleaup_transfer(invalid_trigger_path):
    cleanup_transfer(invalid_trigger_path / "invalid_trigger")
    ok_files = os.listdir(invalid_trigger_path)
    assert len(ok_files) == 0

# test guessing primary id
@pytest.mark.parametrize(
    "input, expected",
    [
        (["SC1234", "abc", "POL-1234", "RA-9999-99"], "RA-9999-99"),
        (["SC1234", "RA-8888-88", "POL-1234", "RA-9999-99"], "RA-8888-88"),
        (["nonsense"], None),
        (["SC1234"], "SC1234")
    ],
)
def test_guess_primary_id(input, expected):
    result = guess_primary_id(input)
    assert result == expected